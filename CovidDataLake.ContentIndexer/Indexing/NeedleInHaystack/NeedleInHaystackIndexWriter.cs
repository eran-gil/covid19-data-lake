using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using CovidDataLake.Cloud.Amazon;
using CovidDataLake.Cloud.Amazon.Configuration;
using CovidDataLake.Common;
using CovidDataLake.Common.Files;
using CovidDataLake.Common.Probabilistic;
using CovidDataLake.ContentIndexer.Configuration;
using CovidDataLake.ContentIndexer.Extensions;
using CovidDataLake.ContentIndexer.Extraction.Models;
using CovidDataLake.ContentIndexer.Indexing.Models;
using Newtonsoft.Json;

namespace CovidDataLake.ContentIndexer.Indexing.NeedleInHaystack
{
    public class NeedleInHaystackIndexWriter
    {
        private readonly IAmazonAdapter _amazonAdapter;
        private readonly NeedleInHaystackIndexReader _indexReader;
        private readonly string _bucketName;
        private readonly int _maxRowsPerFile;
        private readonly JsonSerializer _serializer;
        private readonly int _numOfRowsPerMetadataSection;
        private readonly double _bloomFilterErrorRate;
        private readonly int _bloomFilterCapacity;

        public NeedleInHaystackIndexWriter(IAmazonAdapter amazonAdapter, NeedleInHaystackIndexReader indexReader, BasicAmazonIndexFileConfiguration indexConfig, NeedleInHaystackIndexConfiguration configuration)
        {
            _amazonAdapter = amazonAdapter;
            _indexReader = indexReader;
            _bucketName = indexConfig.BucketName!;
            _numOfRowsPerMetadataSection = configuration.NumOfMetadataRows;
            _bloomFilterErrorRate = configuration.BloomFilterErrorRate;
            _bloomFilterCapacity = configuration.BloomFilterCapacity;
            _maxRowsPerFile = configuration.MaxRowsPerFile;
            _serializer = new JsonSerializer();
        }
        public async Task<IEnumerable<RootIndexRow>> UpdateIndexFileWithValues(string columnName, string indexFilename, IEnumerable<RawEntry> values)
        {
            var (indexName, localFilename) = await _indexReader.DownloadIndexFile(columnName, indexFilename);
            var indexDictionary = NeedleInHaystackIndexReader.GetIndexFromFile(localFilename);
            var newIndexValues = values.Select(rawValue => new IndexValueModel(rawValue.Value, rawValue.OriginFilenames));

            Parallel.ForEach(newIndexValues, indexValue =>
                {
                    indexDictionary.AddOrUpdate(indexValue.Value, indexValue, (_, currentValue) =>
                    {
                        currentValue.AddFiles(indexValue.Files);
                        return currentValue;
                    });
                }
            );

            var rootIndexRows = WriteIndexToFiles(indexDictionary);
            var localFileNames = OverrideLocalFileNames(indexName, columnName, rootIndexRows);

            await UploadIndexFiles(rootIndexRows, localFileNames);

            return rootIndexRows;
        }

        private IReadOnlyCollection<RootIndexRow> WriteIndexToFiles(ConcurrentDictionary<string, IndexValueModel> indexDictionary)
        {
            var orderedIndexValues = indexDictionary.OrderBy(kvp => kvp.Key).Select(kvp => kvp.Value);
            var indexValueBatches = orderedIndexValues.Chunk(_maxRowsPerFile);
            var rootIndexRows = new ConcurrentBag<RootIndexRow>();
            Parallel.ForEach(indexValueBatches, batch =>
            {
                var rootIndexRow = WriteBatchToFile(batch);
                rootIndexRows.Add(rootIndexRow);
            });
            return rootIndexRows;
        }

        private RootIndexRow WriteBatchToFile(IEnumerable<IndexValueModel> batch)
        {
            var outputFilename = Path.Combine(CommonKeys.TEMP_FOLDER_NAME, Guid.NewGuid().ToString());
            return WriteIndexFile(batch, outputFilename);
        }

        private RootIndexRow WriteIndexFile(IEnumerable<IndexValueModel> indexValues, string outputFilename)
        {
             using var outputFile = FileCreator.OpenFileWriteAndCreatePath(outputFilename);
             using var outputStreamWriter = new StreamWriter(outputFile);
            outputStreamWriter.AutoFlush = false;
            using var jsonWriter = new JsonTextWriter(outputStreamWriter);
            var rowsMetadata = WriteIndexValuesToFile(indexValues, jsonWriter, outputFile);
            outputStreamWriter.Flush();
            var newMetadataOffset = outputFile.Position;
            var rootIndexRow = AddUpdatedMetadataToFile(rowsMetadata, jsonWriter);
            rootIndexRow.FileName = outputFilename;
            outputStreamWriter.Flush();
            var bloomOffset = outputFile.Position;
            AddBloomFilterToIndex(rowsMetadata, outputFile);
            outputFile.Flush();
            outputFile.WriteBinaryLongsToStream(new[] { newMetadataOffset, bloomOffset });
            return rootIndexRow;
        }

        private List<FileRowMetadata> WriteIndexValuesToFile(
            IEnumerable<IndexValueModel> indexValues, JsonWriter jsonWriter, Stream stream)
        {
            var rowsMetadata = new List<FileRowMetadata>();
            foreach (var indexValue in indexValues)
            {
                var rowMetadata = new FileRowMetadata(stream.Position, indexValue.Value);
                rowsMetadata.Add(rowMetadata);
                _serializer.Serialize(jsonWriter, indexValue);
                jsonWriter.WriteWhitespace(Environment.NewLine);
            }

            return rowsMetadata;
        }

        private RootIndexRow AddUpdatedMetadataToFile(IList<FileRowMetadata> rowsMetadata,
            JsonWriter jsonWriter)
        {
            var metadataSections = CreateMetadataFromRows(rowsMetadata).ToList();
            var minValue = metadataSections.First().Min;
            var maxValue = metadataSections.Last().Max;
            var rootIndexRow = new RootIndexRow(null, minValue, maxValue, null);
            foreach (var metadataSection in metadataSections)
            {
                _serializer.Serialize(jsonWriter, metadataSection);
                jsonWriter.WriteWhitespace(Environment.NewLine);
            }

            return rootIndexRow;
        }

        private void AddBloomFilterToIndex(IEnumerable<FileRowMetadata> rows, Stream outputStream)
        {
            var bloomFilter = GetBloomFilter();
            foreach (var row in rows)
            {
                bloomFilter.Add(row.Value);
            }
            bloomFilter.Serialize(outputStream);
        }

        private BasicBloomFilter GetBloomFilter(byte[] serializedBloomFilter = null)
        {
            // ReSharper disable once ConvertIfStatementToReturnStatement
            if (serializedBloomFilter == null)
            {
                return new BasicBloomFilter(_bloomFilterCapacity, _bloomFilterErrorRate);
            }

            using var bloomFilterStream = new MemoryStream(serializedBloomFilter);
            return new BasicBloomFilter(bloomFilterStream);
        }

        private IEnumerable<IndexMetadataSectionModel> CreateMetadataFromRows(IList<FileRowMetadata> rowMetadatas)
        {
            var numOfRows = rowMetadatas.Count;
            for (var i = 0; i < numOfRows; i += _numOfRowsPerMetadataSection)
            {
                var currentRow = rowMetadatas[i];
                var min = currentRow.Value;
                var endRow = Math.Min(i + _numOfRowsPerMetadataSection - 1, numOfRows - 1);
                var max = rowMetadatas[endRow].Value;
                var offset = currentRow.Offset;
                var metadataSection = new IndexMetadataSectionModel(min, max, offset);
                yield return metadataSection;
            }
        }

        private static Dictionary<string, string> OverrideLocalFileNames(string indexFilename, string columnName,
            IReadOnlyCollection<RootIndexRow> rootIndexRows)
        {
            var localFileNames = new Dictionary<string, string>();
            for (var i = 0; i < rootIndexRows.Count; i++)
            {
                var rootIndexRow = rootIndexRows.ElementAt(i);
                var localFileName = rootIndexRow.FileName;
                rootIndexRow.FileName = i == 0 ? indexFilename : NeedleInHaystackUtils.CreateNewColumnIndexFileName(columnName);
                localFileNames[rootIndexRow.FileName] = localFileName;
            }

            return localFileNames;
        }

        private async Task UploadIndexFiles(IEnumerable<RootIndexRow> rootIndexRows, IReadOnlyDictionary<string, string> localFileNames)
        {
            var tasks = rootIndexRows.Select(row => UploadFile(row, localFileNames));
            await Task.WhenAll(tasks);
        }

        private Task UploadFile(RootIndexRow row, IReadOnlyDictionary<string, string> localFileNames)
        {
            var localFileName = localFileNames[row.FileName];
            return _amazonAdapter.UploadObjectAsync(_bucketName, row.FileName, localFileName);
        }
    }
}
