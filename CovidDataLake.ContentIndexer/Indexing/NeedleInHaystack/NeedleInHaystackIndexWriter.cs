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
        public async Task<IEnumerable<RootIndexRow>> UpdateIndexFileWithValues(string columnName, string indexFilename, IAsyncEnumerable<RawEntry> values)
        {
            var (indexName, localFilename) = await _indexReader.DownloadIndexFile(columnName, indexFilename);
            var indexDictionary = NeedleInHaystackIndexReader.GetIndexFromFile(localFilename);
            var newIndexValues = values.Select(rawValue => new IndexValueModel(rawValue.Value, rawValue.OriginFilenames));

            await foreach (var indexValue in newIndexValues)
            {
                indexDictionary.AddOrUpdate(indexValue.Value, indexValue, (_, currentValue) =>
                {
                    currentValue.AddFiles(indexValue.Files);
                    return currentValue;
                });
            }

            var rootIndexRows = await WriteIndexToFiles(indexDictionary).ToListAsync();
            var localFileNames = OverrideLocalFileNames(indexName, columnName, rootIndexRows);

            await UploadIndexFiles(rootIndexRows, localFileNames);

            return rootIndexRows;
        }

        private async IAsyncEnumerable<RootIndexRow> WriteIndexToFiles(ConcurrentDictionary<string, IndexValueModel> indexDictionary)
        {
            var orderedIndexValues = indexDictionary.OrderBy(kvp => kvp.Key).Select(kvp => kvp.Value);
            var indexValueBatches = orderedIndexValues.Chunk(_maxRowsPerFile);
            foreach (var indexValueBatch in indexValueBatches)
            {
                var rootIndexRow = await WriteBatchToFile(indexValueBatch);
                yield return rootIndexRow;
            }
        }

        private async Task<RootIndexRow> WriteBatchToFile(IEnumerable<IndexValueModel> batch)
        {
            var outputFilename = Path.Combine(CommonKeys.TEMP_FOLDER_NAME, Guid.NewGuid().ToString());
            return await WriteIndexFile(batch, outputFilename);
        }

        private async Task<RootIndexRow> WriteIndexFile(IEnumerable<IndexValueModel> indexValues, string outputFilename)
        {
            await using var outputFile = FileCreator.OpenFileWriteAndCreatePath(outputFilename);
            await using var outputStreamWriter = new StreamWriter(outputFile);
            using var jsonWriter = new JsonTextWriter(outputStreamWriter);
            outputStreamWriter.AutoFlush = false;
            var bloomFilter = GetBloomFilter();
            var rowsMetadata = WriteIndexValuesToFile(indexValues, jsonWriter, outputFile, bloomFilter);
            await jsonWriter.FlushAsync();
            var newMetadataOffset = outputFile.Position;
            var rootIndexRow = await AddUpdatedMetadataToFile(rowsMetadata, jsonWriter);
            rootIndexRow.FileName = outputFilename;
            await outputStreamWriter.FlushAsync();
            var bloomOffset = outputFile.Position;
            bloomFilter.Serialize(outputFile);
            await outputFile.FlushAsync();
            outputFile.WriteBinaryLongsToStream(new[] { newMetadataOffset, bloomOffset });
            await outputFile.FlushAsync();
            return rootIndexRow;
        }

        private async IAsyncEnumerable<FileRowMetadata> WriteIndexValuesToFile(
            IEnumerable<IndexValueModel> indexValues,
            JsonWriter jsonWriter,
            Stream stream,
            BasicBloomFilter bloomFilter)
        {
            foreach (var indexValue in indexValues)
            {
                _serializer.Serialize(jsonWriter, indexValue);
                await jsonWriter.WriteWhitespaceAsync(Environment.NewLine);
                bloomFilter.Add(indexValue.Value);
                yield return new FileRowMetadata(stream.Position, indexValue.Value);
            }
        }

        private async Task<RootIndexRow> AddUpdatedMetadataToFile(IAsyncEnumerable<FileRowMetadata> rowsMetadata,
            JsonWriter jsonWriter)
        {
            var metadataSections = await CreateMetadataFromRows(rowsMetadata).ToListAsync();
            var minValue = metadataSections.First().Min;
            var maxValue = metadataSections.Last().Max;
            var rootIndexRow = new RootIndexRow(null, minValue, maxValue, null);
            foreach (var metadataSection in metadataSections)
            {
                _serializer.Serialize(jsonWriter, metadataSection);
                await jsonWriter.WriteWhitespaceAsync(Environment.NewLine);
            }

            return rootIndexRow;
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

        private async IAsyncEnumerable<IndexMetadataSectionModel> CreateMetadataFromRows(IAsyncEnumerable<FileRowMetadata> rowMetadatas)
        {
            //var numOfRows = rowMetadatas.Count;

            var enumerator = rowMetadatas.GetAsyncEnumerator();
            while (await enumerator.MoveNextAsync())
            {
                var currentRow = enumerator.Current;
                var min = currentRow.Value;
                var maxRow = await enumerator.NthItemOrLast(_numOfRowsPerMetadataSection, currentRow);
                
                var max = maxRow!.Value;
                var offset = currentRow.Offset;
                yield return new IndexMetadataSectionModel(min, max, offset);
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
