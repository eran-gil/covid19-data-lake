using System;
using System.Collections.Generic;
using System.Collections.Immutable;
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
            var (indexName, localFilename) = await _indexReader.DownloadIndexFile(columnName, indexFilename).ConfigureAwait(false);
            var indexDictionary = NeedleInHaystackIndexReader.GetIndexFromFile(localFilename);

            await foreach (var indexValue in values)
            {
                if (indexDictionary.ContainsKey(indexValue.Value))
                {
                    indexDictionary[indexValue.Value] = indexDictionary[indexValue.Value].Union(indexValue.GetFileNames());
                }
                else
                {
                    indexDictionary[indexValue.Value] = indexValue.GetFileNames();
                }
            }

            var rootIndexRows = await WriteIndexToFiles(indexDictionary).ConfigureAwait(false);
            var localFileNames = OverrideLocalFileNames(indexName, columnName, rootIndexRows);

           await UploadIndexFiles(rootIndexRows, localFileNames).ConfigureAwait(false);

            return rootIndexRows;
        }

        private async Task<RootIndexRow[]> WriteIndexToFiles(IDictionary<string, ImmutableHashSet<StringWrapper>> indexDictionary)
        {
            var orderedIndexValues = indexDictionary.OrderBy(kvp => kvp.Key);
            var indexValueBatches = orderedIndexValues.Chunk(_maxRowsPerFile);
            var batchTasks = indexValueBatches.Select(WriteBatchToFile);
            return await Task.WhenAll(batchTasks).ConfigureAwait(false);
        }

        private async Task<RootIndexRow> WriteBatchToFile(IEnumerable<KeyValuePair<string, ImmutableHashSet<StringWrapper>>> batch)
        {
            var convertedBatch = batch.Select(kvp => new IndexValueModel(kvp.Key, kvp.Value));
            var outputFilename = Path.Combine(CommonKeys.TEMP_FOLDER_NAME, Guid.NewGuid().ToString());
            return await WriteIndexFile(convertedBatch, outputFilename).ConfigureAwait(false);
        }

        private async Task<RootIndexRow> WriteIndexFile(IEnumerable<IndexValueModel> indexValues, string outputFilename)
        {
            await using var outputFile = FileCreator.OpenFileWriteAndCreatePath(outputFilename);
            await using var outputStreamWriter = new StreamWriter(outputFile);
            using var jsonWriter = new JsonTextWriter(outputStreamWriter);
            outputStreamWriter.AutoFlush = false;
            var bloomFilter = GetBloomFilter();
            var rowsMetadata = await WriteIndexValuesToFile(indexValues, jsonWriter, outputFile, bloomFilter).ToListAsync();
            await jsonWriter.FlushAsync().ConfigureAwait(false);
            var newMetadataOffset = outputFile.Position;
            var rootIndexRow = await AddUpdatedMetadataToFile(rowsMetadata, jsonWriter).ConfigureAwait(false);
            rootIndexRow.FileName = outputFilename;
            await outputStreamWriter.FlushAsync().ConfigureAwait(false);
            var bloomOffset = outputFile.Position;
            bloomFilter.Serialize(outputFile);
            await outputFile.FlushAsync().ConfigureAwait(false);
            outputFile.WriteBinaryLongsToStream(new[] { newMetadataOffset, bloomOffset });
            await outputFile.FlushAsync().ConfigureAwait(false);
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
                await jsonWriter.WriteWhitespaceAsync(Environment.NewLine).ConfigureAwait(false);
                await jsonWriter.FlushAsync().ConfigureAwait(false);
                bloomFilter.Add(indexValue.Value);
                yield return new FileRowMetadata(stream.Position, indexValue.Value);
            }
        }

        private async Task<RootIndexRow> AddUpdatedMetadataToFile(IEnumerable<FileRowMetadata> rowsMetadata,
            JsonWriter jsonWriter)
        {
            var metadataSections = await CreateMetadataFromRows(rowsMetadata).ToListAsync().ConfigureAwait(false);
            var minValue = metadataSections.First().Min;
            var maxValue = metadataSections.Last().Max;
            var rootIndexRow = new RootIndexRow(null, minValue, maxValue, null);
            foreach (var metadataSection in metadataSections)
            {
                _serializer.Serialize(jsonWriter, metadataSection);
                await jsonWriter.WriteWhitespaceAsync(Environment.NewLine).ConfigureAwait(false);
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

        private async IAsyncEnumerable<IndexMetadataSectionModel> CreateMetadataFromRows(IEnumerable<FileRowMetadata> rowMetadatas)
        {
            using var enumerator = rowMetadatas.GetEnumerator();
            while (enumerator.MoveNext())
            {
                var currentRow = enumerator.Current;
                var min = currentRow?.Value;
                var maxRow = enumerator.NthItemOrLast(_numOfRowsPerMetadataSection, currentRow);

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
            await Task.WhenAll(tasks).ConfigureAwait(false);
        }

        private Task UploadFile(RootIndexRow row, IReadOnlyDictionary<string, string> localFileNames)
        {
            var localFileName = localFileNames[row.FileName];
            return _amazonAdapter.UploadObjectAsync(_bucketName, row.FileName, localFileName);
        }
    }
}
