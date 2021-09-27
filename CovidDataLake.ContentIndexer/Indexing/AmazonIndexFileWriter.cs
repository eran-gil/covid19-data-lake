using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using CovidDataLake.Amazon;
using CovidDataLake.Bloom;
using CovidDataLake.Common;
using CovidDataLake.ContentIndexer.Configuration;
using CovidDataLake.ContentIndexer.Extensions;
using CovidDataLake.ContentIndexer.Indexing.Models;

namespace CovidDataLake.ContentIndexer.Indexing
{
    public class AmazonIndexFileWriter : IIndexFileWriter
    {
        private readonly IAmazonAdapter _amazonAdapter;
        private readonly string _bucketName;
        private readonly int _numOfRowsPerMetadataSection;
        private readonly AmazonIndexFileConfiguration _config;

        public AmazonIndexFileWriter(IAmazonAdapter amazonAdapter, AmazonIndexFileConfiguration configuration)
        {
            _amazonAdapter = amazonAdapter;
            _bucketName = configuration.BucketName;
            _numOfRowsPerMetadataSection = configuration.NumOfMetadataRows;
            _config = configuration;
        }

        public async Task<IEnumerable<RootIndexRow>> UpdateIndexFileWithValues(IList<ulong> values, string indexFilename, string originFilename)
        {
            //todo: handle split if necessary
            var downloadedFilename =
                $"{CommonKeys.INDEX_FOLDER_NAME}/{CommonKeys.COLUMN_INDICES_FOLDER_NAME}/{Guid.NewGuid()}.txt";
            if (indexFilename != CommonKeys.END_OF_INDEX_FLAG)
            {
                downloadedFilename = await _amazonAdapter.DownloadObjectAsync(_bucketName, indexFilename);
            }
            else
            {
                indexFilename = downloadedFilename;
            }

            var originalIndexValues = GetIndexValuesFromFile(downloadedFilename);
            var indexValues = MergeIndexWithUpdatedValues(originalIndexValues, values, originFilename);

            var outputFilename = downloadedFilename + "_new";
            using var outputFile = File.OpenWrite(outputFilename);
            using var outputStreamWriter = new StreamWriter(outputFile);
            var rowsMetadata = await WriteIndexValuesToFile(indexValues, outputStreamWriter);
            var newMetadataOffset = outputFile.Position;
            var rootIndexRow = await AddUpdatedMetadataToFile(rowsMetadata, outputStreamWriter);

            var bloomOffset = outputFile.Position;
            await AddUpdatedBloomFilterToIndex(values, downloadedFilename, outputStreamWriter);
            outputFile.WriteBinaryLongToStream(newMetadataOffset);
            outputFile.WriteBinaryLongToStream(bloomOffset);
            await _amazonAdapter.UploadObjectAsync(_bucketName, indexFilename, outputFilename);
            return new List<RootIndexRow> {rootIndexRow};
        }

        private async Task<RootIndexRow> AddUpdatedMetadataToFile(IList<FileRowMetadata> rowsMetadata, StreamWriter outputStreamWriter)
        {
            var metadataSections = CreateMetadataFromRows(rowsMetadata).ToList();
            var minValue = metadataSections.First().Min;
            var maxValue = metadataSections.Last().Max;
            var rootIndexRow = new RootIndexRow(null, minValue, maxValue, null);
            foreach (var metadataSection in metadataSections)
            {
                await outputStreamWriter.WriteObjectToLineAsync(metadataSection);
            }

            return rootIndexRow;
        }

        private async Task AddUpdatedBloomFilterToIndex(IEnumerable<ulong> values, string downloadedFilename,
            TextWriter outputStreamWriter)
        {
            var serializedBloomFilter = GetSerializedBloomFilterFromFile(downloadedFilename);
            using var bloomFilter = GetBloomFilter(serializedBloomFilter);
            foreach (var value in values)
            {
                bloomFilter.AddToFilter(value);
            }

            var outputBloomFilter = bloomFilter.Serialize();
            await outputStreamWriter.WriteLineAsync(outputBloomFilter);
        }

        private PythonBloomFilter GetBloomFilter(string serializedBloomFilter)
        {
            // ReSharper disable once ConvertIfStatementToReturnStatement
            if (serializedBloomFilter == null)
            {
                return new PythonBloomFilter(_config.BloomFilterCapacity, _config.BloomFilterErrorRate);

            }
            return new PythonBloomFilter(serializedBloomFilter);
        }

        private static async Task<List<FileRowMetadata>> WriteIndexValuesToFile(
            IAsyncEnumerable<IndexValueModel> indexValues, StreamWriter outputStreamWriter)
        {
            var rowsMetadata = new List<FileRowMetadata>();
            await foreach (var indexValue in indexValues)
            {
                var rowMetadata = new FileRowMetadata(outputStreamWriter.BaseStream.Position, indexValue.Value);
                rowsMetadata.Add(rowMetadata);
                await outputStreamWriter.WriteObjectToLineAsync(indexValue);
            }
            return rowsMetadata;
        }

        private static IAsyncEnumerable<IndexValueModel> GetIndexValuesFromFile(string filename)
        {
            if (new FileInfo(filename).Length == 0)
            {
                return AsyncEnumerable.Empty<IndexValueModel>();
            }
            using var inputFile = File.OpenRead(filename);
            inputFile.Seek(-(2 * sizeof(long)), SeekOrigin.End);
            var metadataOffset = inputFile.ReadBinaryLongFromStream();
            inputFile.Seek(0, SeekOrigin.Begin);
            var rows = inputFile.GetDeserializedRowsFromFileAsync<IndexValueModel>(metadataOffset);
            return rows;
        }

        private static string GetSerializedBloomFilterFromFile(string filename)
        {
            if (new FileInfo(filename).Length == 0)
            {
                return null;
            }
            using var inputFile = File.OpenRead(filename);
            inputFile.Seek(-(sizeof(long)), SeekOrigin.End);
            var bloomOffset = inputFile.ReadBinaryLongFromStream();
            inputFile.Seek(bloomOffset, SeekOrigin.Begin);
            using var streamReader = new StreamReader(inputFile);
            var serializedBloomFilter = streamReader.ReadLine();
            return serializedBloomFilter;
        }

        private static async IAsyncEnumerable<IndexValueModel> MergeIndexWithUpdatedValues(
            IAsyncEnumerable<IndexValueModel> originalIndexValues,
            IList<ulong> values, string originFilename)
        {
            await foreach(var indexValue in originalIndexValues)
            {
                if (!values.Any())
                {
                    yield return indexValue;
                    continue;
                }

                var currentInputValue = values.First();
                if (currentInputValue == indexValue.Value)
                {
                    indexValue.Files.Add(originFilename);
                    values.RemoveAt(0);
                }
                if (currentInputValue < indexValue.Value)
                {
                    var newIndexValue = new IndexValueModel(currentInputValue, new List<string> {originFilename});
                    values.RemoveAt(0);
                    yield return newIndexValue;
                }

                yield return indexValue;
            }

            if (!values.Any()) yield  break;
            foreach (var currentValue in values)
            {
                var newIndexValue = new IndexValueModel(currentValue, new List<string> { originFilename });
                yield return newIndexValue;
            }
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
    }
}
