using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using CovidDataLake.Common;
using CovidDataLake.Common.Files;
using CovidDataLake.Common.Probabilistic;
using CovidDataLake.ContentIndexer.Configuration;
using CovidDataLake.ContentIndexer.Extensions;
using CovidDataLake.ContentIndexer.Extraction.Models;
using CovidDataLake.ContentIndexer.Indexing.Models;
using Newtonsoft.Json;

namespace CovidDataLake.ContentIndexer.Indexing
{
    public class NeedleInHaystackIndexFileAccess : IIndexFileAccess
    {
        private readonly int _numOfRowsPerMetadataSection;
        private readonly double _bloomFilterErrorRate;
        private readonly int _bloomFilterCapacity;
        private readonly int _maxRowsPerFile;
        private readonly JsonSerializer _serializer;

        public NeedleInHaystackIndexFileAccess(NeedleInHaystackIndexConfiguration configuration)
        {
            _numOfRowsPerMetadataSection = configuration.NumOfMetadataRows;
            _bloomFilterErrorRate = configuration.BloomFilterErrorRate;
            _bloomFilterCapacity = configuration.BloomFilterCapacity;
            _maxRowsPerFile = configuration.MaxRowsPerFile;
            _serializer = new JsonSerializer();

        }

        public async Task<IReadOnlyCollection<RootIndexRow>> CreateUpdatedIndexFileWithValues(string sourceIndexFileName, IEnumerable<RawEntry> values)
        {
            var originalIndexValues = Enumerable.Empty<IndexValueModel>();

            using var fileStream = OptionalFileStream.CreateOptionalFileReadStream(sourceIndexFileName);
            if (fileStream.BaseStream != null)
            {
                originalIndexValues = GetIndexValuesFromFile(fileStream.BaseStream);
            }

            var indexValues = MergeIndexWithUpdatedValues(originalIndexValues, values);

            var indexValueBatches = indexValues.Chunk(_maxRowsPerFile);
            var rootIndexRows = new ConcurrentBag<RootIndexRow>();
            await Parallel.ForEachAsync(indexValueBatches, async (batch, _) =>
            {
                var outputFilename = Path.Combine(CommonKeys.TEMP_FOLDER_NAME, Guid.NewGuid().ToString());
                var rootIndexRow = await WriteIndexFile(batch, outputFilename);
                rootIndexRows.Add(rootIndexRow);
            });

            return rootIndexRows;
        }

        private static IEnumerable<IndexValueModel> GetIndexValuesFromFile(FileStream inputFile)
        {
            inputFile.Seek(-(2 * sizeof(long)), SeekOrigin.End);
            var metadataOffset = inputFile.ReadBinaryLongFromStream();
            var rows = inputFile.GetDeserializedRowsFromFile<IndexValueModel>(0, metadataOffset);
            return rows;
        }

        private static IEnumerable<IndexValueModel> MergeIndexWithUpdatedValues(
            IEnumerable<IndexValueModel> originalIndexValues,
            IEnumerable<RawEntry> newValues)
        {
            var newIndexValues = newValues.Select(rawValue => new IndexValueModel(rawValue.Value, rawValue.OriginFilenames));
            var allIndexValues = originalIndexValues
                .Concat(newIndexValues)
                .GroupBy(indexValue => indexValue.Value)
                .Select(group =>
                {
                    return group.Aggregate((a, b) =>
                    {
                        a.AddFiles(b.Files);
                        return a;
                    });
                })
                .OrderBy(v => v.Value);
            return allIndexValues;
        }

        private async Task<RootIndexRow> WriteIndexFile(IEnumerable<IndexValueModel> indexValues, string outputFilename)
        {
            await using var outputFile = FileCreator.OpenFileWriteAndCreatePath(outputFilename);
            await using var outputStreamWriter = new StreamWriter(outputFile);
            using var jsonWriter = new JsonTextWriter(outputStreamWriter);
            var rowsMetadata = WriteIndexValuesToFile(indexValues, jsonWriter, outputFile);
            await outputStreamWriter.FlushAsync();
            var newMetadataOffset = outputFile.Position;
            var rootIndexRow = AddUpdatedMetadataToFile(rowsMetadata, jsonWriter);
            rootIndexRow.FileName = outputFilename;
            await outputStreamWriter.FlushAsync();
            var bloomOffset = outputFile.Position;
            AddBloomFilterToIndex(rowsMetadata, outputFile);
            await outputFile.FlushAsync();
            outputFile.WriteBinaryLongsToStream(new[] { newMetadataOffset, bloomOffset });
            return rootIndexRow;
        }

        private List<FileRowMetadata> WriteIndexValuesToFile(
            IEnumerable<IndexValueModel> indexValues, JsonTextWriter jsonWriter, Stream stream)
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
            JsonTextWriter jsonWriter)
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
    }
}
