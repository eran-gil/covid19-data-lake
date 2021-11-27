using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using CovidDataLake.Bloom;
using CovidDataLake.Common.Files;
using CovidDataLake.ContentIndexer.Configuration;
using CovidDataLake.ContentIndexer.Extensions;
using CovidDataLake.ContentIndexer.Indexing.Models;
using CovidDataLake.Storage.Utils;

namespace CovidDataLake.ContentIndexer.Indexing
{
    public class NeedleInHaystackIndexFileAccess : IIndexFileAccess
    {
        private readonly int _numOfRowsPerMetadataSection;
        private readonly double _bloomFilterErrorRate;
        private readonly int _bloomFilterCapacity;
        private readonly int _maxRowsPerFile;

        public NeedleInHaystackIndexFileAccess(NeedleInHaystackIndexConfiguration configuration)
        {
            _numOfRowsPerMetadataSection = configuration.NumOfMetadataRows;
            _bloomFilterErrorRate = configuration.BloomFilterErrorRate;
            _bloomFilterCapacity = configuration.BloomFilterCapacity;
            _maxRowsPerFile = configuration.MaxRowsPerFile;
        }

        public async Task<IList<RootIndexRow>> CreateUpdatedIndexFileWithValues(string sourceIndexFileName, IList<string> values,
            string valuesFileName
        )
        {
            var originalIndexValues = AsyncEnumerable.Empty<IndexValueModel>();

                using var fileStream = OptionalFileStream.CreateOptionalFileReadStream(sourceIndexFileName);
            if (fileStream.BaseStream != null)
            {
                originalIndexValues = GetIndexValuesFromFile(fileStream.BaseStream);
            }

            var indexValues = MergeIndexWithUpdatedValues(originalIndexValues, values, valuesFileName);

            var rootIndexRows = new List<RootIndexRow>();
            var indexValueBatches = indexValues.Batch(_maxRowsPerFile);
            await foreach (var batch in indexValueBatches)
            {
                var outputFilename = Guid.NewGuid().ToString();
                var rootIndexRow = await MergeIndexValuesToFile(batch, outputFilename);
                rootIndexRows.Add(rootIndexRow);
            }
            
            return rootIndexRows;
        }

        private static IAsyncEnumerable<IndexValueModel> GetIndexValuesFromFile(FileStream inputFile)
        {
            inputFile.Seek(-(2 * sizeof(long)), SeekOrigin.End);
            var metadataOffset = inputFile.ReadBinaryLongFromStream();
            inputFile.Seek(0, SeekOrigin.Begin);
            var rows = inputFile.GetDeserializedRowsFromFileAsync<IndexValueModel>(metadataOffset);
            return rows;
        }

        private static async IAsyncEnumerable<IndexValueModel> MergeIndexWithUpdatedValues(
            IAsyncEnumerable<IndexValueModel> originalIndexValues,
            IList<string> newValues, string valuesFileName)
        {
            await foreach (var indexValue in originalIndexValues)
            {
                if (!newValues.Any())
                {
                    yield return indexValue;
                    continue;
                }

                var currentInputValue = newValues.First();
                if (currentInputValue == indexValue.Value)
                {
                    indexValue.Files.Add(valuesFileName);
                    newValues.RemoveAt(0);
                }

                if (string.Compare(currentInputValue, indexValue.Value, StringComparison.Ordinal) < 0)
                {
                    var newIndexValue = new IndexValueModel(currentInputValue, new List<string> {valuesFileName});
                    newValues.RemoveAt(0);
                    yield return newIndexValue;
                }

                yield return indexValue;
            }

            if (!newValues.Any()) yield break;
            foreach (var currentValue in newValues)
            {
                var newIndexValue = new IndexValueModel(currentValue, new List<string> {valuesFileName});
                yield return newIndexValue;
            }
        }

        private async Task<RootIndexRow> MergeIndexValuesToFile(IAsyncEnumerable<IndexValueModel> indexValues, string outputFilename)
        {
            using var outputFile = FileCreator.OpenFileWriteAndCreatePath(outputFilename);
            using var outputStreamWriter = new StreamWriter(outputFile);
            var rowsMetadata = await WriteIndexValuesToFile(indexValues, outputStreamWriter);
            await outputStreamWriter.FlushAsync();
            var newMetadataOffset = outputFile.Position;
            var rootIndexRow = await AddUpdatedMetadataToFile(rowsMetadata, outputStreamWriter);
            rootIndexRow.FileName = outputFilename;
            await outputStreamWriter.FlushAsync();
            var bloomOffset = outputFile.Position;
            await AddBloomFilterToIndex(rowsMetadata, outputFile);
            await outputFile.FlushAsync();
            outputFile.WriteBinaryLongsToStream(new[] {newMetadataOffset, bloomOffset});
            return rootIndexRow;
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

        private async Task<RootIndexRow> AddUpdatedMetadataToFile(IList<FileRowMetadata> rowsMetadata,
            StreamWriter outputStreamWriter)
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

        private async Task AddBloomFilterToIndex(IEnumerable<FileRowMetadata> rows, Stream outputStream)
        {
            var bloomFilter = GetBloomFilter();
            foreach (var row in rows)
            {
                bloomFilter.AddToFilter(row.Value);
            }

            var outputBloomFilter = bloomFilter.Serialize();
            await outputStream.WriteAsync(outputBloomFilter);
        }

        private PythonBloomFilter GetBloomFilter(byte[] serializedBloomFilter = null)
        {
            // ReSharper disable once ConvertIfStatementToReturnStatement
            if (serializedBloomFilter == null)
            {
                return new PythonBloomFilter(_bloomFilterCapacity, _bloomFilterErrorRate);
            }

            return new PythonBloomFilter(serializedBloomFilter);
        }

        /*private static byte[] GetSerializedBloomFilterFromFile(OptionalFileStream stream)
        {
            var inputFile = stream.BaseStream;
            if (inputFile == null)
            {
                return null;
            }

            inputFile.Seek(-(sizeof(long)), SeekOrigin.End);
            var bloomOffset = inputFile.ReadBinaryLongFromStream();
            inputFile.Seek(bloomOffset, SeekOrigin.Begin);
            var bloomOffsetLength = (inputFile.Length - 2 * sizeof(long)) - bloomOffset;
            var serializedBloomFilter = new byte[bloomOffsetLength];
            inputFile.ReadAsync(serializedBloomFilter);
            return serializedBloomFilter;
        }*/

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