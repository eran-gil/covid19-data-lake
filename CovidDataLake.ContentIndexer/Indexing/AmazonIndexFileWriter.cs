using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using CovidDataLake.Amazon;
using CovidDataLake.ContentIndexer.Indexing.Models;
using Utf8Json;

namespace CovidDataLake.ContentIndexer.Indexing
{
    public class AmazonIndexFileWriter : IIndexFileWriter
    {
        private readonly IAmazonAdapter _amazonAdapter;
        private readonly string _bucketName; //TODO: initialize from config
        private readonly int _numOfRowsPerMetadataSection; //TODO: move to config

        public AmazonIndexFileWriter(IAmazonAdapter amazonAdapter)
        {
            _amazonAdapter = amazonAdapter;
            _bucketName = "test"; //TODO: inject from config
            _numOfRowsPerMetadataSection = 5;
        }

        public async Task UpdateIndexFileWithValues(IList<ulong> values, string indexFilename, string originFilename)
        {
            var downloadedFilename = await _amazonAdapter.DownloadObject(_bucketName, indexFilename);
            
            var originalIndexValues = GetIndexValuesFromFile(downloadedFilename);
            var indexValues = GetUpdatedIndexValues(originalIndexValues, values, originFilename);

            var outputFilename = downloadedFilename + "_new";
            using var outputFile = File.OpenWrite(outputFilename);
            using var outputStreamWriter = new StreamWriter(outputFile);
            var rowsMetadata = await WriteIndexValuesToFile(indexValues, outputStreamWriter);

            var newMetadataOffset = outputFile.Position;
            var metadataSections = CreateMetadataFromRows(rowsMetadata);

            foreach (var metadataSection in metadataSections)
            {
                await WriteObjectToFile(metadataSection, outputStreamWriter);
            }
            outputFile.Write(new byte[]{});
            WriteMetadataOffsetToFile(outputFile, newMetadataOffset);
            await _amazonAdapter.UploadObject(_bucketName, indexFilename, outputFilename);
        }

        private static async Task<List<FileRowMetadata>> WriteIndexValuesToFile(
            IAsyncEnumerable<IndexValueModel> indexValues, StreamWriter outputStreamWriter)
        {
            var rowsMetadata = new List<FileRowMetadata>();
            await foreach (var indexValue in indexValues)
            {
                var rowMetadata = new FileRowMetadata(outputStreamWriter.BaseStream.Position, indexValue.Value);
                rowsMetadata.Add(rowMetadata);
                await WriteObjectToFile(indexValue, outputStreamWriter);
            }
            return rowsMetadata;
        }

        private static async IAsyncEnumerable<IndexValueModel> GetIndexValuesFromFile(string filename)
        {
            if (new FileInfo(filename).Length == 0)
            {
                yield break;
            }
            using var inputFile = File.OpenRead(filename);
            inputFile.Seek(-(sizeof(long)), SeekOrigin.End);
            var metadataOffset = GetMetadataOffsetFromFile(inputFile);
            inputFile.Seek(0, SeekOrigin.Begin);
            using var inputStreamReader = new StreamReader(inputFile);
            while (inputFile.Position < metadataOffset)
            {
                var currentLine = await inputStreamReader.ReadLineAsync();
                var currentIndexValue = JsonSerializer.Deserialize<IndexValueModel>(currentLine);
                if (currentIndexValue == null)
                {
                    throw new InvalidDataException("The index is not in the expected format");
                }

                yield return currentIndexValue;
            }
        }

        private static async IAsyncEnumerable<IndexValueModel> GetUpdatedIndexValues(
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

        private static async Task WriteObjectToFile<T>(T indexValue, StreamWriter outputStreamWriter)
        {
            if (outputStreamWriter == null) throw new ArgumentNullException(nameof(outputStreamWriter));
            await JsonSerializer.SerializeAsync(outputStreamWriter.BaseStream, indexValue);
            await outputStreamWriter.WriteLineAsync("");
        }

        private static long GetMetadataOffsetFromFile(FileStream indexFile)
        {
            if (indexFile == null) throw new ArgumentNullException(nameof(indexFile));
            using var binaryReader = new BinaryReader(indexFile);
            var metadataIndex = binaryReader.ReadInt64();

            return metadataIndex;
        }

        private static void WriteMetadataOffsetToFile(Stream outputFile, long newMetadataOffset)
        {
            using var binaryWriter = new BinaryWriter(outputFile);
            binaryWriter.Write(newMetadataOffset);
        }
    }
}
