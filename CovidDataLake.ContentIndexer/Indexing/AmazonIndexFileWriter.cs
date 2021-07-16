using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;
using CovidDataLake.ContentIndexer.Indexing.Models;
using Utf8Json;

namespace CovidDataLake.ContentIndexer.Indexing
{
    public class AmazonIndexFileWriter : IIndexFileWriter
    {
        private readonly string _bucketName; //TODO: initialize from config
        private readonly AmazonS3Client _awsClient;
        private readonly int _numOfRowsPerMetadataSection; //TODO: move to config

        public AmazonIndexFileWriter()
        {
            _awsClient = new AmazonS3Client(); //TODO: inject factory
            _bucketName = "test"; //TODO: inject from config
            _numOfRowsPerMetadataSection = 5;
        }

        public async Task UpdateIndexFileWithValues(IList<ulong> values, string indexFilename, string originFilename)
        {
            var downloadedFilename = await DownloadIndexFile(indexFilename);
            using var inputIndexFile = File.OpenRead(downloadedFilename);

            inputIndexFile.Seek(-4, SeekOrigin.End);
            var metadataOffset = GetMetadataOffsetFromFile(inputIndexFile);
            inputIndexFile.Seek(0, SeekOrigin.Begin);

            var indexValues = GetIndexValuesToWriteFromFile(values, originFilename, metadataOffset, inputIndexFile);

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

            WriteMetadataOffsetToFile(outputFile, newMetadataOffset);
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

        private static async IAsyncEnumerable<IndexValueModel> GetIndexValuesToWriteFromFile(IList<ulong> values,
            string originFilename, long metadataOffset, Stream inputFile)
        {
            using var inputStreamReader = new StreamReader(inputFile);

            while (inputFile.Position < metadataOffset)
            {
                var currentLine = await inputStreamReader.ReadLineAsync();
                var currentIndexValue = JsonSerializer.Deserialize<IndexValueModel>(currentLine);
                if (currentIndexValue == null || !values.Any())
                {
                    throw new InvalidDataException("The index is not in the expected format");
                }

                var currentInputValue = values.First();
                if (currentInputValue == currentIndexValue.Value)
                {
                    currentIndexValue.Files.Add(originFilename);
                    values.RemoveAt(0);
                }

                if (currentInputValue < currentIndexValue.Value)
                {
                    var newIndexValue = new IndexValueModel(currentInputValue, new List<string> {originFilename});
                    values.RemoveAt(0);
                    yield return newIndexValue;
                }

                yield return currentIndexValue;
            }
        }

        private static void WriteMetadataOffsetToFile(FileStream outputFile, long newMetadataOffset)
        {
            using var binaryWriter = new BinaryWriter(outputFile);
            binaryWriter.Write(newMetadataOffset);
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

        private async Task<string> DownloadIndexFile(string indexFilename)
        {
            var downloadedFilename = Guid.NewGuid().ToString();
            var getRequest = new GetObjectRequest
            {
                BucketName = _bucketName,
                Key = indexFilename
            };
            using var response = await _awsClient.GetObjectAsync(getRequest);
            await response.WriteResponseStreamToFileAsync(downloadedFilename, false, CancellationToken.None);

            return downloadedFilename;
        }
    }
}