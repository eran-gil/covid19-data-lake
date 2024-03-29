﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using CovidDataLake.Amazon;
using CovidDataLake.ContentIndexer.Extensions;
using CovidDataLake.ContentIndexer.Indexing.Models;

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
            //todo: handle split if necessary
            //todo: handle file does not exist yet
            var downloadedFilename = await _amazonAdapter.DownloadObjectAsync(_bucketName, indexFilename);
            
            var originalIndexValues = GetIndexValuesFromFile(downloadedFilename);
            var indexValues = MergeIndexWithUpdatedValues(originalIndexValues, values, originFilename);

            var outputFilename = downloadedFilename + "_new";
            using var outputFile = File.OpenWrite(outputFilename);
            using var outputStreamWriter = new StreamWriter(outputFile);
            var rowsMetadata = await WriteIndexValuesToFile(indexValues, outputStreamWriter);

            var newMetadataOffset = outputFile.Position;
            var metadataSections = CreateMetadataFromRows(rowsMetadata);

            foreach (var metadataSection in metadataSections)
            {
                await outputStreamWriter.WriteObjectToLineAsync(metadataSection);
            }
            outputFile.Write(new byte[]{});
            outputFile.WriteBinaryLongToStream(newMetadataOffset);
            await _amazonAdapter.UploadObjectAsync(_bucketName, indexFilename, outputFilename);
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
            inputFile.Seek(-(sizeof(long)), SeekOrigin.End);
            var metadataOffset = inputFile.ReadBinaryLongFromStream();
            inputFile.Seek(0, SeekOrigin.Begin);
            var rows = inputFile.GetDeserializedRowsFromFileAsync<IndexValueModel>(metadataOffset);
            return rows;
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
