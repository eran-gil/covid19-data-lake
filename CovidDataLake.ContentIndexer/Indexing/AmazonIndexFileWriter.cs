﻿using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using CovidDataLake.Cloud.Amazon;
using CovidDataLake.Cloud.Amazon.Configuration;
using CovidDataLake.Common;
using CovidDataLake.ContentIndexer.Extraction.Models;
using CovidDataLake.ContentIndexer.Indexing.Models;

namespace CovidDataLake.ContentIndexer.Indexing
{
    public class AmazonIndexFileWriter : IIndexFileWriter
    {
        private readonly IAmazonAdapter _amazonAdapter;
        private readonly IIndexFileAccess _indexFileAccess;
        private readonly string _bucketName;

        public AmazonIndexFileWriter(IAmazonAdapter amazonAdapter, BasicAmazonIndexFileConfiguration configuration, IIndexFileAccess indexFileAccess)
        {
            _amazonAdapter = amazonAdapter;
            _indexFileAccess = indexFileAccess;
            _bucketName = configuration.BucketName;
        }

        public async Task<IEnumerable<RootIndexRow>> UpdateIndexFileWithValues(IList<RawEntry> values, string indexFilename, string columnName)
        {
            var downloadedFilename =
                CreateNewColumnIndexFileName(columnName);
            if (indexFilename != CommonKeys.END_OF_INDEX_FLAG)
            {
                downloadedFilename = await _amazonAdapter.DownloadObjectAsync(_bucketName, indexFilename);
            }
            else
            {
                indexFilename = downloadedFilename;
            }

            var rootIndexRows = _indexFileAccess.CreateUpdatedIndexFileWithValues(downloadedFilename, values);
            var localFileNames = new Dictionary<string, string>();
            for (var i = 0; i < rootIndexRows.Count; i++)
            {
                var rootIndexRow = rootIndexRows[i];
                var localFileName = rootIndexRow.FileName;
                rootIndexRow.FileName = i == 0 ? indexFilename : CreateNewColumnIndexFileName(columnName);
                localFileNames[rootIndexRow.FileName] = localFileName;
            }

            await UploadIndexFiles(rootIndexRows, localFileNames);

            return rootIndexRows;
        }

        private async Task UploadIndexFiles(IList<RootIndexRow> rootIndexRows, Dictionary<string, string> localFileNames)
        {
            await Parallel.ForEachAsync(rootIndexRows, (async (row, _) =>
            {
                var localFileName = localFileNames[row.FileName];
                await _amazonAdapter.UploadObjectAsync(_bucketName, row.FileName, localFileName);
            }));
        }

        private static string CreateNewColumnIndexFileName(string columnName)
        {
            return $"{CommonKeys.INDEX_FOLDER_NAME}/{CommonKeys.COLUMN_INDICES_FOLDER_NAME}/{columnName}/{Guid.NewGuid()}.txt";
        }
    }
}
