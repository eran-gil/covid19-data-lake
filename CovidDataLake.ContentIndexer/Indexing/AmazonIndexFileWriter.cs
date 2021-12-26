using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using CovidDataLake.Cloud.Amazon;
using CovidDataLake.Common;
using CovidDataLake.ContentIndexer.Configuration;
using CovidDataLake.ContentIndexer.Indexing.Models;

namespace CovidDataLake.ContentIndexer.Indexing
{
    public class AmazonIndexFileWriter : IIndexFileWriter
    {
        private readonly IAmazonAdapter _amazonAdapter;
        private readonly IIndexFileAccess _indexFileAccess;
        private readonly string _bucketName;

        public AmazonIndexFileWriter(IAmazonAdapter amazonAdapter, BasicAmazonIndexConfiguration configuration, IIndexFileAccess indexFileAccess)
        {
            _amazonAdapter = amazonAdapter;
            _indexFileAccess = indexFileAccess;
            _bucketName = configuration.BucketName;
        }

        public async Task<IEnumerable<RootIndexRow>> UpdateIndexFileWithValues(IList<string> values,
            string indexFilename, string originFilename)
        {
            //todo: test after refactor
            //todo: test update integrity
            var downloadedFilename =
                CreateNewColumnIndexFileName();
            if (indexFilename != CommonKeys.END_OF_INDEX_FLAG)
            {
                downloadedFilename = await _amazonAdapter.DownloadObjectAsync(_bucketName, indexFilename);
            }
            else
            {
                indexFilename = downloadedFilename;
            }

            var rootIndexRows = await _indexFileAccess.CreateUpdatedIndexFileWithValues(downloadedFilename, values, originFilename);
            for (var i = 0; i < rootIndexRows.Count; i++)
            {
                var rootIndexRow = rootIndexRows[i];
                var localFileName = rootIndexRow.FileName;
                rootIndexRow.FileName = i == 0 ? indexFilename : CreateNewColumnIndexFileName();
                await _amazonAdapter.UploadObjectAsync(_bucketName, rootIndexRow.FileName, localFileName);
            }

            return rootIndexRows;
        }

        private static string CreateNewColumnIndexFileName()
        {
            return $"{CommonKeys.INDEX_FOLDER_NAME}/{CommonKeys.COLUMN_INDICES_FOLDER_NAME}/{Guid.NewGuid()}.txt";
        }
    }
}
