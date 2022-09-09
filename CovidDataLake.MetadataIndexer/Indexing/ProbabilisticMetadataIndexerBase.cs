using CovidDataLake.Cloud.Amazon;
using CovidDataLake.Common;
using CovidDataLake.Common.Locking;

namespace CovidDataLake.MetadataIndexer.Indexing
{
    public abstract class ProbabilisticMetadataIndexerBase<T> : IMetadataIndexer
    {
        private readonly ILock _fileLock;
        private readonly IAmazonAdapter _amazonAdapter;
        private readonly TimeSpan _lockTimeSpan;
        private readonly string _bucketName;

        protected ProbabilisticMetadataIndexerBase(ILock fileLock, IAmazonAdapter amazonAdapter, TimeSpan lockTimeSpan, string bucketName)
        {
            _fileLock = fileLock;
            _amazonAdapter = amazonAdapter;
            _lockTimeSpan = lockTimeSpan;
            _bucketName = bucketName;
        }

        protected abstract string IndexFolder { get; }
        protected abstract string FileType { get; }

        public async Task IndexMetadata(KeyValuePair<string, List<string>> data)
        {
            var indexFileName = GetIndexFilePath(data.Key);
            _fileLock.TakeLock(indexFileName, _lockTimeSpan);

            string? downloadedIndexFile;
            try
            {
                downloadedIndexFile = await _amazonAdapter.DownloadObjectAsync(_bucketName, indexFileName);
            }
            catch (Exception)
            {
                downloadedIndexFile = null;
            }

            var indexObject = GetIndexObjectFromFile(downloadedIndexFile);
            UpdateIndexObjectWithMetadata(indexObject, data.Value);
            var outputFileName = WriteIndexObjectToFile(indexObject);
            await _amazonAdapter.UploadObjectAsync(_bucketName, indexFileName, outputFileName);
            _fileLock.ReleaseLock(indexFileName);
        }

        protected abstract T GetIndexObjectFromFile(string? indexFile);

        protected abstract string WriteIndexObjectToFile(T indexObject);

        protected abstract void UpdateIndexObjectWithMetadata(T indexObject, List<string> metadataValues);

        private string GetIndexFilePath(string metadataName)
        {
            return $"{CommonKeys.METADATA_INDICES_FOLDER_NAME}/{IndexFolder}/{metadataName}.{FileType}";
        }
    }
}
