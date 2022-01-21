using System;
using System.IO;
using CovidDataLake.Cloud.Amazon;
using CovidDataLake.Common;
using CovidDataLake.Common.Locking;
using CovidDataLake.Common.Probabilistic;
using CovidDataLake.MetadataIndexer.Indexing.Configuration;

namespace CovidDataLake.MetadataIndexer.Indexing
{
    public class CountMinSketchMetadataIndexer : ProbabilisticMetadataIndexerBase<StringCountMinSketch>
    {
        private readonly double _confidence;
        private readonly double _errorRate;

        public CountMinSketchMetadataIndexer(ILock fileLock, IAmazonAdapter amazonAdapter, CountMinSketchMetadataIndexConfiguration configuration)
            : base(fileLock, amazonAdapter, TimeSpan.FromSeconds(configuration.LockIntervalInSeconds), configuration.BucketName)
        {
            _confidence = configuration.Confidence;
            _errorRate = configuration.ErrorRate;
        }

        protected override string IndexFolder => CommonKeys.CMS_FOLDER_NAME;
        protected override string FileType => CommonKeys.CMS_FILE_TYPE;
        protected override StringCountMinSketch GetIndexObjectFromFile(string indexFile)
        {
            if (string.IsNullOrEmpty(indexFile))
            {
                return new StringCountMinSketch(_confidence, _errorRate);
            }

            using var stream = File.OpenRead(indexFile);
            var hll = new StringCountMinSketch(stream);
            return hll;
        }

        protected override string WriteIndexObjectToFile(StringCountMinSketch indexObject)
        {
            var outputFileName = Path.Combine(CommonKeys.TEMP_FOLDER_NAME, Guid.NewGuid().ToString());
            using var stream = File.OpenWrite(outputFileName);
            indexObject.SerializeToStream(stream);
            return outputFileName;
        }

        protected override void UpdateIndexObjectWithMetadata(StringCountMinSketch indexObject, string metadataValue)
        {
            indexObject.Add(metadataValue);
        }
    }
}
