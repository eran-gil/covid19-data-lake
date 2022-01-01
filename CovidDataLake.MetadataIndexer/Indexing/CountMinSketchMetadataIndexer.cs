using System;
using System.IO;
using CovidDataLake.Cloud.Amazon;
using CovidDataLake.Common;
using CovidDataLake.Common.Locking;
using CovidDataLake.Common.Probabilistic;
using CovidDataLake.MetadataIndexer.Indexing.Configuration;

namespace CovidDataLake.MetadataIndexer.Indexing
{
    public class CountMinSketchMetadataIndexer : ProbabilisticMetadataIndexerBase<PythonCountMinSketch>
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
        protected override PythonCountMinSketch GetIndexObjectFromFile(string indexFile)
        {
            if (string.IsNullOrEmpty(indexFile))
            {
                return new PythonCountMinSketch(_confidence, _errorRate);
            }
            var hll = new PythonCountMinSketch(indexFile);
            return hll;
        }

        protected override string WriteIndexObjectToFile(PythonCountMinSketch indexObject)
        {
            var outputFileName = Path.Combine(CommonKeys.TEMP_FOLDER_NAME, Guid.NewGuid().ToString());
            indexObject.ExportToFile(outputFileName);
            return outputFileName;
        }

        protected override void UpdateIndexObjectWithMetadata(PythonCountMinSketch indexObject, string metadataValue)
        {
            indexObject.Add(metadataValue);
        }
    }
}
