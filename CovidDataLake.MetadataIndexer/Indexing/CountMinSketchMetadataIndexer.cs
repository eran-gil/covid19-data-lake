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
            : base(fileLock, amazonAdapter, TimeSpan.FromSeconds(configuration.LockIntervalInSeconds), configuration.BucketName) //todo: get from config
        {
            _confidence = configuration.Confidence;
            _errorRate = configuration.ErrorRate;
        }

        protected override string IndexFolder => "CMS_FREQUENCY";
        protected override string FileType => "cms";
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
