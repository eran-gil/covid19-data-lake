using System;
using System.IO;
using CovidDataLake.Cloud.Amazon;
using CovidDataLake.Common;
using CovidDataLake.Common.Locking;
using CovidDataLake.Common.Probabilistic;
using CovidDataLake.MetadataIndexer.Indexing.Configuration;

namespace CovidDataLake.MetadataIndexer.Indexing
{
    public class HyperLogLogMetadataIndexer : ProbabilisticMetadataIndexerBase<HyperLogLog>
    {
        public HyperLogLogMetadataIndexer(ILock fileLock, IAmazonAdapter amazonAdapter, HyperLogLogMetadataIndexConfiguration configuration)
            : base(fileLock, amazonAdapter, TimeSpan.FromSeconds(configuration.LockIntervalInSeconds), configuration.BucketName) //todo: get from config
        {
        }
        
        protected override string IndexFolder => "HLL_Cardinality";
        protected override string FileType => "hll";
        protected override HyperLogLog GetIndexObjectFromFile(string indexFile)
        {
            if (string.IsNullOrEmpty(indexFile))
            {
                return new HyperLogLog();
            }
            using var indexFileStream = File.OpenRead(indexFile);
            var hll = new HyperLogLog(indexFileStream);
            return hll;
        }

        protected override string WriteIndexObjectToFile(HyperLogLog indexObject)
        {
            var outputFileName = Path.Combine(CommonKeys.TEMP_FOLDER_NAME, Guid.NewGuid().ToString());
            var outputStream = File.OpenRead(outputFileName);
            indexObject.SerializeToStream(outputStream);
            return outputFileName;
        }

        protected override void UpdateIndexObjectWithMetadata(HyperLogLog indexObject, string metadataValue)
        {
            indexObject.Add(metadataValue);
        }
    }
}
