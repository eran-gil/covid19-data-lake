using System;
using System.Collections.Generic;
using CovidDataLake.Cloud.Amazon;
using CovidDataLake.Common.Locking;
using CovidDataLake.Common.Probabilistic;

namespace CovidDataLake.MetadataIndexer.Indexing
{
    class HyperLogLogMetadataIndexer : IMetadataIndexer
    {
        private readonly ILock _fileLock;
        private readonly IAmazonAdapter _amazonAdapter;

        public HyperLogLogMetadataIndexer(ILock fileLock, IAmazonAdapter amazonAdapter)
        {
            _fileLock = fileLock;
            _amazonAdapter = amazonAdapter;
        }
        public void IndexMetadata(KeyValuePair<string, string> data)
        {
            //lock index file
            _fileLock.TakeLockAsync("", TimeSpan.MaxValue);
            //get file from amazon
            //load into hyperloglog
            //if file does not exist
            var hyperloglog = new HyperLogLog();
            //write data
            hyperloglog.Add(data.Value);
            hyperloglog.SerializeToStream(null);
            //upload back

            //release lock
            _fileLock.ReleaseLockAsync("");
        }
    }
}
