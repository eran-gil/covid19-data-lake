using System;
using System.Collections.Generic;
using CovidDataLake.Cloud.Amazon;
using CovidDataLake.Common.Locking;
using CovidDataLake.Common.Probabilistic;

namespace CovidDataLake.MetadataIndexer.Indexing
{
    public class CountMinSketchMetadataIndexer : IMetadataIndexer
    {
        private readonly ILock _fileLock;
        private readonly IAmazonAdapter _amazonAdapter;

        public CountMinSketchMetadataIndexer(ILock fileLock, IAmazonAdapter amazonAdapter)
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
            var countMinSketch = new PythonCountMinSketch(0.9, 0.1);
            //write data
            countMinSketch.Add(data.Value);
            countMinSketch.ExportToFile(null);
            //upload back

            //release lock
            _fileLock.ReleaseLockAsync("");
        }
    }
}
