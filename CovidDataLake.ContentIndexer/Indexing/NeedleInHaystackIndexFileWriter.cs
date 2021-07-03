using System.Collections.Generic;
using System.Threading.Tasks;
using CovidDataLake.Common.Hashing;

namespace CovidDataLake.ContentIndexer.Indexing
{
    public class NeedleInHaystackIndexFileWriter : IIndexFileWriter
    {
        private readonly IStringHash _hash;

        public NeedleInHaystackIndexFileWriter(IStringHash hash)
        {
            _hash = hash;
        }

        public async Task WriteValuesToIndexFile(IEnumerable<ulong> values, string originFile)
        {
            /*
             *TODO:
             * for each value get exact file from metadata file and group them
             * write to each of the sub-files and split if necessary 
             * 
             */
        }

       
    }
}
