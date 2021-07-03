using System.Collections.Generic;
using System.Threading.Tasks;

namespace CovidDataLake.ContentIndexer.Indexing
{
    public interface IIndexFileWriter
    {
        Task WriteValuesToIndexFile(IEnumerable<ulong> values, string originFile);
    }
}