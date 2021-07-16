using System.Collections.Generic;
using System.Threading.Tasks;

namespace CovidDataLake.ContentIndexer.Indexing
{
    public interface IIndexFileWriter
    {
        Task UpdateIndexFileWithValues(IList<ulong> values, string indexFilename, string originFilename);
    }
}