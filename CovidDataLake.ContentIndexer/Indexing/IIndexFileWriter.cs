using System.Collections.Generic;
using System.Threading.Tasks;
using CovidDataLake.ContentIndexer.Indexing.Models;

namespace CovidDataLake.ContentIndexer.Indexing
{
    public interface IIndexFileWriter
    {
        Task<IEnumerable<RootIndexRow>> UpdateIndexFileWithValues(IList<ulong> values, string indexFilename, string originFilename);
    }
}