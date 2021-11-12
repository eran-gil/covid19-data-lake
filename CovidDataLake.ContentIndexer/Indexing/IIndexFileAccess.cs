using System.Collections.Generic;
using System.Threading.Tasks;
using CovidDataLake.ContentIndexer.Indexing.Models;

namespace CovidDataLake.ContentIndexer.Indexing
{
    public interface IIndexFileAccess
    {
        Task<IList<RootIndexRow>> CreateUpdatedIndexFileWithValues(string sourceIndexFileName,
            IList<ulong> values,
            string valuesFileName
        );
    }
}