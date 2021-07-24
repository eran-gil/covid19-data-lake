using System.Collections.Generic;
using System.Threading.Tasks;
using CovidDataLake.ContentIndexer.Indexing.Models;

namespace CovidDataLake.ContentIndexer.Indexing
{
    public interface IRootIndexAccess
    {
        Task UpdateColumnRanges(SortedSet<RootIndexColumnUpdate> columnMappings);
        Task<string> GetFileNameForColumnAndValue(string column, ulong val);
    }
}
