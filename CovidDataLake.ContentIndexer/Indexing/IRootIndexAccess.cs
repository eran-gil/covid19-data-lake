using System.Threading.Tasks;
using CovidDataLake.ContentIndexer.Indexing.Models;

namespace CovidDataLake.ContentIndexer.Indexing
{
    public interface IRootIndexAccess
    {
        Task UpdateColumnRanges(RootIndexColumnMappings columnMappings);
        Task<string> GetFileNameForColumnAndValue(string column, ulong val);
    }
}
