using System.Threading.Tasks;

namespace CovidDataLake.ContentIndexer.Indexing
{
    public interface IRootIndexAccess
    {
        Task UpdateRanges(object mapping);
        Task<string> GetFileNameForColumnAndValue(string column, ulong val);
    }
}
