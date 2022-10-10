using System.Collections.Generic;
using System.Threading.Tasks;
using CovidDataLake.ContentIndexer.Indexing.Models;

namespace CovidDataLake.ContentIndexer.Indexing.NeedleInHaystack.RootIndex
{
    public interface IRootIndexCache : IRootIndexAccess
    {
        Task LoadAllEntries(IEnumerable<RootIndexRow> indexRows);

    }
}
