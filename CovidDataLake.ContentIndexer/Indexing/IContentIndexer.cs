using System.Collections.Generic;
using System.Threading.Tasks;
using CovidDataLake.ContentIndexer.Extraction.TableWrappers;

namespace CovidDataLake.ContentIndexer.Indexing
{
    public interface IContentIndexer
    {
        Task IndexTableAsync(IEnumerable<IFileTableWrapper> tableWrappers);
    }
}
