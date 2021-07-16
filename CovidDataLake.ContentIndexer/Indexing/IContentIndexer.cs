using System.Threading.Tasks;
using CovidDataLake.ContentIndexer.TableWrappers;

namespace CovidDataLake.ContentIndexer.Indexing
{
    public interface IContentIndexer
    {
        Task IndexTable(IFileTableWrapper tableWrapper);
    }
}
