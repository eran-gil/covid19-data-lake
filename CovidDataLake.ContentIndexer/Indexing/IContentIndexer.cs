using CovidDataLake.ContentIndexer.TableWrappers;

namespace CovidDataLake.ContentIndexer.Indexing
{
    public interface IContentIndexer
    {
        void IndexTable(IFileTableWrapper tableWrapper);
    }
}
