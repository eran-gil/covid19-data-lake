namespace CovidDataLake.ContentIndexer.Indexing
{
    public interface IContentIndexer
    {
        void IndexCsv(object csv);
    }
}
