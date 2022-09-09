namespace CovidDataLake.MetadataIndexer.Indexing
{
    public interface IMetadataIndexer
    {
        Task IndexMetadata(KeyValuePair<string, List<string>> data);
    }
}
