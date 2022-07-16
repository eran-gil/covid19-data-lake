namespace CovidDataLake.ContentIndexer.Extraction.TableWrappers.Json;

class JsonFileTableWrapperFactory : IFileTableWrapperFactory
{
    public bool IsFileTypeSupported(string fileType)
    {
        return fileType.ToLowerInvariant() == ".json";
    }

    public IFileTableWrapper CreateTableWrapperForFile(string filename)
    {
        return new JsonFileTableWrapper(filename);
    }
}
