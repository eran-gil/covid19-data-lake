namespace CovidDataLake.ContentIndexer.Extraction.TableWrappers.Json;

class JsonFileTableWrapperFactory : IFileTableWrapperFactory
{
    public bool IsFileTypeSupported(string fileType)
    {
        return fileType.ToLowerInvariant() == ".json";
    }

    public IFileTableWrapper CreateTableWrapperForFile(string filename, string originFilename)
    {
        return new JsonFileTableWrapper(filename, originFilename);
    }
}
