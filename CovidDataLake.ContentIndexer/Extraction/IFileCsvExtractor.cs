namespace CovidDataLake.ContentIndexer.Extraction
{
    public interface IFileCsvExtractor
    {
        bool IsFileTypeSupported(string fileType);
        object ExtractCsvFromFile(string filename);
    }
}
