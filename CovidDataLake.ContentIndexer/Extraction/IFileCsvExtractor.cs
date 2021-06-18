using System.Collections.Generic;

namespace CovidDataLake.ContentIndexer.Extraction
{
    public interface IFileCsvExtractor
    {
        bool IsFileTypeSupported(string fileType);
        IEnumerable<object> ExtractCsvFromFile(string filename);
    }
}
