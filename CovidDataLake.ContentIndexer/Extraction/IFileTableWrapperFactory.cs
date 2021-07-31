using System;
using CovidDataLake.ContentIndexer.Extraction.TableWrappers;

namespace CovidDataLake.ContentIndexer.Extraction
{
    public interface IFileTableWrapperFactory
    {
        bool IsFileTypeSupported(string fileType);
        IFileTableWrapper CreateTableWrapperForFile(string filename);
    }
}
