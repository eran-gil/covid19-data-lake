using System;
using CovidDataLake.ContentIndexer.TableWrappers;

namespace CovidDataLake.ContentIndexer.Extraction
{
    public interface IFileTableWrapperFactory : IDisposable
    {
        bool IsFileTypeSupported(string fileType);
        IFileTableWrapper CreateTableWrapperForFile(string filename);
    }
}
