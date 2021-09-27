﻿using CovidDataLake.ContentIndexer.Extraction.TableWrappers;

namespace CovidDataLake.ContentIndexer.Extraction
{
    class CsvFileTableWrapperFactory : IFileTableWrapperFactory
    {
        public bool IsFileTypeSupported(string fileType)
        {
            return fileType.ToLowerInvariant() == ".csv";
        }

        public IFileTableWrapper CreateTableWrapperForFile(string filename)
        {
            return new CsvFileTableWrapper(filename);
        }
    }
}