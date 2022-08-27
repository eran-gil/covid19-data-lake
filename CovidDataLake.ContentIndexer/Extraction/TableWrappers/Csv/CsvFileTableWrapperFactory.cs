namespace CovidDataLake.ContentIndexer.Extraction.TableWrappers.Csv
{
    class CsvFileTableWrapperFactory : IFileTableWrapperFactory
    {
        public bool IsFileTypeSupported(string fileType)
        {
            return fileType.ToLowerInvariant() == ".csv";
        }

        public IFileTableWrapper CreateTableWrapperForFile(string filename, string originFilename)
        {
            return new CsvFileTableWrapper(filename, originFilename);
        }
    }
}
