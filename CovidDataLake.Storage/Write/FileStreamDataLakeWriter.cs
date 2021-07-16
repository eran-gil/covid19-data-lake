using System;
using System.IO;
using System.Threading.Tasks;

namespace CovidDataLake.Storage.Write
{
    public class StreamDataLakeWriter : IDataLakeWriter
    {
        private readonly DataLakeWriterConfiguration _configuration;

        public StreamDataLakeWriter(DataLakeWriterConfiguration configuration)
        {
            _configuration = configuration;
        }

        public string GenerateFilePath(string fileType)
        {
            var today = DateTime.Today;
            var path = $"{_configuration.BasePath}\\{today.Year}\\{today.Month}\\{today.Day}";
            var filename = Guid.NewGuid().ToString();
            return $"{path}\\{filename}{fileType}";
        }

        public Stream CreateFileStream(string filepath)
        {
            var directoryPath = Path.GetDirectoryName(filepath);
            Directory.CreateDirectory(directoryPath);
            var fileStream = File.Create(filepath);
            return fileStream;
        }

        public Task DeleteFileAsync(string filePath)
        {
            var fi = new FileInfo(filePath);
            return Task.Factory.StartNew(() => fi.Delete());
        }

        
        
    }
}
