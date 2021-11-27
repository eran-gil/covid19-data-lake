using System;
using System.IO;
using System.Threading.Tasks;
using CovidDataLake.Storage.Utils;

namespace CovidDataLake.Storage.Write
{
    public class FileStreamDataLakeWriter : IDataLakeWriter
    {
        private readonly DataLakeWriterConfiguration _configuration;

        public FileStreamDataLakeWriter(DataLakeWriterConfiguration configuration)
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
            var fileStream = FileCreator.CreateFileAndPath(filepath);
            return fileStream;
        }

        public Task DeleteFileAsync(string filePath)
        {
            var fi = new FileInfo(filePath);
            return Task.Factory.StartNew(() => fi.Delete());
        }



    }
}
