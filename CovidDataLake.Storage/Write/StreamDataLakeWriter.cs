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

        public Stream CreateFileStream(string fileType, out string filepath)
        {
            filepath = GeneratePath(fileType);
            var directoryPath = Path.GetDirectoryName(filepath);
            Directory.CreateDirectory(directoryPath);
            var fileStream = File.Create(filepath);
            return fileStream;
        }

        public bool WriteFile(byte[] bytes, string fileType)
        {
            var stream = CreateFileStream(fileType, out _);
            try
            {
                stream.Write(bytes);
                return true;
            }
            catch (Exception)
            {
                //TODO: write log
                return false;
            }
        }

        public Task DeleteFileAsync(string filePath)
        {
            var fi = new FileInfo(filePath);
            return Task.Factory.StartNew(() => fi.Delete());
        }

        private string GeneratePath(string fileType)
        {
            var today = DateTime.Today;
            var path = $"{_configuration.BasePath}\\{today.Year}\\{today.Month}\\{today.Day}";
            var filename = Guid.NewGuid().ToString();
            return $"{path}\\{filename}{fileType}";
        }
        
    }
}
