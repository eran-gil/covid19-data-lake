using System;
using System.IO;
using System.Threading.Tasks;
using CovidDataLake.Amazon;

namespace CovidDataLake.Storage.Write
{
    public class AmazonDataLakeWriter : IDataLakeWriter
    {
        private readonly IAmazonAdapter _amazonAdapter;
        private readonly string _bucketName;
        private const string TempFolderPath = "temp";

        public AmazonDataLakeWriter(IAmazonAdapter amazonAdapter)
        {
            _amazonAdapter = amazonAdapter;
            _bucketName = "test"; //TODO: inject from config
        }

        public string GenerateFilePath(string fileType)
        {
            var today = DateTime.Today;
            var path = $"files\\{today.Year}\\{today.Month}\\{today.Day}";
            var filename = Guid.NewGuid().ToString();
            return $"{path}\\{filename}{fileType}";
        }

        public Stream CreateFileStream(string filepath)
        {
            var tempFilePath = GetTempFilePathFromOriginalFilename(filepath);
            var fileStream = File.OpenWrite(tempFilePath);
            var amazonStream = new AmazonDataLakeStream(_amazonAdapter, fileStream, _bucketName, filepath);
            return amazonStream;
        }

        private static string GetTempFilePathFromOriginalFilename(string filepath)
        {
            var filename = Path.GetFileName(filepath);
            var tempFilePath = TempFolderPath + "\\" + filename;
            return tempFilePath;
        }

        public async Task DeleteFileAsync(string filePath)
        {
            await _amazonAdapter.DeleteObject(_bucketName, filePath);
        }
    }
}
