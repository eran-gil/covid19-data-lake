using System.IO;
using System.Threading.Tasks;

namespace CovidDataLake.Storage.Write
{
    public interface IDataLakeWriter
    {
        string GenerateFilePath(string fileType);
        Stream CreateFileStream(string filepath);
        Task DeleteFileAsync(string filePath);
    }
}