using System.IO;
using System.Threading.Tasks;

namespace CovidDataLake.DAL.Write
{
    public interface IDataLakeWriter
    {
        Stream CreateFileStream(string fileType, out string filepath);
        bool WriteFile(byte[] bytes, string fileType);
        Task DeleteFileAsync(string filePath);
    }
}