using System.IO;

namespace CovidDataLake.DAL.Write
{
    public interface IDataLakeWriter
    {
        Stream CreateFileStream(string fileType);
        bool WriteFile(byte[] bytes, string fileType);
    }
}