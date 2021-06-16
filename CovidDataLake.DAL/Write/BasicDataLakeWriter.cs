using System;
using System.IO;

namespace CovidDataLake.DAL.Write
{
    public class BasicDataLakeWriter : IDataLakeWriter
    {
        public Stream CreateFileStream(string fileType)
        {
            var path = GeneratePath(fileType);
            var fileStream = new FileStream(path, FileMode.Create);
            return fileStream;
        }

        public bool WriteFile(byte[] bytes, string fileType)
        {
            var stream = CreateFileStream(fileType);
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

        private string GeneratePath(string fileType)
        {
            var path = string.Empty;
            return $"{path}.{fileType}";
        }
        
    }
}
