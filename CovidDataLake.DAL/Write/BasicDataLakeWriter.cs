﻿using System;
using System.IO;
using System.Threading.Tasks;

namespace CovidDataLake.DAL.Write
{
    public class BasicDataLakeWriter : IDataLakeWriter
    {
        public Stream CreateFileStream(string fileType, out string filepath)
        {
            var path = GeneratePath(fileType);
            var fileStream = new FileStream(path, FileMode.Create);
            filepath = path;
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
            var path = string.Empty;
            return $"{path}.{fileType}";
        }
        
    }
}
