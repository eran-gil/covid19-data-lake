using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace CovidDataLake.DAL.Utils
{
    public static class FilePathExtensions
    {
        public static string GetExtensionFromPath(this string path)
        {
            return Path.GetExtension(path);
        }
    }
}
