using System.IO;

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
