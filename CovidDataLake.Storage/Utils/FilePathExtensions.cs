using System.IO;

namespace CovidDataLake.Storage.Utils
{
    public static class FilePathExtensions
    {
        public static string GetExtensionFromPath(this string path)
        {
            return Path.GetExtension(path);
        }
    }
}
