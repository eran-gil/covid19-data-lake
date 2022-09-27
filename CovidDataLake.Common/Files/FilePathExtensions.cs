namespace CovidDataLake.Common.Files
{
    public static class FilePathExtensions
    {
        public static string GetExtensionFromPath(this string path)
        {
            return Path.GetExtension(path);
        }

        public static long GetFileLength(this string path)
        {
            var info = new FileInfo(path);
            return info.Length;
        }
    }
}
