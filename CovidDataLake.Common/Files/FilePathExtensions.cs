namespace CovidDataLake.Common.Files
{
    public static class FilePathExtensions
    {
        public static string GetExtensionFromPath(this string path)
        {
            return Path.GetExtension(path);
        }
    }
}
