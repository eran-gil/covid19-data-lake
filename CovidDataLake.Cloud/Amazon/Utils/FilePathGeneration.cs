using CovidDataLake.Common;

namespace CovidDataLake.Cloud.Amazon.Utils
{
    public static class FilePathGeneration
    {
        public static string GenerateFilePath(string fileType)
        {
            var today = DateTime.Today;
            var path = $"{CommonKeys.CONTENT_FOLDER_NAME}/{today.Year}/{today.Month}/{today.Day}";
            var filename = Guid.NewGuid().ToString();
            return $"{path}/{filename}{fileType}";
        }
    }
}
