namespace CovidDataLake.Cloud
{
    public interface IDataLakeWriter
    {
        string GenerateFilePath(string fileType);
        Stream CreateFileStream(string filepath);
        Task DeleteFileAsync(string filePath);
    }
}
