namespace CovidDataLake.Cloud.Amazon
{
    public interface IAmazonAdapter : IDisposable
    {
        Task UploadObjectAsync(string? bucketName, string objectKey, string sourceFilename);
        Task<string> DownloadObjectAsync(string? bucketName, string objectKey);
        Task<bool> ObjectExistsAsync(string bucketName, string objectKey);
        Task DeleteObjectAsync(string? bucketName, string objectKey);
        Task<IEnumerable<string>> ListObjectsAsync(string? bucketName, string path);
    }
}
