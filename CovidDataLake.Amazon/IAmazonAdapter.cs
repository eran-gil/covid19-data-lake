using System;
using System.Threading.Tasks;

namespace CovidDataLake.Amazon
{
    public interface IAmazonAdapter : IDisposable
    {
        Task UploadObjectAsync(string bucketName, string objectKey, string sourceFilename);
        Task<string> DownloadObjectAsync(string bucketName, string objectKey);
        Task DeleteObjectAsync(string bucketName, string objectKey);
    }
}
