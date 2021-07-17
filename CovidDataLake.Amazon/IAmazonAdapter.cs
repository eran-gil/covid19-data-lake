using System;
using System.Threading.Tasks;

namespace CovidDataLake.Amazon
{
    public interface IAmazonAdapter : IDisposable
    {
        Task UploadObject(string bucketName, string objectKey, string sourceFilename);
        Task<string> DownloadObject(string bucketName, string objectKey);
        Task DeleteObject(string bucketName, string objectKey);
    }
}