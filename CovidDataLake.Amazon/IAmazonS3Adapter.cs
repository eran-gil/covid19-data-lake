using System;
using System.Threading.Tasks;

namespace CovidDataLake.Amazon
{
    public interface IAmazonS3Adapter : IDisposable
    {
        Task UploadFileToS3(string bucketName, string objectKey, string sourceFilename);
        Task<string> DownloadObjectFromAmazon(string bucketName, string objectKey);
    }
}