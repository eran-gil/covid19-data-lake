using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;

namespace CovidDataLake.Amazon
{
    public class AmazonClientAdapter : IAmazonAdapter
    {
        private readonly AmazonS3Client _awsClient;

        public AmazonClientAdapter(AmazonS3Config config, AWSCredentials credentials)
        {
            _awsClient = new AmazonS3Client(credentials, config);
        }
        public async Task UploadObjectAsync(string bucketName, string objectKey, string sourceFilename)
        {
            var putObjectRequest = new PutObjectRequest()
            {
                BucketName = bucketName,
                Key = objectKey,
                FilePath = sourceFilename
            };
            var response = await _awsClient.PutObjectAsync(putObjectRequest);
            if (response.HttpStatusCode != HttpStatusCode.OK)
            {
                throw new Exception("couldn't upload to amazon");
            }
        }

        public async Task<string> DownloadObjectAsync(string bucketName, string objectKey)
        {
            var downloadedFilename = Guid.NewGuid().ToString();
            var getRequest = new GetObjectRequest
            {
                BucketName = bucketName,
                Key = objectKey
            };
            using var response = await _awsClient.GetObjectAsync(getRequest);
            await response.WriteResponseStreamToFileAsync(downloadedFilename, false, CancellationToken.None);

            return downloadedFilename;
        }

        public async Task DeleteObjectAsync(string bucketName, string objectKey)
        {
            await _awsClient.DeleteObjectAsync(bucketName, objectKey);
        }

        public void Dispose()
        {
            _awsClient?.Dispose();
        }
    }
}
