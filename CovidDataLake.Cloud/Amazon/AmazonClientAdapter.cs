using System;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using CovidDataLake.Common;

namespace CovidDataLake.Cloud.Amazon
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
            var downloadedFilename = Path.Combine(CommonKeys.TEMP_FOLDER_NAME, Guid.NewGuid().ToString());
            var getRequest = new GetObjectRequest
            {
                BucketName = bucketName,
                Key = objectKey
            };
            try
            {
                using var response = await _awsClient.GetObjectAsync(getRequest);
                await response.WriteResponseStreamToFileAsync(downloadedFilename, false, CancellationToken.None);

                return downloadedFilename;
            }
            
            catch (AmazonS3Exception e)
            {
                if (e.StatusCode == HttpStatusCode.NotFound)
                {
                    throw new ResourceNotFoundException(bucketName, objectKey);
                }

                throw new CloudException(e);
            }
        }

        public async Task<bool> ObjectExistsAsync(string bucketName, string objectKey)
        {
            var request = new GetObjectMetadataRequest { BucketName = bucketName, Key = objectKey };
            try
            {
                var metadataResponse = await _awsClient.GetObjectMetadataAsync(request);
                return metadataResponse.HttpStatusCode == HttpStatusCode.OK;
            }
            catch
            {
                return false;
            }

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
