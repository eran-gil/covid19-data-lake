using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;

namespace CovidDataLake.Amazon
{
    public class AmazonClientAdapter : IAmazonAdapter
    {
        private readonly AmazonS3Client _awsClient;

        public AmazonClientAdapter()
        {
            _awsClient = new AmazonS3Client(); //TODO: inject config
        }
        public async Task UploadObject(string bucketName, string objectKey, string sourceFilename)
        {
            var uploadSession = await _awsClient.InitiateMultipartUploadAsync(bucketName, objectKey);
            if (uploadSession.HttpStatusCode != HttpStatusCode.OK)
            {
                throw new Exception("couldn't upload to amazon");
            }
            var uploadPartRequest = new UploadPartRequest
            {
                BucketName = bucketName,
                Key = objectKey,
                UploadId = uploadSession.UploadId,
                FilePath = sourceFilename
            };
            var uploadPartResponse = await _awsClient.UploadPartAsync(uploadPartRequest);
            if (uploadPartResponse.HttpStatusCode != HttpStatusCode.OK)
            {
                throw new Exception("couldn't upload to amazon");
            }
            var completeUploadRequest = new CompleteMultipartUploadRequest
                { BucketName = bucketName, Key = objectKey, UploadId = uploadSession.UploadId };
            var completeMultipartUploadResponse = await _awsClient.CompleteMultipartUploadAsync(completeUploadRequest);
            if (completeMultipartUploadResponse.HttpStatusCode != HttpStatusCode.OK)
            {
                throw new Exception("couldn't upload to amazon");
            }
        }

        public async Task<string> DownloadObject(string bucketName, string objectKey)
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

        public async Task DeleteObject(string bucketName, string objectKey)
        {
            await _awsClient.DeleteObjectAsync(bucketName, objectKey);
        }

        public void Dispose()
        {
            _awsClient?.Dispose();
        }
    }
}
