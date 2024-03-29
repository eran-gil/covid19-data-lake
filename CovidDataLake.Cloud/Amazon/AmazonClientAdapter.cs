﻿using System.Net;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using CovidDataLake.Common;

namespace CovidDataLake.Cloud.Amazon
{
    public class AmazonClientAdapter : IAmazonAdapter
    {
        private readonly AmazonS3Client _awsClient;

        public AmazonClientAdapter(AWSCredentials credentials)
        {
            var config = new AmazonS3Config { UseAccelerateEndpoint = true };
            _awsClient = new AmazonS3Client(credentials, config);
        }
        public async Task UploadObjectAsync(string? bucketName, string objectKey, string sourceFilename)
        {
            var putObjectRequest = new PutObjectRequest()
            {
                BucketName = bucketName,
                Key = objectKey,
                FilePath = sourceFilename
            };
            var response = await _awsClient.PutObjectAsync(putObjectRequest).ConfigureAwait(false);
            if (response.HttpStatusCode != HttpStatusCode.OK)
            {
                throw new Exception("couldn't upload to amazon");
            }
        }

        public async Task<string> DownloadObjectAsync(string? bucketName, string objectKey)
        {
            var fileType = objectKey.Split('.').LastOrDefault();
            if (!string.IsNullOrEmpty(fileType))
            {
                fileType = "." + fileType;
            }

            var filename = $"{Guid.NewGuid()}{fileType}";
            var downloadedFilePath = Path.Combine(CommonKeys.TEMP_FOLDER_NAME, filename);
            var getRequest = new GetObjectRequest
            {
                BucketName = bucketName,
                Key = objectKey
            };
            try
            {
                using var response = await _awsClient.GetObjectAsync(getRequest).ConfigureAwait(false);
                await response.WriteResponseStreamToFileAsync(downloadedFilePath, false, CancellationToken.None).ConfigureAwait(false);

                return downloadedFilePath;
            }
            
            catch (AmazonS3Exception? e)
            {
                if (e.StatusCode == HttpStatusCode.NotFound)
                {
                    throw new ResourceNotFoundException(bucketName, objectKey);
                }

                throw new CloudException(e);
            }
        }

        public async Task<bool> ObjectExistsAsync(string? bucketName, string objectKey)
        {
            var request = new GetObjectMetadataRequest { BucketName = bucketName, Key = objectKey };
            try
            {
                var metadataResponse = await _awsClient.GetObjectMetadataAsync(request).ConfigureAwait(false);
                return metadataResponse.HttpStatusCode == HttpStatusCode.OK;
            }
            catch
            {
                return false;
            }

        }

        public async Task<IEnumerable<string>> ListObjectsAsync(string? bucketName, string path)
        {
            var results = new List<string>();
            var shouldContinue = true;
            while (shouldContinue)
            {
                try
                {
                    var request = new ListObjectsV2Request() { BucketName = bucketName, Prefix = path, StartAfter = results.LastOrDefault()};
                    var listObjectsResponse = await _awsClient.ListObjectsV2Async(request).ConfigureAwait(false);
                    if (listObjectsResponse.HttpStatusCode != HttpStatusCode.OK)
                    {
                        return Enumerable.Empty<string>();
                    }
                    var objectNames = listObjectsResponse.S3Objects.Select(o => o.Key);
                    results.AddRange(objectNames);
                    shouldContinue = listObjectsResponse.IsTruncated;
                }
                catch
                {
                    shouldContinue = false;
                }
            }
            

            return results;


        }

        public async Task DeleteObjectAsync(string? bucketName, string objectKey)
        {
            await _awsClient.DeleteObjectAsync(bucketName, objectKey).ConfigureAwait(false);
        }

        public void Dispose()
        {
            _awsClient.Dispose();
        }
    }
}
