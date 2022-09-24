namespace CovidDataLake.Cloud.Amazon
{
    public class ResourceNotFoundException : Exception
    {
        public string? BucketName { get; }
        public string ObjectKey { get; }

        public ResourceNotFoundException(string? bucketName, string objectKey) : base($"Could not find resource '{objectKey}' in bucket '{bucketName}'")
        {
            BucketName = bucketName;
            ObjectKey = objectKey;
        }
    }
}
