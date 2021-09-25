namespace CovidDataLake.ContentIndexer.Configuration
{
    public abstract class BaseAmazonIndexConfiguration
    {
        public string BucketName { get; set; }
    }

    public class AmazonIndexFileConfiguration : BaseAmazonIndexConfiguration
    {
        public int NumOfMetadataRows { get; set; }
        public int BloomFilterCapacity { get; set; }
        public double BloomFilterErrorRate { get; set; }
    }

    public class AmazonRootIndexFileConfiguration : BaseAmazonIndexConfiguration
    {
        public string RootIndexName { get; set; }
        public int LockTimeSpanInSeconds { get; set; }
    }
}
