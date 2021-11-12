namespace CovidDataLake.ContentIndexer.Configuration
{
    public class BasicAmazonIndexConfiguration
    {
        public string BucketName { get; set; }
    }

    public class AmazonRootIndexFileConfiguration : BasicAmazonIndexConfiguration
    {
        public string RootIndexName { get; set; }
        public int LockTimeSpanInSeconds { get; set; }
    }
}
