using CovidDataLake.Cloud.Amazon.Configuration;

namespace CovidDataLake.ContentIndexer.Configuration
{

    public class AmazonRootIndexFileConfiguration : BasicAmazonIndexFileConfiguration
    {
        public string RootIndexName { get; set; }
        public int LockTimeSpanInSeconds { get; set; }
    }
}
