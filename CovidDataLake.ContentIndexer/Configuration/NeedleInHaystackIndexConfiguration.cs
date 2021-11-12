namespace CovidDataLake.ContentIndexer.Configuration
{
    public class NeedleInHaystackIndexConfiguration
    {
        public int MaxRowsPerFile { get; set; }
        public int NumOfMetadataRows { get; set; }
        public int BloomFilterCapacity { get; set; }
        public double BloomFilterErrorRate { get; set; }
    }
}
