﻿using CovidDataLake.Cloud.Amazon.Configuration;

namespace CovidDataLake.MetadataIndexer.Indexing.Configuration
{
    public class ProbabilisticMetadataIndexConfigurationBase : BasicAmazonIndexFileConfiguration
    {
        public int LockIntervalInSeconds { get; set; }
    }

    public class HyperLogLogMetadataIndexConfiguration : ProbabilisticMetadataIndexConfigurationBase
    {
    }

    public class CountMinSketchMetadataIndexConfiguration : ProbabilisticMetadataIndexConfigurationBase
    {
        public double Confidence { get; set; }
        public double ErrorRate { get; set; }
    }
}
