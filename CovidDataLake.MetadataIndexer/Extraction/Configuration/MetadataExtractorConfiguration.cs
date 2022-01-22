using System.Collections.Generic;

namespace CovidDataLake.MetadataIndexer.Extraction.Configuration
{
    public class MetadataExtractorConfiguration
    {
        public IEnumerable<string> IgnoreMetadataKeys { get; set; }
    }
}
