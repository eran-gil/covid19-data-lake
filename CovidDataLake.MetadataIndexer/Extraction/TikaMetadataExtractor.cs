using System.Collections.Generic;
using CovidDataLake.MetadataIndexer.Extraction.Configuration;
using TikaOnDotNet.TextExtraction;

namespace CovidDataLake.MetadataIndexer.Extraction
{
    class TikaMetadataExtractor : IMetadataExtractor
    {
        private readonly TextExtractor _extractor;
        private readonly IEnumerable<string> _keysToIgnore;

        public TikaMetadataExtractor(MetadataExtractorConfiguration configuration)
        {
            _extractor = new TextExtractor();
            _keysToIgnore = configuration.IgnoreMetadataKeys;
        }
        public IDictionary<string, string> ExtractMetadata(string filename)
        {
            var metadataResult = _extractor.Extract(filename);
            foreach (var key in _keysToIgnore)
            {
                metadataResult.Metadata.Remove(key);
            }
            return metadataResult.Metadata;
        }
    }
}
