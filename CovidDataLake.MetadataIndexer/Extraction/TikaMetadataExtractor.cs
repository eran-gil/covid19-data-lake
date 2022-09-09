using CovidDataLake.MetadataIndexer.Extraction.Configuration;
using CovidDataLake.MetadataIndexer.Extraction.Tika;

namespace CovidDataLake.MetadataIndexer.Extraction
{
    class TikaMetadataExtractor : IMetadataExtractor
    {
        private readonly TikaAdapter _extractor;
        private readonly IEnumerable<string> _keysToIgnore;

        public TikaMetadataExtractor(TikaAdapter tikaAdapter, MetadataExtractorConfiguration configuration)
        {
            _extractor = tikaAdapter;
            _keysToIgnore = configuration.IgnoreMetadataKeys;
        }
        public IDictionary<string, string> ExtractMetadata(string filename)
        {
            var metadataResult = _extractor.ExtractMetadata(filename);
            foreach (var key in _keysToIgnore)
            {
                metadataResult.Remove(key);
            }
            return metadataResult;
        }
    }
}
