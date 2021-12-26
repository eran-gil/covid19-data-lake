using System.Collections.Generic;
using TikaOnDotNet.TextExtraction;

namespace CovidDataLake.MetadataIndexer.Extraction
{
    class TikaMetadataExtractor : IMetadataExtractor
    {
        private readonly TextExtractor _extractor;

        public TikaMetadataExtractor()
        {
            _extractor = new TextExtractor();
        }
        public IDictionary<string, string> ExtractMetadata(string filename)
        {
            var metadataResult = _extractor.Extract(filename);
            return metadataResult.Metadata;
        }
    }
}
