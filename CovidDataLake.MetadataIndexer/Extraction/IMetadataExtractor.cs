namespace CovidDataLake.MetadataIndexer.Extraction
{
    internal interface IMetadataExtractor
    {
        IDictionary<string, string> ExtractMetadata(string filename);
    }
}
