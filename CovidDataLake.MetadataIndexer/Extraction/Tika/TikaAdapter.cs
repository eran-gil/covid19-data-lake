using java.io;
using org.apache.tika.metadata;
using TikaClient = org.apache.tika.Tika;

namespace CovidDataLake.MetadataIndexer.Extraction.Tika
{
    public class TikaAdapter
    {
        public Dictionary<string, string> ExtractMetadata(string filename)
        {
            try
            {
                TikaClient tika = new TikaClient();
                Metadata metadata = new Metadata();
                FileInputStream inputStream = new FileInputStream(filename);
                var reader = tika.parse(inputStream, metadata);
                var metadataNames = metadata.names();
                var metadataResult = metadataNames.ToDictionary(name => name, name => metadata.get(name));
                reader.close();
                return metadataResult;
            }
            catch
            {
                return new Dictionary<string, string>();
            }
            
        }
    }
}
