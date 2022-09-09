using java.io;
using org.apache.tika.metadata;
using org.apache.tika.parser;
using org.apache.tika.sax;
using File = java.io.File;

namespace CovidDataLake.MetadataIndexer.Extraction.Tika
{
    public class TikaAdapter
    {
        public Dictionary<string, string> ExtractMetadata(string filename)
        {
            File file = new File(filename);
            Parser parser = new AutoDetectParser();
            BodyContentHandler handler = new BodyContentHandler(-1);
            Metadata metadata = new Metadata();
            FileInputStream inputStream = new FileInputStream(file);
            ParseContext context = new ParseContext();
            parser.parse(inputStream, handler, metadata, context);
            var metadataNames = metadata.names();
            var metadataResult = metadataNames.ToDictionary(name => name, name => metadata.get(name));
            return metadataResult;
        }
    }
}
