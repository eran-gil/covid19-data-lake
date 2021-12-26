using System.Collections.Generic;

namespace CovidDataLake.MetadataIndexer.Indexing
{
    public interface IMetadataIndexer
    {
        void IndexMetadata(KeyValuePair<string, string> data);
    }
}
