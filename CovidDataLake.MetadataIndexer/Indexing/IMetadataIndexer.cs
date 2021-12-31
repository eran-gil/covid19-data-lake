using System.Collections.Generic;
using System.Threading.Tasks;

namespace CovidDataLake.MetadataIndexer.Indexing
{
    public interface IMetadataIndexer
    {
        Task IndexMetadata(KeyValuePair<string, string> data);
    }
}
