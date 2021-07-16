using System.Collections.Generic;

namespace CovidDataLake.ContentIndexer.Indexing.Models
{
    public class IndexValueModel
    {
        public IndexValueModel(ulong value, List<string> files)
        {
            Value = value;
            Files = files;
        }
        public ulong Value { get; set; }
        public List<string> Files { get; set; }
    }
}
