using System.Collections.Generic;

namespace CovidDataLake.ContentIndexer.Indexing.Models
{
    public class IndexValueModel
    {

        public IndexValueModel()
        {
            
        }
        public IndexValueModel(string value, List<string> files)
        {
            Value = value;
            Files = files;
        }
        public string Value { get; set; }
        public List<string> Files { get; set; }
    }
}
