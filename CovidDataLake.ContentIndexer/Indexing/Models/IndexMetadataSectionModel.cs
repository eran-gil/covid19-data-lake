using Newtonsoft.Json;

namespace CovidDataLake.ContentIndexer.Indexing.Models
{
    public class IndexMetadataSectionModel
    {

        public IndexMetadataSectionModel()
        {
        }
        public IndexMetadataSectionModel(string min, string max, long offset)
        {
            Min = min;
            Max = max;
            Offset = offset;
        }
        [JsonProperty(Order = 1)]
        public string Min { get; set; }
        [JsonProperty(Order = 2)]
        public string Max { get; set; }
        [JsonProperty(Order = 3)]
        public long Offset { get; set; }
    }
}
