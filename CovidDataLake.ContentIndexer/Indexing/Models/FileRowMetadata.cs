using Newtonsoft.Json;

namespace CovidDataLake.ContentIndexer.Indexing.Models
{
    public class FileRowMetadata
    {
        public FileRowMetadata(long offset, string value)
        {
            Offset = offset;
            Value = value;
        }

        [JsonProperty(Order = 1)]
        public string Value { get; set; }

        [JsonProperty(Order = 2)]
        public long Offset { get; set; }
    }
}
