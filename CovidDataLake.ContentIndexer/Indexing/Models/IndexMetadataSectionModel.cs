namespace CovidDataLake.ContentIndexer.Indexing.Models
{
    public class IndexMetadataSectionModel
    {
        public IndexMetadataSectionModel(string min, string max, long offset)
        {
            Min = min;
            Max = max;
            Offset = offset;
        }
        public string Min { get; set; }
        public string Max { get; set; }
        public long Offset { get; set; }
    }
}
