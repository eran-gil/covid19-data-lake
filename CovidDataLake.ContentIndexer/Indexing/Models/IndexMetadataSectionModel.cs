namespace CovidDataLake.ContentIndexer.Indexing.Models
{
    public class IndexMetadataSectionModel
    {
        public IndexMetadataSectionModel(ulong min, ulong max, long offset)
        {
            Min = min;
            Max = max;
            Offset = offset;
        }
        public ulong Min { get; set; }
        public ulong Max { get; set; }
        public long Offset { get; set; }
    }
}
