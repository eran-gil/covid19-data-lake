namespace CovidDataLake.ContentIndexer.Indexing.Models
{
    public class FileRowMetadata
    {
        public FileRowMetadata(long offset, ulong value)
        {
            Offset = offset;
            Value = value;
        }

        public long Offset { get; set; }
        public ulong Value { get; set; }
    }
}
