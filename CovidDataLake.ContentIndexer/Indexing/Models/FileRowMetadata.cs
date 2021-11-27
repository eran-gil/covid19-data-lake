namespace CovidDataLake.ContentIndexer.Indexing.Models
{
    public class FileRowMetadata
    {
        public FileRowMetadata(long offset, string value)
        {
            Offset = offset;
            Value = value;
        }

        public long Offset { get; set; }
        public string Value { get; set; }
    }
}
