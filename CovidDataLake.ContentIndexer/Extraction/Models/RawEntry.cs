using System.Collections.Generic;

namespace CovidDataLake.ContentIndexer.Extraction.Models
{
    public class RawEntry
    {
        public List<string> OriginFilenames { get; set; }
        public string Value { get; set; }

        public RawEntry(string originFilename, string value)
        {
            OriginFilenames = new List<string>{originFilename};
            Value = value;
        }

        public void AddFileName(string filename)
        {
            OriginFilenames.Add(filename);
        }
    }
}
