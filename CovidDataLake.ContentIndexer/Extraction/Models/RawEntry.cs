using System.Collections.Generic;
using System.Linq;

namespace CovidDataLake.ContentIndexer.Extraction.Models
{
    public class RawEntry
    {
        private readonly HashSet<string> _originFilenames;
        //todo: add filename wrapper to save memory
       //todo: test just reading the file and see how much time it takes
       public List<string> OriginFilenames => _originFilenames.ToList();

       public string Value { get; set; }

        public RawEntry(string originFilename, string value)
        {
            _originFilenames = new HashSet<string>{originFilename};
            Value = value;
        }

        public void AddFileName(string filename)
        {
            _originFilenames.Add(filename);
        }

        public void AddFileNames(IEnumerable<string> filenames)
        {
            _originFilenames.UnionWith(filenames);
        }
    }
}
