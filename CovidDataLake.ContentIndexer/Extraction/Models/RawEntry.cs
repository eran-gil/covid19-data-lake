using System.Collections.Generic;
using System.Linq;

namespace CovidDataLake.ContentIndexer.Extraction.Models
{
    public class RawEntry
    {
        //todo: add filename wrapper to save memory
        private List<StringWrapper> _originFileNames;
        private readonly bool _initiatedFromList;
        public IEnumerable<string> OriginFilenames => _originFileNames.Select(wrapper => wrapper.Value);

        public string Value { get; set; }

        public RawEntry(StringWrapper originFilename, string value)
        {
            _originFileNames = new List<StringWrapper> { originFilename };
            Value = value;
            _initiatedFromList = false;
        }

        public RawEntry(List<StringWrapper> originFilenames, string value)
        {
            _originFileNames = originFilenames;
            Value = value;
            _initiatedFromList = true;
        }

        public void MergeEntries(RawEntry other)
        {
            if (_initiatedFromList)
            {
                _originFileNames = new List<StringWrapper>(_originFileNames);
            }
            _originFileNames.AddRange(other._originFileNames);
            _originFileNames = _originFileNames.Distinct().ToList();
        }
    }
}
