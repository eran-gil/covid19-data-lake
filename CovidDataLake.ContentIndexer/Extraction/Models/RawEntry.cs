using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace CovidDataLake.ContentIndexer.Extraction.Models
{
    public class RawEntry
    {
        private readonly ImmutableHashSet<StringWrapper> _originFileNames;
        public IEnumerable<string> OriginFilenames => _originFileNames.Select(wrapper => wrapper.Value);

        public string Value { get; set; }

        public RawEntry(StringWrapper originFilename, string value)
        {
            _originFileNames = ImmutableHashSet.Create(originFilename);
            Value = value;
        }

        public RawEntry(ImmutableHashSet<StringWrapper> originFilenames, string value)
        {
            _originFileNames = originFilenames;
            Value = value;
        }

        public ImmutableHashSet<StringWrapper> GetFileNames()
        {
            return _originFileNames;
        }
    }
}
