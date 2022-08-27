using System.Collections.Generic;
using System.Linq;

namespace CovidDataLake.ContentIndexer.Indexing.Models
{
    public class IndexValueModel
    {
        private HashSet<string> _files = new();
        public IndexValueModel()
        {
        }
        public IndexValueModel(string value, IEnumerable<string> files)
        {
            Value = value;
            _files = new HashSet<string>(files);
        }
        public string Value { get; set; }
        public List<string> Files { get { return _files.ToList(); } set { _files = new HashSet<string>(value); } }

        public void AddFiles(IEnumerable<string> files)
        {
            _files.UnionWith(files);
        }

    }
}
