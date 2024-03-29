﻿using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using CovidDataLake.ContentIndexer.Extraction.Models;
using Newtonsoft.Json;

namespace CovidDataLake.ContentIndexer.Indexing.Models
{
    public class IndexValueModel
    {
        private ImmutableHashSet<StringWrapper> _files = ImmutableHashSet<StringWrapper>.Empty;
        public IndexValueModel()
        {
        }
        public IndexValueModel(string value, IEnumerable<string> files)
        {
            Value = value;
            var stringWrappers = files.Select(f => new StringWrapper(f));
            _files = ImmutableHashSet.CreateRange(stringWrappers);
        }

        public IndexValueModel(string value, IEnumerable<StringWrapper> files)
        {
            Value = value;
            _files = _files.Union(files);
        }
        [JsonProperty(Order = 1)]
        public string Value { get; set; }
        [JsonProperty(Order = 2, ObjectCreationHandling = ObjectCreationHandling.Replace)]
        public List<string> Files { get { return _files.Select(f => f.Value).ToList(); } set { _files = ImmutableHashSet.CreateRange(value.Select(f => new StringWrapper(f))); } }


        public ImmutableHashSet<StringWrapper> GetUniqueFiles()
        {
            return _files;
        }

    }
}
