using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace CovidDataLake.ContentIndexer.Indexing.Models
{
    public class RootIndexColumnMappings : IEnumerable<KeyValuePair<string, IEnumerable<RootIndexRow>>>
    {
        private readonly IEnumerable<KeyValuePair<string, IEnumerable<RootIndexRow>>> _enumerableImplementation;

        public RootIndexColumnMappings(IEnumerable<KeyValuePair<string, IEnumerable<RootIndexRow>>> enumerableImplementation)
        {
            _enumerableImplementation = enumerableImplementation;
        }

        public IEnumerator<KeyValuePair<string, IEnumerable<RootIndexRow>>> GetEnumerator()
        {
            return _enumerableImplementation.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable) _enumerableImplementation).GetEnumerator();
        }
    }
}
