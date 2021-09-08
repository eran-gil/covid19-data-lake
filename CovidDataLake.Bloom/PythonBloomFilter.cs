
using System;
using Python.Runtime;

namespace CovidDataLake.Bloom
{
    public class PythonBloomFilter : IDisposable
    {
        private readonly Py.GILState _pyGil;
        private readonly dynamic _filter;

        public PythonBloomFilter(int capacity, double errorRate)
        {
            _pyGil = Py.GIL();
            dynamic pyBloomFilter = Py.Import("pybloomfilter");
            _filter = pyBloomFilter.BloomFilter(capacity, errorRate);
        }

        public PythonBloomFilter(string serializedFilter)
        {
            _pyGil = Py.GIL();
            dynamic pyBloomFilter = Py.Import("pybloomfilter");
            _filter = pyBloomFilter.BloomFilter.from_base64(serializedFilter);
        }

        public void AddToFilter(ulong value)
        {
            _filter.add(value);
        }

        public string Serialize()
        {
            var serialized = _filter.to_base64().ToString();
            return serialized;
        }

        public void Dispose()
        {
            _pyGil?.Dispose();
        }
    }
}
