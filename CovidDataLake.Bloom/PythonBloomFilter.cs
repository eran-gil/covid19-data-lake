
using System;
using Python.Runtime;

namespace CovidDataLake.Bloom
{
    public class PythonBloomFilter
    {
        // ReSharper disable once NotAccessedField.Local
        private static readonly Py.GILState PyGil;
        private readonly dynamic _filter;
        private const string BloomFilterLibrary = "bloomfilter";
        private const string BuiltinsLibrary = "builtins";
        private readonly dynamic _pyBloomFilter;
        private readonly dynamic _builtins;

        static PythonBloomFilter()
        {
            PyGil = Py.GIL();
        }

        public PythonBloomFilter(int capacity, double errorRate) : this()
        {
            _filter = _pyBloomFilter.BloomFilter(expected_insertions: capacity, err_rate: errorRate);
        }

        public PythonBloomFilter(string serializedFilter)
        {
            
            _filter = _pyBloomFilter.BloomFilter.loads(serializedFilter);
        }
        private PythonBloomFilter()
        {
            _pyBloomFilter = Py.Import(BloomFilterLibrary);
            _builtins = Py.Import(BuiltinsLibrary);
        }

        public void AddToFilter(ulong value)
        {
            var byteArray = BitConverter.GetBytes(value);
            PyObject bytes = _builtins.GetAttr("bytes");
            var bytesObject = bytes.Invoke(byteArray.ToPython());
            _filter.put(bytesObject);
        }

        public string Serialize()
        {
            var serialized = _filter.dumps().ToString();
            return serialized;
        }

    }
}
