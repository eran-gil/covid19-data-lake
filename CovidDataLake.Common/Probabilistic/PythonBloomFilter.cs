using System.Text;
using Python.Runtime;

namespace CovidDataLake.Common.Probabilistic
{
    public class PythonBloomFilter
    {
        // ReSharper disable once NotAccessedField.Local
        private static readonly Py.GILState PyGil;
        private readonly dynamic _filter;
        private const string BloomFilterLibrary = "probables";
        private readonly dynamic _pyProbables;

        static PythonBloomFilter()
        {
            PyGil = Py.GIL();
        }

        public PythonBloomFilter(int capacity, double errorRate) : this()
        {
            _filter = _pyProbables.BloomFilter(est_elements: capacity, false_positive_rate: errorRate);
        }

        public PythonBloomFilter(byte[] serialized) : this()
        {
            var hexRepresentation = Encoding.UTF8.GetString(serialized);
            _filter = _pyProbables.BloomFilter(hex_string: hexRepresentation);
        }
        private PythonBloomFilter()
        {
            _pyProbables = Py.Import(BloomFilterLibrary);
        }

        public void Add(string value)
        {
            _filter.add(value);
        }

        public bool IsInFilter(string value)
        {
            return (bool) _filter.check(value);
        }

        public byte[] Serialize()
        {
            var serialized = (string)_filter.export_hex();
            var serializedBytes = Encoding.UTF8.GetBytes(serialized);
            return serializedBytes;
        }

    }
}
