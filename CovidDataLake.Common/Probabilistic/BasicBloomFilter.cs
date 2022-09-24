using System.Runtime.Serialization.Formatters.Binary;
using Maybe.BloomFilter;

namespace CovidDataLake.Common.Probabilistic
{
    public class BasicBloomFilter
    {
        private readonly BloomFilter<string> _filter;

        public BasicBloomFilter(int capacity, double errorRate)
        {
            _filter = new BloomFilter<string>(capacity, errorRate);
        }

        public BasicBloomFilter(Stream stream)
        {
            var formatter = new BinaryFormatter();
            _filter = (BloomFilter<string>)formatter.Deserialize(stream);
        }
        public void Add(string value)
        {
            _filter.Add(value);
        }

        public bool IsInFilter(string value)
        {
            return _filter.Contains(value);
        }

        public void Serialize(Stream stream)
        {
            var formatter = new BinaryFormatter();
            formatter.Serialize(stream, _filter);
        }
    }
}
