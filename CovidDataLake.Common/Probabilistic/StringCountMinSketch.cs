using System.Runtime.Serialization.Formatters.Binary;
using Maybe.CountMinSketch;
#pragma warning disable SYSLIB0011

namespace CovidDataLake.Common.Probabilistic
{
    public class StringCountMinSketch
    {

        private readonly CountMinSketch<string> _sketch;

        public StringCountMinSketch(double confidence, double errorRate)
        {
            _sketch = new CountMinSketch<string>(errorRate, confidence, 0);
        }

        public StringCountMinSketch(Stream stream)
        {
            var formatter = new BinaryFormatter();
            _sketch = (CountMinSketch<string>) formatter.Deserialize(stream);
        }

        public void Add(string value)
        {
            _sketch.Add(value);
        }

        public long GetOccurrences(string value)
        {
            return _sketch.EstimateCount(value);
        }

        public void SerializeToStream(Stream stream)
        {
            var formatter = new BinaryFormatter();
            formatter.Serialize(stream, _sketch);
        }
    }
}
