using System.IO;
using CardinalityEstimation;

namespace CovidDataLake.Common.Probabilistic
{
    public class HyperLogLog
    {
        private readonly CardinalityEstimator _hyperLogLog;
        private static readonly CardinalityEstimatorSerializer Serializer;

        static HyperLogLog()
        {
            Serializer = new CardinalityEstimatorSerializer();
        }

        public HyperLogLog()
        {
            _hyperLogLog = new CardinalityEstimator();
        }

        public HyperLogLog(Stream stream)
        {
            _hyperLogLog = Serializer.Deserialize(stream);
        }

        public void Add(string value)
        {
            _hyperLogLog.Add(value);
        }

        public ulong Count()
        {
            return _hyperLogLog.Count();
        }

        public void SerializeToStream(Stream stream)
        {
            Serializer.Serialize(stream, _hyperLogLog);
        }
    }
}
