using System.Collections.Generic;
using CovidDataLake.Kafka.Producer.Configuration;
using System.Linq;

namespace CovidDataLake.Kafka.Producer
{
    public class KafkaProducerFactory : IProducerFactory
    {
        private readonly KafkaProducerConfiguration _configuration;
        private readonly Dictionary<string, IProducer> _cache;
        public KafkaProducerFactory(KafkaProducerConfiguration configuration)
        {
            _configuration = configuration;
            _cache = new Dictionary<string, IProducer>();
        }

        public IProducer CreateProducer(string clientId)
        {
            if(_cache.ContainsKey(clientId))
            {
                return _cache[clientId];
            }
            var servers = _configuration.Instances
                .Select(instance => $"{instance.Host}:{instance.Port}")
                .Aggregate((s1, s2) => $"{s1},{s2}");

            var producer = new KafkaProducer(servers, clientId);
            _cache[clientId] = producer;
            return producer;
        }
    }
}
