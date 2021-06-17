using System.Collections.Generic;
using CovidDataLake.Kafka.Producer.Configuration;
using System.Linq;

namespace CovidDataLake.Kafka.Producer
{
    public class KafkaProducerFactory : IProducerFactory
    {
        private readonly KafkaProducerConfiguration _configuration;
        public KafkaProducerFactory(KafkaProducerConfiguration configuration)
        {
            _configuration = configuration;
        }

        public IProducer CreateProducer(string clientId)
        {
            var servers = _configuration.Instances
                .Select(instance => $"{instance.Host}:{instance.Port}")
                .Aggregate((s1, s2) => $"{s1},{s2}");

            var producer = new KafkaProducer(servers, clientId);
            return producer;
        }
    }
}
