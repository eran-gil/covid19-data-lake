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
            //TODO: use configuration
            var servers = _configuration.Instances
                .Select(instance => $"{instance.Host}:{instance.Port}")
                .Aggregate((s1, s2) => $"{s1},{s2}");

            return new KafkaProducer(servers, clientId);
        }
    }
}
