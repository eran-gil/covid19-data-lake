using CovidDataLake.Pubsub.Kafka.Extensions;
using CovidDataLake.Pubsub.Kafka.Producer.Configuration;

namespace CovidDataLake.Pubsub.Kafka.Producer
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
            var connectionString = _configuration.Instances!.ToConnectionString();

            var producer = new KafkaProducer(connectionString, clientId, _configuration.Topic);
            return producer;
        }
    }
}
