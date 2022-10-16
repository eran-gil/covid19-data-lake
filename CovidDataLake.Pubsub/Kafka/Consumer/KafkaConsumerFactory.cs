using CovidDataLake.Pubsub.Kafka.Consumer.Configuration;
using CovidDataLake.Pubsub.Kafka.Extensions;

namespace CovidDataLake.Pubsub.Kafka.Consumer
{
    public class KafkaConsumerFactory : IConsumerFactory
    {
        private readonly KafkaConsumerConfiguration _configuration;

        public KafkaConsumerFactory(KafkaConsumerConfiguration configuration)
        {
            _configuration = configuration;
        }

        public IConsumer CreateConsumer(string clientId)
        {
            var connectionString = _configuration.Instances!.ToConnectionString();

            var consumer = new KafkaConsumer(connectionString, clientId, _configuration.GroupId);
            consumer.Subscribe(_configuration.Topic);
            return consumer;
        }
    }
}
