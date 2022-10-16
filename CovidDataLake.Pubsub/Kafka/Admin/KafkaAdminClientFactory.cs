using CovidDataLake.Pubsub.Kafka.Extensions;
using CovidDataLake.Pubsub.Kafka.Producer.Configuration;

namespace CovidDataLake.Pubsub.Kafka.Admin
{
    public class KafkaAdminClientFactory : IPubSubAdminFactory
    {
        private readonly KafkaProducerConfiguration _configuration;
        public KafkaAdminClientFactory(KafkaProducerConfiguration configuration)
        {
            _configuration = configuration;
        }

        public IPubSubAdmin CreateAdminClient(string clientId)
        {
            var connectionString = _configuration.Instances!.ToConnectionString();


            var producer = new KafkaAdminClient(connectionString, clientId);
            return producer;
        }
    }
}
