using System.Linq;
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
            var servers = _configuration.Instances
                .Select(instance => $"{instance.Host}:{instance.Port}")
                .Aggregate((s1, s2) => $"{s1},{s2}");

            var producer = new KafkaAdminClient(servers, clientId);
            return producer;
        }
    }
}
