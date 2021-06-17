using CovidDataLake.Kafka.Consumer.Configuration;
using System.Linq;

namespace CovidDataLake.DAL.Pubsub
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
            var servers = _configuration.Instances
                .Select(instance => $"{instance.Host}:{instance.Port}")
                .Aggregate((s1,s2) => $"{s1},{s2}");
            return new KafkaConsumer(servers, clientId, _configuration.GroupId);
        }
    }
}
