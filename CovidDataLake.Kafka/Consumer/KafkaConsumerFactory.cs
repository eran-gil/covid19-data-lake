using System.Collections.Generic;
using CovidDataLake.Kafka.Consumer.Configuration;
using System.Linq;

namespace CovidDataLake.Kafka.Consumer
{
    public class KafkaConsumerFactory : IConsumerFactory
    {
        private readonly KafkaConsumerConfiguration _configuration;
        private readonly Dictionary<string, IConsumer> _cache;
        public KafkaConsumerFactory(KafkaConsumerConfiguration configuration)
        {
            _configuration = configuration;
            _cache = new Dictionary<string, IConsumer>();
        }
        public IConsumer CreateConsumer(string clientId)
        {
            if (_cache.ContainsKey(clientId))
            {
                return _cache[clientId];
            }
            var servers = _configuration.Instances
                .Select(instance => $"{instance.Host}:{instance.Port}")
                .Aggregate((s1,s2) => $"{s1},{s2}");
            var consumer = new KafkaConsumer(servers, clientId, _configuration.GroupId);
            _cache[clientId] = consumer;
            return consumer;
        }
    }
}
