using Confluent.Kafka;

namespace CovidDataLake.Pubsub.Kafka.Admin
{
    public class KafkaAdminClient : IPubSubAdmin
    {
        private readonly IAdminClient _adminClient;

        public KafkaAdminClient(string servers, string clientId)
        {
            var adminConfig = new AdminClientConfig
            {
                BootstrapServers = servers,
                ClientId = clientId,
            };
            _adminClient = new AdminClientBuilder(adminConfig).Build();
        }

        public IEnumerable<string> GetTopicNames()
        {
            var metadata = _adminClient.GetMetadata(TimeSpan.FromMinutes(1));
            var topicNames = metadata.Topics.Select(t => t.Topic).ToList();
            return topicNames;
        }

        public async Task DeleteTopicAsync(IEnumerable<string> topics)
        {
            await _adminClient.DeleteTopicsAsync(topics);
        }

    }
}
