using System.Net;
using CovidDataLake.Pubsub.Kafka.Admin;
using Newtonsoft.Json;

namespace CovidDataLake.Scripts.Actions
{
    internal class ResetKafkaAction : IScriptAction
    {
        private readonly IPubSubAdmin _adminClient;
        private readonly IEnumerable<string> _ignoredTopics = new List<string>{"__consumer_offsets"};
        public ResetKafkaAction(IPubSubAdminFactory adminFactory)
        {
            _adminClient = adminFactory.CreateAdminClient(Dns.GetHostName());
        }
        public string Name => "reset_kafka";
        public async Task<bool> Run()
        {
            var topics = _adminClient.GetTopicNames().Except(_ignoredTopics).ToList();
            Console.WriteLine("Topics: " + JsonConvert.SerializeObject(topics));
            if (topics.Any())
            {
                await _adminClient.DeleteTopicAsync(topics);
            }
            return true;

        }
    }
}
