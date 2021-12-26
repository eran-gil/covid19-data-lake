using System.Threading.Tasks;
using Confluent.Kafka;

namespace CovidDataLake.Pubsub.Kafka.Producer
{
    public class KafkaProducer : IProducer
    {
        private readonly string _topic;
        private readonly IProducer<string, string> _producer;

        public KafkaProducer(string servers, string clientId, string topic)
        {
            _topic = topic;
            var config = new ProducerConfig
            {
                BootstrapServers = servers,
                ClientId = clientId
            };
            _producer = new ProducerBuilder<string, string>(config).Build();
        }

        public async Task<bool> SendMessage(string filename)
        {
            var message = CreateMessageFromFileName(filename);
            try
            {
                await _producer.ProduceAsync(_topic, message);
            }
            catch (ProduceException<string, string>)
            {
                //TODO: add logging
                return false;
            }
            return true;
        }

        private Message<string, string> CreateMessageFromFileName(string filename)
        {
            return new Message<string, string> { Key = filename, Value = filename };
        }


        public void Dispose()
        {
            _producer?.Dispose();
        }
    }
}
