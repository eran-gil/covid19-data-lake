using System;
using Confluent.Kafka;

namespace CovidDataLake.Kafka.Consumer
{
    public class KafkaConsumer : IConsumer
    {
        private readonly IConsumer<string, string> _kafkaConsumer;
        
        public KafkaConsumer(string servers, string clientId, string groupId)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = servers,
                GroupId = groupId,
                ClientId = clientId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false
            };
            _kafkaConsumer = new ConsumerBuilder<string, string>(config).Build();
        }

        public void Subscribe(string topic)
        {
            _kafkaConsumer.Subscribe(topic);
        }

        public void Consume(Action<string> handleMessage)
        {
            var consumeResult = _kafkaConsumer.Consume();
            handleMessage(consumeResult.Message.Value);
            try
            {
                _kafkaConsumer.Commit(consumeResult);
            }
            catch (KafkaException)
            {
                //TODO: log
            }
        }

        public void Dispose()
        {
            _kafkaConsumer?.Dispose();
        }
    }
}