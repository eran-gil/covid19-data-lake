using System;
using System.Threading;
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

        public void Consume(Action<string> handleMessage, CancellationToken cancellationToken)
        {
            try
            {
                var consumeResult = _kafkaConsumer.Consume(cancellationToken);
                handleMessage(consumeResult.Message.Value);
                _kafkaConsumer.Commit(consumeResult);
            }
            catch (Exception)
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