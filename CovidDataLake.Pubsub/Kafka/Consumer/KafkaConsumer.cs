using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace CovidDataLake.Pubsub.Kafka.Consumer
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

        public async Task Consume(Func<IEnumerable<string>, Task> handleMessages, CancellationToken cancellationToken)
        {
            var batch = ConsumeBatch();
            await handleMessages(batch);
        }

        protected IEnumerable<string> ConsumeBatch()
        {
            while (true)
            {
                ConsumeResult<string, string> consumeResult;
                try
                {
                    consumeResult = _kafkaConsumer.Consume();
                    
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    yield break;
                }
                if (consumeResult == null)
                {
                    yield break;
                }
                yield return consumeResult.Message.Value;
                _kafkaConsumer.Commit(consumeResult);
            }
        }

        public void Dispose()
        {
            _kafkaConsumer?.Dispose();
        }
    }
}
