using System;
using System.Collections.Generic;
using System.Linq;
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
            var consumeResults = ConsumeBatch();
            var batch = consumeResults.Select(result => result.Message.Value);
            
            await handleMessages(batch);
            foreach (var result in consumeResults)
            {
                _kafkaConsumer.Commit(result);
            }
        }

        protected IList<ConsumeResult<string, string>> ConsumeBatch()
        {
            var consumeResults = new List<ConsumeResult<string, string>>();
            while (true)
            {
                ConsumeResult<string, string> consumeResult;
                try
                {
                    consumeResult = _kafkaConsumer.Consume(1000);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    break;
                }
                if (consumeResult == null)
                {
                    break;
                }
                consumeResults.Add(consumeResult);
            }

            return consumeResults;
        }

        public void Dispose()
        {
            _kafkaConsumer?.Dispose();
        }
    }
}
