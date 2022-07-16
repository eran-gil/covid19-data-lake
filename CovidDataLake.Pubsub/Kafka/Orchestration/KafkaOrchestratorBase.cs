using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using CovidDataLake.Pubsub.Kafka.Consumer;
using CovidDataLake.Pubsub.Kafka.Orchestration.Configuration;

namespace CovidDataLake.Pubsub.Kafka.Orchestration
{
    public abstract class KafkaOrchestratorBase : IOrchestrator
    {

        private readonly IConsumer _consumer;
        private readonly CancellationTokenSource _cancellationTokenSource;
        private readonly TimeSpan _consumptionInterval;

        public KafkaOrchestratorBase(IConsumerFactory consumerFactory, BatchOrchestratorConfiguration batchConfiguration)
        {
            _consumer = consumerFactory.CreateConsumer(Dns.GetHostName());
            _cancellationTokenSource = new CancellationTokenSource();
            _consumptionInterval = TimeSpan.FromSeconds(batchConfiguration.BatchIntervalInSeconds);
        }

        public async Task StartOrchestration()
        {
            while (true)
            {
                await _consumer.Consume(HandleMessages, _cancellationTokenSource.Token);
                Thread.Sleep(_consumptionInterval);
            }
            // ReSharper disable once FunctionNeverReturns
        }

        protected abstract Task HandleMessages(IEnumerable<string> files);

        public void Dispose()
        {
            _consumer?.Dispose();
            _cancellationTokenSource.Cancel();
            _cancellationTokenSource?.Dispose();
        }
    }
}
