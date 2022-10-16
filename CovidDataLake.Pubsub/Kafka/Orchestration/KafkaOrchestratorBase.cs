using System.Net;
using CovidDataLake.Common;
using CovidDataLake.Pubsub.Kafka.Consumer;
using CovidDataLake.Pubsub.Kafka.Orchestration.Configuration;
using Microsoft.Extensions.Logging;

namespace CovidDataLake.Pubsub.Kafka.Orchestration
{
    public abstract class KafkaOrchestratorBase : IOrchestrator
    {
        private readonly ILogger<KafkaOrchestratorBase> _logger;

        private readonly IConsumer _consumer;
        private readonly CancellationTokenSource _cancellationTokenSource;
        private readonly TimeSpan _consumptionInterval;

        protected KafkaOrchestratorBase(IConsumerFactory consumerFactory,
            BatchOrchestratorConfiguration batchConfiguration,
            ILogger<KafkaOrchestratorBase> logger)
        {
            _logger = logger;
            _consumer = consumerFactory.CreateConsumer(Dns.GetHostName());
            _cancellationTokenSource = new CancellationTokenSource();
            _consumptionInterval = TimeSpan.FromSeconds(batchConfiguration.BatchIntervalInSeconds);
        }

        public async Task StartOrchestration()
        {
            while (true)
            {
                await _consumer.Consume(LogAndHandleMessages, _cancellationTokenSource.Token);
                Thread.Sleep(_consumptionInterval);
            }
            // ReSharper disable once FunctionNeverReturns
        }

        private async Task LogAndHandleMessages(IReadOnlyCollection<string> files)
        {
            using var step = _logger.Step("batch");
            _logger.LogInformation("batch-started");
            await HandleMessages(files);
            _logger.LogInformation("batch-ended");
        }

        protected abstract Task HandleMessages(IReadOnlyCollection<string> files);

        public void Dispose()
        {
            _consumer.Dispose();
            _cancellationTokenSource.Cancel();
            _cancellationTokenSource.Dispose();
        }
    }
}
