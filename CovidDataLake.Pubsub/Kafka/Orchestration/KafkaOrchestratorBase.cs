using System.Net;
using System.Threading;
using System.Threading.Tasks;
using CovidDataLake.Pubsub.Kafka.Consumer;

namespace CovidDataLake.Pubsub.Kafka.Orchestration
{
    public abstract class KafkaOrchestratorBase : IOrchestrator
    {

        private readonly IConsumer _consumer;
        private readonly CancellationTokenSource _cancellationTokenSource;

        public KafkaOrchestratorBase(IConsumerFactory consumerFactory)
        {
            _consumer = consumerFactory.CreateConsumer(Dns.GetHostName());
            _cancellationTokenSource = new CancellationTokenSource();
        }

        public async Task StartOrchestration()
        {
            while (true)
            {
                await _consumer.Consume(HandleMessage, _cancellationTokenSource.Token);
            }
            // ReSharper disable once FunctionNeverReturns
        }

        protected abstract Task HandleMessage(string filename);

        public void Dispose()
        {
            _consumer?.Dispose();
            _cancellationTokenSource.Cancel();
            _cancellationTokenSource?.Dispose();
        }
    }
}
