using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using CovidDataLake.ContentIndexer.Extraction;
using CovidDataLake.ContentIndexer.Indexing;
using CovidDataLake.Kafka.Consumer;
using CovidDataLake.Storage.Utils;

namespace CovidDataLake.ContentIndexer.Orchestration
{
    public class KafkaOrchestrator : IOrchestrator
    {
        private readonly IEnumerable<IFileTableWrapperFactory> _tableWrapperFactories;
        private readonly IContentIndexer _contentIndexer;
        private readonly IConsumer _consumer;
        private readonly CancellationTokenSource _cancellationTokenSource;

        public KafkaOrchestrator(IConsumerFactory consumerFactory, IEnumerable<IFileTableWrapperFactory> tableWrapperFactories,
            IContentIndexer contentIndexer)
        {
            _tableWrapperFactories = tableWrapperFactories;
            _contentIndexer = contentIndexer;
            _consumer = consumerFactory.CreateConsumer(Dns.GetHostName());
            _cancellationTokenSource = new CancellationTokenSource();
        }
        //TODO: create orchestrator that takes from kafka, moves to csv extractor and then to writer (also bloom)

        public async Task StartOrchestration()
        {
            while (true)
            {
                await _consumer.Consume(HandleMessage, _cancellationTokenSource.Token);
            }
            // ReSharper disable once FunctionNeverReturns
        }

        private async Task HandleMessage(string filename)
        {
            var fileType = filename.GetExtensionFromPath();
            var tableWrapperFactory = _tableWrapperFactories.AsParallel().First(extractor => extractor.IsFileTypeSupported(fileType));
            var tableWrapper = tableWrapperFactory.CreateTableWrapperForFile(filename);
            await _contentIndexer.IndexTable(tableWrapper);
        }

        public void Dispose()
        {
            _consumer?.Dispose();
            _cancellationTokenSource.Cancel();
            _cancellationTokenSource?.Dispose();
        }
    }
}
