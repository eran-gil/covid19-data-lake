using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using CovidDataLake.ContentIndexer.Extraction;
using CovidDataLake.ContentIndexer.Indexing;
using CovidDataLake.Kafka.Consumer;
using CovidDataLake.Storage.Utils;

namespace CovidDataLake.ContentIndexer.Orchestration
{
    public class KafkaOrchestrator : IOrchestrator
    {
        private readonly IEnumerable<IFileCsvExtractor> _extractors;
        private readonly IContentIndexer _contentIndexer;
        private readonly IConsumer _consumer;
        private readonly CancellationTokenSource _cancellationTokenSource;

        public KafkaOrchestrator(IConsumerFactory consumerFactory, IEnumerable<IFileCsvExtractor> extractors,
            IContentIndexer contentIndexer)
        {
            _extractors = extractors;
            _contentIndexer = contentIndexer;
            _consumer = consumerFactory.CreateConsumer(Dns.GetHostName());
            _cancellationTokenSource = new CancellationTokenSource();
        }

        public void StartOrchestration()
        {
            while (true)
            {
                _consumer.Consume(HandleMessage, _cancellationTokenSource.Token);
            }
            // ReSharper disable once FunctionNeverReturns
        }

        private void HandleMessage(string filename)
        {
            var fileType = filename.GetExtensionFromPath();
            var fileExtractor = _extractors.AsParallel().First(extractor => extractor.IsFileTypeSupported(fileType));
            var csv = fileExtractor.ExtractCsvFromFile(filename);
            _contentIndexer.IndexCsv(csv);
        }

        public void Dispose()
        {
            _consumer?.Dispose();
            _cancellationTokenSource.Cancel();
            _cancellationTokenSource?.Dispose();
        }
    }
}
