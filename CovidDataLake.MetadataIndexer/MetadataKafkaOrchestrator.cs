using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CovidDataLake.MetadataIndexer.Extraction;
using CovidDataLake.MetadataIndexer.Indexing;
using CovidDataLake.Pubsub.Kafka.Consumer;
using CovidDataLake.Pubsub.Kafka.Orchestration;

namespace CovidDataLake.MetadataIndexer
{
    class MetadataKafkaOrchestrator : KafkaOrchestratorBase
    {
        private readonly IEnumerable<IMetadataExtractor> _extractors;
        private readonly IEnumerable<IMetadataIndexer> _indexers;

        public MetadataKafkaOrchestrator(IConsumerFactory consumerFactory, IEnumerable<IMetadataExtractor> extractors, IEnumerable<IMetadataIndexer> indexers)
            : base(consumerFactory)
        {
            _extractors = extractors;
            _indexers = indexers;
        }

        protected override Task HandleMessage(string filename)
        {
            var allMetadata = _extractors.AsParallel().SelectMany(extractor => extractor.ExtractMetadata(filename))
                    .ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
            var tasks = new List<Task>();
            foreach (var metadata in allMetadata)
            {
                tasks.AddRange(_indexers.Select(indexer => IndexMetadataAsTask(indexer, metadata)));
            }

            return Task.WhenAll(tasks);

        }

        private static Task IndexMetadataAsTask(IMetadataIndexer indexer, KeyValuePair<string, string> metadata)
        {
            return Task.Run(() => indexer.IndexMetadata(metadata));
        }
    }
}
