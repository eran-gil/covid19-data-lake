using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CovidDataLake.Cloud.Amazon;
using CovidDataLake.Cloud.Amazon.Configuration;
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
        private readonly IAmazonAdapter _amazonAdapter;
        private readonly string _bucketName;

        public MetadataKafkaOrchestrator(IConsumerFactory consumerFactory, IEnumerable<IMetadataExtractor> extractors, IEnumerable<IMetadataIndexer> indexers, IAmazonAdapter amazonAdapter, BasicAmazonIndexFileConfiguration amazonConfig)
            : base(consumerFactory)
        {
            _extractors = extractors;
            _indexers = indexers;
            _amazonAdapter = amazonAdapter;
            _bucketName = amazonConfig.BucketName;
        }

        protected override async Task HandleMessages(IEnumerable<string> files)
        {
            var allMetadata = new Dictionary<string, List<string>>();
            foreach (var file in files)
            {
                var fileMetadata = await GetMetadataFromFile(file);
                if (fileMetadata == null)
                {
                    continue;
                }

                foreach (var metadata in fileMetadata)
                {
                    if (allMetadata.ContainsKey(metadata.Key))
                    {
                        allMetadata[metadata.Key].Add(metadata.Value);
                    }
                    else
                    {
                        allMetadata[metadata.Key] = new List<string> { metadata.Value };
                    }
                }

            }
            var tasks = new List<Task>();
            foreach (var metadata in allMetadata)
            {
                tasks.AddRange(_indexers.Select(indexer => IndexMetadataAsTask(indexer, metadata)));
            }

            await Task.WhenAll(tasks);
        }

        private async Task<Dictionary<string, string>> GetMetadataFromFile(string filename)
        {
            var downloadedFileName = await _amazonAdapter.DownloadObjectAsync(_bucketName, filename);
            var allMetadata = _extractors.AsParallel().SelectMany(extractor => extractor.ExtractMetadata(downloadedFileName))
                .ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
            return allMetadata;
        }

        private static Task IndexMetadataAsTask(IMetadataIndexer indexer, KeyValuePair<string, List<string>> metadata)
        {
            return Task.Run(() => indexer.IndexMetadata(metadata));
        }
    }
}
