using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CovidDataLake.Cloud.Amazon;
using CovidDataLake.Cloud.Amazon.Configuration;
using CovidDataLake.MetadataIndexer.Extraction;
using CovidDataLake.MetadataIndexer.Indexing;
using CovidDataLake.Pubsub.Kafka.Consumer;
using CovidDataLake.Pubsub.Kafka.Orchestration;
using CovidDataLake.Pubsub.Kafka.Orchestration.Configuration;
using Microsoft.Extensions.Logging;

namespace CovidDataLake.MetadataIndexer
{
    class MetadataKafkaOrchestrator : KafkaOrchestratorBase
    {
        private readonly IEnumerable<IMetadataExtractor> _extractors;
        private readonly IEnumerable<IMetadataIndexer> _indexers;
        private readonly IAmazonAdapter _amazonAdapter;
        private readonly ILogger<MetadataKafkaOrchestrator> _logger;
        private readonly string _bucketName;

        public MetadataKafkaOrchestrator(IConsumerFactory consumerFactory, IEnumerable<IMetadataExtractor> extractors,
            IEnumerable<IMetadataIndexer> indexers, IAmazonAdapter amazonAdapter,
            BasicAmazonIndexFileConfiguration amazonConfig, ILogger<MetadataKafkaOrchestrator> logger,
            BatchOrchestratorConfiguration batchConfiguration) : base(consumerFactory, batchConfiguration)
        {
            _extractors = extractors;
            _indexers = indexers;
            _amazonAdapter = amazonAdapter;
            _logger = logger;
            _bucketName = amazonConfig.BucketName;
        }

        protected override async Task HandleMessages(IEnumerable<string> files)
        {
            var batchGuid = new Guid();
            var filesList = files.ToList();
            var filesCount = filesList.Count; 
            _logger.LogInformation($"Batch {batchGuid} received with {filesCount} files");
            var allMetadata = new Dictionary<string, List<string>>();
            foreach (var file in filesList)
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
            _logger.LogInformation($"Batch {batchGuid} finished");
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
