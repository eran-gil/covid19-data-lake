using System;
using System.Collections.Concurrent;
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
            var batchGuid = Guid.NewGuid();
            var loggingProperties =
                new Dictionary<string, object> { ["IngestionId"] = batchGuid, ["IngestionType"] = "Metadata" };
            using var scope = _logger.BeginScope(loggingProperties);
            _logger.LogInformation("ingestion-start");
            var allMetadata = new ConcurrentDictionary<string, List<string>>();
            var downloadedFiles = files.AsParallel().Select(async file => await DownloadFile(file))
                .Select(downloadedFile => downloadedFile.Result).ToList();
            var filesMetadata = downloadedFiles.AsParallel().Select(GetMetadataFromFile)
                .Where(fileMetadata => fileMetadata != null).ToList();
            Parallel.ForEach(filesMetadata, fileMetadata =>
            {
                foreach (var metadata in fileMetadata)
                {
                    var metadataValues = allMetadata.GetOrAdd(metadata.Key, s => new List<string>());
                    metadataValues.Add(metadata.Value);
                }
            });
            var filesCount = filesMetadata.Count;
            var tasks = new List<Task>();
            foreach (var metadata in allMetadata)
            {
                tasks.AddRange(_indexers.Select(indexer => IndexMetadataAsTask(indexer, metadata)));
            }

            await Task.WhenAll(tasks);
            var filesLoggingProperties =
                new Dictionary<string, object> { ["FilesCount"] = filesCount };
            using var filesScope = _logger.BeginScope(filesLoggingProperties);
            _logger.LogInformation("ingestion-end");
        }

        private async Task<string> DownloadFile(string filename)
        {
            var downloadedFileName = await _amazonAdapter.DownloadObjectAsync(_bucketName, filename);
            return downloadedFileName;
        }

        private Dictionary<string, string> GetMetadataFromFile(string downloadedFileName)
        {
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
