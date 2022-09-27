using System.Collections.Concurrent;
using CovidDataLake.Cloud.Amazon;
using CovidDataLake.Cloud.Amazon.Configuration;
using CovidDataLake.Common.Files;
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
        private readonly string? _bucketName;

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
            
            var downloadedFiles = new ConcurrentBag<string>();
            await Parallel.ForEachAsync(files, async (file, _) =>
            {
                var downloadedFile = await DownloadFile(file);
                downloadedFiles.Add(downloadedFile);
            });
            var filesCount = downloadedFiles.Count;
            var totalFilesLength = downloadedFiles.Sum(file => file.GetFileLength());
            var loggingProperties =
                new Dictionary<string, object> { ["IngestionId"] = batchGuid,
                    ["IngestionType"] = "Metadata",
                    ["FilesCount"] = filesCount,
                    ["TotalSize"] = totalFilesLength
                };
            using var scope = _logger.BeginScope(loggingProperties);
            _logger.LogInformation("ingestion-start");
            var filesMetadata = downloadedFiles
                .AsParallel()
                .Select(GetMetadataFromFile)
                .ToList();

            var allMetadata = filesMetadata
                .SelectMany(fileMetadata => fileMetadata)
                .GroupBy(metadataEntry => metadataEntry.Key)
                .ToDictionary(
                    group => group.Key,
                    group => group.Select(metadata => metadata.Value).ToList()
                );

            var indexingTasks = allMetadata.SelectMany(IndexMetadataForAllIndexers);

            await Task.WhenAll(indexingTasks);
            
            _logger.LogInformation("ingestion-end");
        }

        private IEnumerable<Task> IndexMetadataForAllIndexers(KeyValuePair<string, List<string>> metadata)
        {
            return _indexers.Select(indexer => IndexMetadataAsTask(indexer, metadata));
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
