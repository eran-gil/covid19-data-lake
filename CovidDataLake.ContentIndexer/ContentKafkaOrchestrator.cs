using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CovidDataLake.Cloud.Amazon;
using CovidDataLake.Cloud.Amazon.Configuration;
using CovidDataLake.Common.Files;
using CovidDataLake.ContentIndexer.Extraction;
using CovidDataLake.ContentIndexer.Extraction.TableWrappers;
using CovidDataLake.ContentIndexer.Indexing;
using CovidDataLake.Pubsub.Kafka.Consumer;
using CovidDataLake.Pubsub.Kafka.Orchestration;
using CovidDataLake.Pubsub.Kafka.Orchestration.Configuration;
using Microsoft.Extensions.Logging;

namespace CovidDataLake.ContentIndexer
{
    class ContentKafkaOrchestrator : KafkaOrchestratorBase
    {
        private readonly IEnumerable<IFileTableWrapperFactory> _tableWrapperFactories;
        private readonly IContentIndexer _contentIndexer;
        private readonly IAmazonAdapter _amazonAdapter;
        private readonly ILogger<ContentKafkaOrchestrator> _logger;
        private readonly string _bucketName;

        public ContentKafkaOrchestrator(IConsumerFactory consumerFactory, IEnumerable<IFileTableWrapperFactory> tableWrapperFactories,
            IContentIndexer contentIndexer, IAmazonAdapter amazonAdapter, BasicAmazonIndexFileConfiguration amazonConfig,
            ILogger<ContentKafkaOrchestrator> logger, BatchOrchestratorConfiguration batchConfiguration) : base(consumerFactory, batchConfiguration)
        {
            _tableWrapperFactories = tableWrapperFactories;
            _contentIndexer = contentIndexer;
            _amazonAdapter = amazonAdapter;
            _logger = logger;
            _bucketName = amazonConfig.BucketName;
        }

        protected override async Task HandleMessages(IEnumerable<string> files)
        {
            var batchGuid = Guid.NewGuid();
            var loggingProperties =
                new Dictionary<string, object> { ["IngestionId"] = batchGuid, ["IngestionType"] = "Content" };
            using var scope = _logger.BeginScope(loggingProperties);
            var tableWrappers = new ConcurrentBag<IFileTableWrapper>();
            await Parallel.ForEachAsync(files, async (filename, _) =>
            {
                var tableWrapper = await GetTableWrapperForFile(filename);
                if(tableWrapper != null)
                    tableWrappers.Add(tableWrapper);
            });
            var filesCount = tableWrappers.Count;
            _logger.LogInformation("ingestion-start");

            await _contentIndexer.IndexTableAsync(tableWrappers);
            var filesLoggingProperties =
                new Dictionary<string, object> { ["FilesCount"] = filesCount };
            using var filesScope = _logger.BeginScope(filesLoggingProperties);
            _logger.LogInformation("ingestion-end");

        }

        private async Task<IFileTableWrapper> GetTableWrapperForFile(string originFilename)
        {
            var fileType = originFilename.GetExtensionFromPath();
            var tableWrapperFactory =
                _tableWrapperFactories.FirstOrDefault(extractor => extractor.IsFileTypeSupported(fileType));
            if (tableWrapperFactory == null)
            {
                return null;
            }
            var downloadedFileName = await _amazonAdapter.DownloadObjectAsync(_bucketName, originFilename);
            var tableWrapper = tableWrapperFactory.CreateTableWrapperForFile(downloadedFileName, originFilename);
            return tableWrapper;
        }
    }
}
