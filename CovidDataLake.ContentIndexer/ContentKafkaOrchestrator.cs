using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CovidDataLake.Cloud.Amazon;
using CovidDataLake.Cloud.Amazon.Configuration;
using CovidDataLake.ContentIndexer.Extraction;
using CovidDataLake.ContentIndexer.Extraction.TableWrappers;
using CovidDataLake.ContentIndexer.Indexing;
using CovidDataLake.Pubsub.Kafka.Consumer;
using CovidDataLake.Pubsub.Kafka.Orchestration;
using CovidDataLake.Pubsub.Kafka.Orchestration.Configuration;
using CovidDataLake.Storage.Utils;
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
            var filesCount = 0;
            var loggingProperties =
                new Dictionary<string, object> { ["IngestionId"] = batchGuid, ["IngestionType"] = "Content" };
            using var scope = _logger.BeginScope(loggingProperties);
            _logger.LogInformation("ingestion-start");
            var tableWrappers = new List<IFileTableWrapper>();
            foreach (var filename in files)
            {
                var tableWrapper = await GetTableWrapperForFile(filename);
                tableWrappers.Add(tableWrapper);
                filesCount++;
            }

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
                _tableWrapperFactories.First(extractor => extractor.IsFileTypeSupported(fileType));
            var downloadedFileName = await _amazonAdapter.DownloadObjectAsync(_bucketName, originFilename);
            //todo: add handling of no available
            var tableWrapper = tableWrapperFactory.CreateTableWrapperForFile(downloadedFileName, originFilename);
            return tableWrapper;
        }
    }
}
