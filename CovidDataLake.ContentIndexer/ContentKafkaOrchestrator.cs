using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CovidDataLake.Cloud.Amazon;
using CovidDataLake.Cloud.Amazon.Configuration;
using CovidDataLake.Common;
using CovidDataLake.Common.Files;
using CovidDataLake.ContentIndexer.Extraction;
using CovidDataLake.ContentIndexer.Extraction.TableWrappers;
using CovidDataLake.ContentIndexer.Indexing;
using CovidDataLake.Pubsub.Kafka.Consumer;
using CovidDataLake.Pubsub.Kafka.Orchestration;
using CovidDataLake.Pubsub.Kafka.Orchestration.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

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
            ILogger<ContentKafkaOrchestrator> logger, BatchOrchestratorConfiguration batchConfiguration) : base(consumerFactory, batchConfiguration, logger)
        {
            _tableWrapperFactories = tableWrapperFactories;
            _contentIndexer = contentIndexer;
            _amazonAdapter = amazonAdapter;
            _logger = logger;
            _bucketName = amazonConfig.BucketName;
        }

        protected override async Task HandleMessages(IReadOnlyCollection<string> files)
        {
            var batchGuid = Guid.NewGuid();
            var tableWrappers = (await GetTableWrappersForFiles(files).ConfigureAwait(false)).ToList();
            var filesTotalSize = tableWrappers.Sum(tableWrapper => tableWrapper.Filename.GetFileLength());
            var loggingProperties =
                new Dictionary<string, object>
                {
                    ["IngestionId"] = batchGuid,
                    ["IngestionType"] = "Content",
                    ["FilesCount"] = files.Count,
                    ["TotalSize"] = filesTotalSize,
                    ["Files"] = JsonConvert.SerializeObject(files),
                };
            using var step = _logger.Step("ingestion", loggingProperties);
            await _contentIndexer.IndexTableAsync(tableWrappers).ConfigureAwait(false);

        }

        private async Task<IEnumerable<IFileTableWrapper>> GetTableWrappersForFiles(IEnumerable<string> files)
        {
            var tableWrapperTasks = files.Select(GetTableWrapperForFile);
            var tableWrappers = await Task.WhenAll(tableWrapperTasks).ConfigureAwait(false);
            return tableWrappers.NotNull();
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
            var downloadedFileName = await _amazonAdapter.DownloadObjectAsync(_bucketName, originFilename).ConfigureAwait(false);
            var tableWrapper = tableWrapperFactory.CreateTableWrapperForFile(downloadedFileName, originFilename);
            return tableWrapper;
        }
    }
}
