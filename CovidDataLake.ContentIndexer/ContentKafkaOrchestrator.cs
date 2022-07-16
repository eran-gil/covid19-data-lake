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
            var batchGuid = new Guid();
            var filesList = files.ToList();
            var filesCount = filesList.Count;
            _logger.LogInformation($"Batch {batchGuid} received with {filesCount} files");
            var tableWrappers = new List<IFileTableWrapper>();
            foreach (var filename in filesList)
            {
                var tableWrapper = await GetTableWrapperForFile(filename);
                tableWrappers.Add(tableWrapper);
            }

            await _contentIndexer.IndexTableAsync(tableWrappers);
            _logger.LogInformation($"Batch {batchGuid} finished");

        }

        private async Task<IFileTableWrapper> GetTableWrapperForFile(string filename)
        {
            var fileType = filename.GetExtensionFromPath();
            var tableWrapperFactory =
                _tableWrapperFactories.AsParallel().First(extractor => extractor.IsFileTypeSupported(fileType));
            var downloadedFileName = await _amazonAdapter.DownloadObjectAsync(_bucketName, filename);
            //todo: add handling of no available
            var tableWrapper = tableWrapperFactory.CreateTableWrapperForFile(downloadedFileName);
            return tableWrapper;
        }
    }
}
