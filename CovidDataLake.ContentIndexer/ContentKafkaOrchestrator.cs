using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CovidDataLake.Cloud.Amazon;
using CovidDataLake.Cloud.Amazon.Configuration;
using CovidDataLake.ContentIndexer.Extraction;
using CovidDataLake.ContentIndexer.Indexing;
using CovidDataLake.Pubsub.Kafka.Consumer;
using CovidDataLake.Pubsub.Kafka.Orchestration;
using CovidDataLake.Storage.Utils;

namespace CovidDataLake.ContentIndexer
{
    class ContentKafkaOrchestrator : KafkaOrchestratorBase
    {
        private readonly IEnumerable<IFileTableWrapperFactory> _tableWrapperFactories;
        private readonly IContentIndexer _contentIndexer;
        private readonly IAmazonAdapter _amazonAdapter;
        private readonly string _bucketName;

        public ContentKafkaOrchestrator(IConsumerFactory consumerFactory, IEnumerable<IFileTableWrapperFactory> tableWrapperFactories,
            IContentIndexer contentIndexer, IAmazonAdapter amazonAdapter, BasicAmazonIndexFileConfiguration amazonConfig) : base(consumerFactory)
        {
            _tableWrapperFactories = tableWrapperFactories;
            _contentIndexer = contentIndexer;
            _amazonAdapter = amazonAdapter;
            _bucketName = amazonConfig.BucketName;
        }

        protected override async Task HandleMessage(string filename)
        {
            var fileType = filename.GetExtensionFromPath();
            var tableWrapperFactory = _tableWrapperFactories.AsParallel().First(extractor => extractor.IsFileTypeSupported(fileType));
            var downloadedFileName = await _amazonAdapter.DownloadObjectAsync(_bucketName, filename);
            //todo: add handling of no available
            var tableWrapper = tableWrapperFactory.CreateTableWrapperForFile(downloadedFileName);
            await _contentIndexer.IndexTableAsync(tableWrapper);

        }
    }
}
