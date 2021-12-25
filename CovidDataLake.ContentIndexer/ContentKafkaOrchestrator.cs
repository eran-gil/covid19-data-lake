using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CovidDataLake.ContentIndexer.Extraction;
using CovidDataLake.ContentIndexer.Indexing;
using CovidDataLake.Kafka.Consumer;
using CovidDataLake.Kafka.Orchestration;
using CovidDataLake.Storage.Utils;

namespace CovidDataLake.ContentIndexer
{
    class ContentKafkaOrchestrator : KafkaOrchestratorBase
    {
        private readonly IEnumerable<IFileTableWrapperFactory> _tableWrapperFactories;
        private readonly IContentIndexer _contentIndexer;
        public ContentKafkaOrchestrator(IConsumerFactory consumerFactory, IEnumerable<IFileTableWrapperFactory> tableWrapperFactories, IContentIndexer contentIndexer) : base(consumerFactory)
        {
            _tableWrapperFactories = tableWrapperFactories;
            _contentIndexer = contentIndexer;
        }

        protected override async Task HandleMessage(string filename)
        {
            var fileType = filename.GetExtensionFromPath();
            var tableWrapperFactory = _tableWrapperFactories.AsParallel().First(extractor => extractor.IsFileTypeSupported(fileType));
            //todo: add handling of no available
            var tableWrapper = tableWrapperFactory.CreateTableWrapperForFile(filename);
            await _contentIndexer.IndexTableAsync(tableWrapper);

        }
    }
}
