using System.Net;
using CovidDataLake.Cloud.Amazon;
using CovidDataLake.Cloud.Amazon.Configuration;
using CovidDataLake.Pubsub.Kafka.Producer;

namespace CovidDataLake.Scripts.Actions
{
    internal class IndexFolderAction : IScriptAction
    {
        private readonly IAmazonAdapter _amazonAdapter;
        private readonly IProducer _producer;
        private readonly string _bucketName;
        public IndexFolderAction(IAmazonAdapter amazonAdapter, IProducerFactory producerFactory, BasicAmazonIndexFileConfiguration config)
        {
            _amazonAdapter = amazonAdapter;
            _producer = producerFactory.CreateProducer(Dns.GetHostName());
            _bucketName = config.BucketName!;
        }

        public string Name => "index_folder";
        public async Task<bool> Run()
        {
            Console.WriteLine("Enter the cloud prefix for content to be indexed:");
            var path = Console.ReadLine();
            if (string.IsNullOrEmpty(path!.Trim()))
            {
                Console.WriteLine("An empty path is not accepted");
                return false;
            }
            var pathObjects = (await _amazonAdapter.ListObjectsAsync(_bucketName, path!)).ToList();
            foreach (var pathObject in pathObjects)
            {
                await _producer.SendMessage(pathObject);
            }

            Console.WriteLine($"Published {pathObjects.Count} files for indexing");
            return true;
        }
    }
}
