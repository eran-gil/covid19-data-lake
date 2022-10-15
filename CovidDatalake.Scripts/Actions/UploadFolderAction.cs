using System.Net;
using CovidDataLake.Cloud.Amazon;
using CovidDataLake.Cloud.Amazon.Configuration;
using CovidDataLake.Cloud.Amazon.Utils;
using CovidDataLake.Common.Files;
using CovidDataLake.Pubsub.Kafka.Producer;

namespace CovidDataLake.Scripts.Actions
{
    internal class UploadFolderAction : IScriptAction
    {
        private readonly IAmazonAdapter _amazonAdapter;
        private readonly IProducer _producer;
        private readonly string _bucketName;

        public string Name => "upload_folder";

        public UploadFolderAction(IAmazonAdapter amazonAdapter, IProducerFactory producerFactory, BasicAmazonIndexFileConfiguration config)
        {
            _amazonAdapter = amazonAdapter;
            _producer = producerFactory.CreateProducer(Dns.GetHostName());
            _bucketName = config.BucketName!;
        }

        public async Task<bool> Run()
        {
            try
            {
                Console.WriteLine("Enter the folder containing the files to upload:");
                var folder = Console.ReadLine();
                var allFiles = Directory.GetFiles(folder!, "*", SearchOption.AllDirectories);
                var tasks = allFiles.Select(UploadFileToCloud);
                await Task.WhenAll(tasks);
                return true;
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                return false;
            }
        }

        private async Task UploadFileToCloud(string filename)
        {
            var fileType = filename.GetExtensionFromPath();
            var cloudPath = FilePathGeneration.GenerateFilePath(fileType);
            await _amazonAdapter.UploadObjectAsync(_bucketName, cloudPath, filename);
            await _producer.SendMessage(cloudPath);
        }
    }
}
