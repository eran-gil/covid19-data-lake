using System.Threading.Tasks;
using Amazon.Runtime;
using Amazon.S3;
using CovidDataLake.Cloud.Amazon;
using CovidDataLake.Common;
using CovidDataLake.Common.Locking;
using CovidDataLake.MetadataIndexer.Extraction;
using CovidDataLake.MetadataIndexer.Indexing;
using CovidDataLake.MetadataIndexer.Indexing.Configuration;
using CovidDataLake.Pubsub.Kafka.Consumer;
using CovidDataLake.Pubsub.Kafka.Consumer.Configuration;
using CovidDataLake.Pubsub.Kafka.Orchestration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;

namespace CovidDataLake.MetadataIndexer
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var configuration = BuildConfiguration(args);
            IServiceCollection serviceCollection = new ServiceCollection();
            serviceCollection.BindConfigurationToContainer<KafkaConsumerConfiguration>(configuration, "Kafka");
            serviceCollection.BindConfigurationToContainer<HyperLogLogMetadataIndexConfiguration>(configuration, "HyperLogLog");
            serviceCollection.BindConfigurationToContainer<CountMinSketchMetadataIndexConfiguration>(configuration, "CountMinSketch");
            serviceCollection.BindConfigurationToContainer<AmazonS3Config>(configuration, "AmazonGeneralConfig");
            serviceCollection.AddSingleton<IMetadataExtractor, TikaMetadataExtractor>();
            serviceCollection.AddSingleton<IMetadataIndexer, HyperLogLogMetadataIndexer>();
            serviceCollection.AddSingleton<IMetadataIndexer, CountMinSketchMetadataIndexer>();
            serviceCollection.AddSingleton<IConsumerFactory, KafkaConsumerFactory>();
            var redisConnectionString = configuration.GetValue<string>("Redis");
            var redisConnection = await ConnectionMultiplexer.ConnectAsync(redisConnectionString);
            serviceCollection.AddSingleton<IConnectionMultiplexer>(redisConnection);
            var accessKey = configuration.GetValue<string>("AWS_ACCESS_KEY_ID");
            var secretKey = configuration.GetValue<string>("AWS_SECRET_ACCESS_KEY");
            var awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
            serviceCollection.AddSingleton<AWSCredentials>(awsCredentials);
            serviceCollection.AddSingleton<IAmazonAdapter, AmazonClientAdapter>();
            serviceCollection.AddSingleton<ILock, RedisLock>();
            serviceCollection.AddSingleton<IOrchestrator, MetadataKafkaOrchestrator>();
            /*serviceCollection.AddLogging();*/
            var serviceProvider = serviceCollection.BuildServiceProvider();
            var orchestrator = serviceProvider.GetService<IOrchestrator>();
            await orchestrator.StartOrchestration();
        }

        private static IConfigurationRoot BuildConfiguration(string[] args)
        {
            var configuration = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", false, true)
                .AddEnvironmentVariables()
                .AddCommandLine(args)
                .Build();
            return configuration;
        }
    }
}
