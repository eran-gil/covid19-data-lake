using System;
using System.Threading.Tasks;
using Amazon.Runtime;
using Amazon.S3;
using CovidDataLake.Cloud.Amazon;
using CovidDataLake.Cloud.Amazon.Configuration;
using CovidDataLake.Common;
using CovidDataLake.Common.Locking;
using CovidDataLake.MetadataIndexer.Extraction;
using CovidDataLake.MetadataIndexer.Extraction.Configuration;
using CovidDataLake.MetadataIndexer.Indexing;
using CovidDataLake.MetadataIndexer.Indexing.Configuration;
using CovidDataLake.Pubsub.Kafka.Consumer;
using CovidDataLake.Pubsub.Kafka.Consumer.Configuration;
using CovidDataLake.Pubsub.Kafka.Orchestration;
using CovidDataLake.Pubsub.Kafka.Orchestration.Configuration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
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
            serviceCollection.BindConfigurationToContainer<MetadataExtractorConfiguration>(configuration, "MetadataExtraction");
            serviceCollection.BindConfigurationToContainer<HyperLogLogMetadataIndexConfiguration>(configuration, "HyperLogLog");
            serviceCollection.BindConfigurationToContainer<CountMinSketchMetadataIndexConfiguration>(configuration, "CountMinSketch");
            serviceCollection.BindConfigurationToContainer<AmazonS3Config>(configuration, "AmazonGeneralConfig");
            serviceCollection.BindConfigurationToContainer<BasicAmazonIndexFileConfiguration>(configuration, "AmazonIndexConfig");
            serviceCollection.BindConfigurationToContainer<BatchOrchestratorConfiguration>(configuration, "BatchConfig");
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
            serviceCollection.AddLogging(builder =>
            {
                builder.ClearProviders();
                builder.SetMinimumLevel(LogLevel.Information);
                builder.AddProvider(new Log4NetProvider("log4net.config"));
            });
            var serviceProvider = serviceCollection.BuildServiceProvider();
            var orchestrator = serviceProvider.GetService<IOrchestrator>();
            await orchestrator?.StartOrchestration()!;
        }

        private static IConfigurationRoot BuildConfiguration(string[] args)
        {
            foreach (var arg in args)
            {
                if (arg.IndexOf("=", StringComparison.Ordinal) < 0) continue;
                var splitted = arg.Split('=');
                var key = splitted[0];
                var val = splitted[1];
                Environment.SetEnvironmentVariable(key, val);
            }
            var configuration = new ConfigurationBuilder()
                .AddCommandLine(args)
                .AddJsonFile("appsettings.json", false, true)
                .AddEnvironmentVariables()
                .Build();
            return configuration;
        }
    }
}
