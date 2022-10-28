using System.Threading.Tasks;
using Amazon.Runtime;
using CovidDataLake.Cloud.Amazon;
using CovidDataLake.Cloud.Amazon.Configuration;
using CovidDataLake.Common;
using CovidDataLake.Common.Locking;
using CovidDataLake.ContentIndexer.Configuration;
using CovidDataLake.ContentIndexer.Extraction;
using CovidDataLake.ContentIndexer.Extraction.TableWrappers.Csv;
using CovidDataLake.ContentIndexer.Extraction.TableWrappers.Json;
using CovidDataLake.ContentIndexer.Indexing;
using CovidDataLake.ContentIndexer.Indexing.NeedleInHaystack;
using CovidDataLake.ContentIndexer.Indexing.NeedleInHaystack.RootIndex;
using CovidDataLake.Pubsub.Kafka.Consumer;
using CovidDataLake.Pubsub.Kafka.Consumer.Configuration;
using CovidDataLake.Pubsub.Kafka.Orchestration;
using CovidDataLake.Pubsub.Kafka.Orchestration.Configuration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace CovidDataLake.ContentIndexer
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var configuration = BuildConfiguration(args);
            IServiceCollection serviceCollection = new ServiceCollection();
            serviceCollection.BindConfigurationToContainer<KafkaConsumerConfiguration>(configuration, "Kafka");
            serviceCollection.BindConfigurationToContainer<RedisIndexCacheConfiguration>(configuration, "RedisIndexCache");
            serviceCollection.BindConfigurationToContainer<AmazonRootIndexFileConfiguration>(configuration, "AmazonRootIndex");
            serviceCollection.BindConfigurationToContainer<BasicAmazonIndexFileConfiguration>(configuration, "AmazonIndexFile");
            serviceCollection.BindConfigurationToContainer<NeedleInHaystackIndexConfiguration>(configuration, "NeedleInHaystackIndex");
            serviceCollection.BindConfigurationToContainer<BatchOrchestratorConfiguration>(configuration, "BatchConfig");
            var redisConnectionString = configuration.GetValue<string>("Redis");
            var redisConnection = await ConnectionMultiplexer.ConnectAsync(redisConnectionString);
            serviceCollection.AddSingleton<IConnectionMultiplexer>(redisConnection);
            var awsCredentials = new EnvironmentVariablesAWSCredentials();
            serviceCollection.AddSingleton<AWSCredentials>(awsCredentials);
            serviceCollection.AddSingleton<IConsumerFactory, KafkaConsumerFactory>();
            serviceCollection.AddSingleton<IOrchestrator, ContentKafkaOrchestrator>();
            serviceCollection.AddSingleton<IFileTableWrapperFactory, CsvFileTableWrapperFactory>();
            serviceCollection.AddSingleton<IFileTableWrapperFactory, JsonFileTableWrapperFactory>();
            serviceCollection.AddSingleton<IContentIndexer, NeedleInHaystackContentIndexer>();
            serviceCollection.AddSingleton<NeedleInHaystackIndexReader>();
            serviceCollection.AddSingleton<NeedleInHaystackIndexWriter>();
            serviceCollection.AddSingleton<IRootIndexAccess, AmazonRootIndexAccess>();
            serviceCollection.AddSingleton<IRootIndexCache, InMemoryRootIndexCache>();
            serviceCollection.AddSingleton<IAmazonAdapter, AmazonClientAdapter>();
            serviceCollection.AddSingleton<ILock, RedisLock>();
            serviceCollection.AddLogging(builder =>
            {
                builder.ClearProviders();
                builder.SetMinimumLevel(LogLevel.Information);
                builder.AddProvider(new Log4NetProvider("log4net.config"));
            });

            serviceCollection.AddMemoryCache();
            var serviceProvider = serviceCollection.BuildServiceProvider();
            var orchestrator = serviceProvider.GetService<IOrchestrator>();
            if (orchestrator != null)
            {
                await orchestrator.StartOrchestration();
            }
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
