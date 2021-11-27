using System.Threading.Tasks;
using Amazon.Runtime;
using Amazon.S3;
using CovidDataLake.Amazon;
using CovidDataLake.Common;
using CovidDataLake.Common.Locking;
using CovidDataLake.ContentIndexer.Configuration;
using CovidDataLake.ContentIndexer.Extraction;
using CovidDataLake.ContentIndexer.Indexing;
using CovidDataLake.ContentIndexer.Orchestration;
using CovidDataLake.Kafka.Consumer;
using CovidDataLake.Kafka.Consumer.Configuration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;

namespace CovidDataLake.ContentIndexer
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var configuration = BuildConfiguration(args);
            IServiceCollection serviceCollection = new ServiceCollection();
            serviceCollection.AddLogging();
            serviceCollection.BindConfigurationToContainer<KafkaConsumerConfiguration>(configuration, "Kafka");
            serviceCollection.BindConfigurationToContainer<RedisIndexCacheConfiguration>(configuration, "RedisIndexCache");
            serviceCollection.BindConfigurationToContainer<AmazonRootIndexFileConfiguration>(configuration, "AmazonRootIndex");
            serviceCollection.BindConfigurationToContainer<BasicAmazonIndexConfiguration>(configuration, "AmazonIndexFile");
            serviceCollection.BindConfigurationToContainer<NeedleInHaystackIndexConfiguration>(configuration, "NeedleInHaystackIndex");
            serviceCollection.BindConfigurationToContainer<AmazonS3Config>(configuration, "AmazonGeneralConfig");
            var redisConnectionString = configuration.GetValue<string>("Redis");
            var redisConnection = await ConnectionMultiplexer.ConnectAsync(redisConnectionString);
            serviceCollection.AddSingleton<IConnectionMultiplexer>(redisConnection);
            var awsCredentials = new EnvironmentVariablesAWSCredentials();
            serviceCollection.AddSingleton<AWSCredentials>(awsCredentials);
            serviceCollection.AddSingleton<IConsumerFactory, KafkaConsumerFactory>();
            serviceCollection.AddSingleton<IOrchestrator, KafkaOrchestrator>();
            serviceCollection.AddSingleton<IIndexFileWriter, AmazonIndexFileWriter>();
            serviceCollection.AddSingleton<IFileTableWrapperFactory, CsvFileTableWrapperFactory>();
            serviceCollection.AddSingleton<IContentIndexer, NeedleInHaystackContentIndexer>();
            serviceCollection.AddSingleton<IIndexFileAccess, NeedleInHaystackIndexFileAccess>();
            serviceCollection.AddSingleton<IIndexFileWriter, AmazonIndexFileWriter>();
            serviceCollection.AddSingleton<IRootIndexAccess, AmazonRootIndexFileAccess>();
            serviceCollection.AddSingleton<IRootIndexCache, RedisRootIndexCache>();
            serviceCollection.AddSingleton<IAmazonAdapter, AmazonClientAdapter>();
            serviceCollection.AddSingleton<ILock, RedisLock>();
            
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
