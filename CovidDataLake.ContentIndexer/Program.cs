using CovidDataLake.Common;
using CovidDataLake.Common.Hashing;
using CovidDataLake.ContentIndexer.Indexing;
using CovidDataLake.ContentIndexer.Orchestration;
using CovidDataLake.Kafka.Consumer;
using CovidDataLake.Kafka.Consumer.Configuration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace CovidDataLake.ContentIndexer
{
    class Program
    {
        static void Main(string[] args)
        {
            var configuration = BuildConfiguration(args);
            IServiceCollection serviceCollection = new ServiceCollection();
            serviceCollection.AddLogging();
            serviceCollection.BindConfigurationToContainer<KafkaConsumerConfiguration>(configuration, "Kafka");
            serviceCollection.AddSingleton<IConsumerFactory, KafkaConsumerFactory>();
            serviceCollection.AddSingleton<IOrchestrator, KafkaOrchestrator>();
            serviceCollection.AddSingleton<IStringHash, Md5StringHash>();
            serviceCollection.AddSingleton<IIndexFileWriter, NeedleInHaystackIndexFileWriter>();
            var serviceProvider = serviceCollection.BuildServiceProvider();
            var orchestrator = serviceProvider.GetService<IOrchestrator>();
            orchestrator.StartOrchestration();
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
