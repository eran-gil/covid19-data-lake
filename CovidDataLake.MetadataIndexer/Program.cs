using System.Threading.Tasks;
using CovidDataLake.Pubsub.Kafka.Orchestration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace CovidDataLake.MetadataIndexer
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var configuration = BuildConfiguration(args);
            IServiceCollection serviceCollection = new ServiceCollection();
            serviceCollection.AddLogging();
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
