using System;
using CovidDataLake.Common;
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
            var serviceProvider = serviceCollection.BuildServiceProvider();
            //TODO: create orchestrator that takes from kafka, moves to csv extractor and then to writer (also bloom)
            //TODO: erase
            Console.WriteLine(serviceProvider);
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
