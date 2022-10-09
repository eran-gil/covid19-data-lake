using Amazon.Runtime;
using Amazon.S3;
using CovidDataLake.Cloud.Amazon;
using CovidDataLake.Cloud.Amazon.Configuration;
using CovidDataLake.Common;
using CovidDataLake.Pubsub.Kafka.Admin;
using CovidDataLake.Pubsub.Kafka.Admin.Configuration;
using CovidDataLake.Pubsub.Kafka.Producer;
using CovidDataLake.Pubsub.Kafka.Producer.Configuration;
using CovidDataLake.Scripts.Actions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace CovidDataLake.Scripts
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            var configuration = BuildConfiguration(args);
            var serviceProvider = CreateServiceProvider(configuration);
            var runner = serviceProvider.GetService<IScriptRunner>();
            await runner!.Run();
        }

        private static ServiceProvider CreateServiceProvider(IConfiguration configuration)
        {
            IServiceCollection serviceCollection = new ServiceCollection();
            serviceCollection.BindConfigurationToContainer<KafkaProducerConfiguration>(configuration, "Kafka");
            serviceCollection.BindConfigurationToContainer<KafkaAdminClientConfiguration>(configuration, "Kafka");
            var storageConfig = configuration.GetSection("AmazonGeneralConfig").Get<AmazonStorageConfig>();
            var amazonS3Config = new AmazonS3Config { ServiceURL = storageConfig.ServiceUrl };
            serviceCollection.AddSingleton(amazonS3Config);
            serviceCollection.BindConfigurationToContainer<BasicAmazonIndexFileConfiguration>(configuration, "AmazonIndexFile");
            serviceCollection.AddSingleton<IProducerFactory, KafkaProducerFactory>();
            serviceCollection.AddSingleton<IPubSubAdminFactory, KafkaAdminClientFactory>();
            var redisConnectionString = configuration.GetValue<string>("Redis");
            var redisConnection = ConnectionMultiplexer.Connect(redisConnectionString);
            serviceCollection.AddSingleton<IConnectionMultiplexer>(redisConnection);
            var accessKey = configuration.GetValue<string>("AWS_ACCESS_KEY_ID");
            var secretKey = configuration.GetValue<string>("AWS_SECRET_ACCESS_KEY");
            var awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
            serviceCollection.AddSingleton<AWSCredentials>(awsCredentials);
            serviceCollection.AddSingleton<IAmazonAdapter, AmazonClientAdapter>();
            serviceCollection.AddSingleton<IScriptAction, UploadFolderAction>();
            serviceCollection.AddSingleton<IScriptAction, IndexFolderAction>();
            serviceCollection.AddSingleton<IScriptAction, CleanupAction>();
            serviceCollection.AddSingleton<IScriptAction, ResetRedisAction>();
            serviceCollection.AddSingleton<IScriptAction, ResetKafkaAction>();
            serviceCollection.AddSingleton<IScriptAction, ExitAction>();
            serviceCollection.AddSingleton<IScriptRunner, ScriptRunner>();
            serviceCollection.AddLogging(builder =>
            {
                builder.ClearProviders();
                builder.SetMinimumLevel(LogLevel.Critical);
                //builder.AddProvider(new Log4NetProvider("log4net.config"));
            });
            var serviceProvider = serviceCollection.BuildServiceProvider();
            return serviceProvider;
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

