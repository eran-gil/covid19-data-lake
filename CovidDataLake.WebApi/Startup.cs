using Amazon.Runtime;
using Amazon.S3;
using CovidDataLake.Cloud.Amazon;
using CovidDataLake.Common;
using CovidDataLake.Common.Locking;
using CovidDataLake.ContentIndexer.Configuration;
using CovidDataLake.ContentIndexer.Indexing;
using CovidDataLake.Pubsub.Kafka.Producer;
using CovidDataLake.Pubsub.Kafka.Producer.Configuration;
using CovidDataLake.Queries.Executors;
using CovidDataLake.Storage.Write;
using CovidDataLake.WebApi.Validation;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;

namespace CovidDataLake.WebApi
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }


        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddMvc().SetCompatibilityVersion(CompatibilityVersion.Version_2_1);
            services.BindConfigurationToContainer<KafkaProducerConfiguration>(Configuration, "Kafka");
            services.BindConfigurationToContainer<DataLakeWriterConfiguration>(Configuration, "Storage");
            services.BindConfigurationToContainer<FileTypeValidationConfiguration>(Configuration, "Validation");
            services.BindConfigurationToContainer<AmazonS3Config>(Configuration, "AmazonGeneralConfig");
            services.BindConfigurationToContainer<AmazonRootIndexFileConfiguration>(Configuration, "AmazonRootIndex");
            services.BindConfigurationToContainer<RedisIndexCacheConfiguration>(Configuration, "RedisIndexCache");
            services.BindConfigurationToContainer<AmazonRootIndexFileConfiguration>(Configuration, "AmazonRootIndex");
            services.BindConfigurationToContainer<BasicAmazonIndexConfiguration>(Configuration, "AmazonIndexFile");
            var redisConnectionString = Configuration.GetValue<string>("Redis");
            var redisConnection = ConnectionMultiplexer.Connect(redisConnectionString);
            services.AddSingleton<IConnectionMultiplexer>(redisConnection);
            var awsCredentials = new EnvironmentVariablesAWSCredentials();
            services.AddSingleton<AWSCredentials>(awsCredentials);
            services.AddSingleton<IRootIndexAccess, AmazonRootIndexFileAccess>();
            services.AddSingleton<IRootIndexCache, RedisRootIndexCache>();
            services.AddSingleton<IAmazonAdapter, AmazonClientAdapter>();
            services.AddSingleton<ILock, RedisLock>();
            services.AddSingleton<IQueryExecutor, NeedleInHaystackQueryExecutor>();

            services.AddSingleton<IDataLakeWriter, FileStreamDataLakeWriter>();
            services.AddSingleton<IFileTypeValidator, ClosedListFileTypeValidator>();
            services.AddSingleton<IProducerFactory, KafkaProducerFactory>();
            services.AddSwaggerGen();
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.

        public void Configure(IApplicationBuilder app, IHostingEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }
            else
            {
                app.UseExceptionHandler("/Error");
                // The default HSTS value is 30 days. You may want to change this for production scenarios, see https://aka.ms/aspnetcore-hsts.
                app.UseHsts();
            }

            app.UseHttpsRedirection();
            app.UseStaticFiles();

            app.UseMvc();

            app.UseSwagger();

            app.UseSwaggerUI(c =>
            {
                c.SwaggerEndpoint("../swagger/v1/swagger.json", "COVID-19 Data Lake");
            });
        }
    }
}
