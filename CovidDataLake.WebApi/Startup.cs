using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using CovidDataLake.Kafka.Producer;
using CovidDataLake.Kafka.Producer.Configuration;
using CovidDataLake.Storage.Write;
using Microsoft.AspNetCore.Mvc;
using CovidDataLake.WebApi.Validation;

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
            BindConfigurationToContainer<KafkaProducerConfiguration>(services, "Kafka");
            BindConfigurationToContainer<DataLakeWriterConfiguration>(services, "Storage");
            BindConfigurationToContainer<FileTypeValidationConfiguration>(services, "Validation");
            services.AddSingleton<IDataLakeWriter, StreamDataLakeWriter>();
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

        private void BindConfigurationToContainer<T>(IServiceCollection services, string sectionName) where T : class, new()
        {
            var configSection = new T();
            Configuration.Bind(sectionName, configSection);
            services.AddSingleton(configSection);
        }
    }
}
