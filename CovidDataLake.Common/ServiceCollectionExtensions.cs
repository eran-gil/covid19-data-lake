using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace CovidDataLake.Common
{
    public static class ServiceCollectionExtensions
    {
        public static void BindConfigurationToContainer<T>(this IServiceCollection services, IConfiguration configuration, string sectionName) where T : class, new()
        {
            var configSection = new T();
            configuration.Bind(sectionName, configSection);
            services.AddSingleton(configSection);
        }
    }
}
