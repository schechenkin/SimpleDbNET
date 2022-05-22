using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace SimpleDb.Extensions
{
    public static class ServiceCollectionExtension
    {
        public static IServiceCollection AddSimpleDb(this IServiceCollection services, IConfiguration configuration)
        {
            services.AddSingleton<ISimpleDbServer, SimpleDbConext>();
            
            return services;
        }

        private static bool IsTest()
        {
            var env_name = System.Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT");
            return env_name == "Test" || env_name == "Tests" || env_name == "Tests_local";
        }
    }
}
