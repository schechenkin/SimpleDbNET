using AspNetCore.Testing.Authentication.ClaimInjector;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Configuration;
using SimpleDbNET.Api;

namespace SimpleDbNET.IntegrationTests
{
    internal static class TestServer
    {
        public static SimpleDbNetTestsWebApplicationFactory<Startup> ClientFactory { get; private set; }


        public static void Initialize()
        {
            ClientFactory = new SimpleDbNetTestsWebApplicationFactory<Startup>();
            ClientFactory.CreateClient();
        }

        public static void Dispose() => ClientFactory.Dispose();
    }

    public class SimpleDbNetTestsWebApplicationFactory<T> : ClaimInjectorWebApplicationFactory<T> where T : class
    {
        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            base.ConfigureWebHost(builder);

            builder.ConfigureAppConfiguration((hostingContext, config) =>
            {
                config.AddJsonFile("appsettings.json", optional: false, reloadOnChange: false);
#if DEBUG
                config.AddJsonFile("appsettings.Tests.json", optional: false, reloadOnChange: false);
#else
                config.AddJsonFile("appsettings.Tests.json", optional: false, reloadOnChange: false);
#endif
                config.AddEnvironmentVariables();
            });

            builder.ConfigureTestServices(services =>
            {
            });
        }
    }
}
