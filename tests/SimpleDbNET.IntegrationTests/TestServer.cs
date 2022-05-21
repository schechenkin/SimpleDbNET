using AspNetCore.Testing.Authentication.ClaimInjector;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Configuration;

namespace SimpleDbNET.IntegrationTests
{
    internal static class TestServer
    {
        public static InternalDamTestsWebApplicationFactory<Startup> ClientFactory { get; private set; }


        public static void Initialize()
        {
            ClientFactory = new InternalDamTestsWebApplicationFactory<Startup>();
            ClientFactory.CreateClient();
        }

        public static void Dispose() => ClientFactory.Dispose();
    }

    public class InternalDamTestsWebApplicationFactory<T> : ClaimInjectorWebApplicationFactory<T> where T : class
    {
        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            base.ConfigureWebHost(builder);

            builder.ConfigureAppConfiguration((hostingContext, config) =>
            {
                config.AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);
#if DEBUG
                config.AddJsonFile("appsettings.Tests.json", optional: false, reloadOnChange: true);
#else
                config.AddJsonFile("appsettings.Tests.json", optional: false, reloadOnChange: true);
#endif
                config.AddEnvironmentVariables();
            });

            builder.ConfigureTestServices(services =>
            {
            });
        }
    }
}
