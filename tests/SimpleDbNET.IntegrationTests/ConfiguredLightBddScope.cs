using LightBDD.Core.Configuration;
using LightBDD.Core.Execution;
using LightBDD.Core.Extensibility.Execution;
using LightBDD.XUnit2;
using Microsoft.Extensions.DependencyInjection;
using SimpleDbNET.IntegrationTests;
using Xunit;


[assembly: CollectionBehavior(DisableTestParallelization = true)]
[assembly: ConfiguredLightBddScope]

namespace SimpleDbNET.IntegrationTests
{
    internal class ConfiguredLightBddScopeAttribute : LightBddScopeAttribute
    {
        protected override void OnSetUp()
        {
            Environment.SetEnvironmentVariable("ASPNETCORE_ENVIRONMENT", "Test");

            TestServer.Initialize();

            ServiceScopeFactory.Init(() => TestServer.ClientFactory.Server.Services.CreateScope());

            using (var serviceScope = ServiceScopeFactory.CreateScope())
            {
                //serviceScope.ServiceProvider.GetService<MediaStorageDbContext>().Clear();
            }
        }

        protected override void OnConfigure(LightBddConfiguration configuration)
        {
            base.OnConfigure(configuration);

            configuration.ExecutionExtensionsConfiguration()
                            .EnableStepDecorator<OwnScope>();
        }
    }

    public class OwnScope : IStepDecorator
    {
        public static IServiceScope Scope;

        public async Task ExecuteAsync(IStep step, Func<Task> stepInvocation)
        {
            using (var serviceScope = TestServer.ClientFactory.Server.Services.CreateScope())
            {
                Scope = serviceScope;
                try
                {
                    await stepInvocation();
                }
                finally
                {
                    Scope = null;
                }
            }
        }
    }
}
