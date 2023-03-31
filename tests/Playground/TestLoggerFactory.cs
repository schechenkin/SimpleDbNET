using Microsoft.Extensions.Logging;
using Serilog;

namespace Playground
{
    public static class TestLoggerFactory
    {
        static TestLoggerFactory()
        {
            Instance = LoggerFactory.Create(builder =>
            {
                var logger = new LoggerConfiguration()
                            .MinimumLevel.Error()
                            .WriteTo.Console()
                            .Enrich.FromLogContext()
                            .CreateLogger();

                builder.ClearProviders();
                builder.AddSerilog(logger);
            });
        }

        public static ILoggerFactory Instance;
    }
}
