using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace SimpleDb.BackgroundWorkers
{
    public class LogFlushToDiskWorker : BackgroundService
    {
        private readonly ISimpleDbServer dbServer;
        private readonly ILogger<LogFlushToDiskWorker> _logger;

        public LogFlushToDiskWorker(ISimpleDbServer dbServer, ILogger<LogFlushToDiskWorker> logger)
        {
            this.dbServer = dbServer;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("LogFlushToDiskWorker Service is starting.");

            using PeriodicTimer timer = new(TimeSpan.FromMinutes(1));

            try
            {
                while (await timer.WaitForNextTickAsync(stoppingToken))
                {
                    _logger.LogInformation("Flush log to Disk");
                    dbServer.Log.Flush();
                }
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("LogFlushToDiskWorker Service is stopping.");
            }
        }
    }
}
