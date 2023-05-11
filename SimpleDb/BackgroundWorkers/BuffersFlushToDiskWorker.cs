using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace SimpleDb.BackgroundWorkers
{
    public class BuffersFlushToDiskWorker : BackgroundService
    {
        private readonly ISimpleDbServer dbServer;
        private readonly ILogger<BuffersFlushToDiskWorker> _logger;

        public BuffersFlushToDiskWorker(ISimpleDbServer dbServer, ILogger<BuffersFlushToDiskWorker> logger)
        {
            this.dbServer = dbServer;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("BuffersFlushToDiskWorker Service running.");

            using PeriodicTimer timer = new(TimeSpan.FromMinutes(1));

            try
            {
                while (await timer.WaitForNextTickAsync(stoppingToken))
                {
                    _logger.LogInformation("run FlushDirtyBuffers");
                    dbServer.GetBufferManager().FlushDirtyBuffers();
                    _logger.LogInformation("end FlushDirtyBuffers");
                }
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("BuffersFlushToDiskWorker Service is stopping.");
            }
        }
    }
}
