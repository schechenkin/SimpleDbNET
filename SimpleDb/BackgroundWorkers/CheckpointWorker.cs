using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using SimpleDb.Checkpoint;

namespace SimpleDb.BackgroundWorkers
{
    public class CheckpointWorker : BackgroundService
    {
        private readonly ICheckpoint _checkpoint;
        private readonly ILogger<CheckpointWorker> _logger;

        public CheckpointWorker(ICheckpoint checkpoint, ILogger<CheckpointWorker> logger)
        {
            _checkpoint = checkpoint;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("CheckpointWorker Service is starting.");

            using PeriodicTimer timer = new(TimeSpan.FromMinutes(1));

            try
            {
                while (await timer.WaitForNextTickAsync(stoppingToken))
                {
                    _logger.LogInformation("Checkpoint starts");
                    _checkpoint.Execute();
                    _logger.LogInformation("Checkpoint ends");
                }
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("CheckpointWorker Service is stopping.");
            }
        }
    }
}
