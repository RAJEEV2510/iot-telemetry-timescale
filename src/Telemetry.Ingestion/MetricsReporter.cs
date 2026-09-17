using System.Threading.Channels;
using Telemetry.Core;

namespace Telemetry.Ingestion;

/// <summary>Logs throughput and queue depth every 10 seconds.</summary>
public sealed class MetricsReporter(
    IngestionMetrics metrics,
    ChannelReader<TelemetryReading> queue,
    ILogger<MetricsReporter> logger) : BackgroundService
{
    private static readonly TimeSpan Interval = TimeSpan.FromSeconds(10);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(Interval);
        long lastReceived = 0, lastWritten = 0;

        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken))
            {
                long received = metrics.Received, written = metrics.Written;
                logger.LogInformation(
                    "received={ReceivedRate:F0}/s written={WrittenRate:F0}/s queue={Queue} total_written={Written} rejected={Rejected} dropped={Dropped} failed={Failed}",
                    (received - lastReceived) / Interval.TotalSeconds,
                    (written - lastWritten) / Interval.TotalSeconds,
                    queue.Count, written, metrics.Rejected, metrics.Dropped, metrics.Failed);
                (lastReceived, lastWritten) = (received, written);
            }
        }
        catch (OperationCanceledException)
        {
        }
    }
}
