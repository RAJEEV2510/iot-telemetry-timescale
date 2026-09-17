using System.Threading.Channels;
using Microsoft.Extensions.Options;
using Npgsql;
using NpgsqlTypes;
using Telemetry.Core;

namespace Telemetry.Ingestion;

/// <summary>
/// Drains the channel in batches and bulk-loads them into the TimescaleDB hypertable using
/// binary COPY, which is far cheaper than row-by-row INSERTs.
/// </summary>
public sealed class TimescaleBatchWriter(
    int writerId,
    ChannelReader<TelemetryReading> queue,
    NpgsqlDataSource dataSource,
    IngestionMetrics metrics,
    IOptions<IngestionSettings> settings,
    ILogger<TimescaleBatchWriter> logger) : BackgroundService
{
    private const int MaxAttempts = 5;

    private const string CopyCommand =
        "COPY telemetry (time, device_id, temperature, humidity, battery) FROM STDIN (FORMAT BINARY)";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var maxWait = TimeSpan.FromMilliseconds(settings.Value.MaxBatchDelayMs);

        // Deliberately not bound to stoppingToken: on shutdown the listener completes the channel
        // and this loop keeps writing until everything buffered has been flushed.
        while (true)
        {
            var batch = await BatchReader.ReadBatchAsync(queue, settings.Value.MaxBatchSize, maxWait, CancellationToken.None);
            if (batch.Count == 0)
                break;

            await WriteWithRetryAsync(batch);
        }

        logger.LogInformation("Writer {WriterId} drained and stopped", writerId);
    }

    private async Task WriteWithRetryAsync(List<TelemetryReading> batch)
    {
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await CopyAsync(batch);
                metrics.OnWritten(batch.Count);
                return;
            }
            catch (Exception ex) when (attempt < MaxAttempts)
            {
                var delay = TimeSpan.FromMilliseconds(250 * Math.Pow(2, attempt));
                logger.LogWarning("Writer {WriterId}: COPY of {Count} rows failed (attempt {Attempt}): {Message}. Retrying in {Delay}",
                    writerId, batch.Count, attempt, ex.Message, delay);
                await Task.Delay(delay);
            }
            catch (Exception ex)
            {
                metrics.OnFailed(batch.Count);
                logger.LogError(ex, "Writer {WriterId}: dropping batch of {Count} rows after {Attempts} attempts",
                    writerId, batch.Count, MaxAttempts);
                return;
            }
        }
    }

    private async Task CopyAsync(List<TelemetryReading> batch)
    {
        await using var connection = await dataSource.OpenConnectionAsync();
        await using var importer = await connection.BeginBinaryImportAsync(CopyCommand);

        foreach (var reading in batch)
        {
            await importer.StartRowAsync();
            await importer.WriteAsync(reading.Time.UtcDateTime, NpgsqlDbType.TimestampTz);
            await importer.WriteAsync(reading.DeviceId, NpgsqlDbType.Text);
            await importer.WriteAsync(reading.Temperature, NpgsqlDbType.Double);
            await importer.WriteAsync(reading.Humidity, NpgsqlDbType.Double);
            await importer.WriteAsync(reading.Battery, NpgsqlDbType.Double);
        }

        await importer.CompleteAsync();
    }
}
