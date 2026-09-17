namespace Telemetry.Ingestion;

public sealed class IngestionSettings
{
    /// <summary>Bounded in-memory buffer between MQTT and the database writers.</summary>
    public int QueueCapacity { get; set; } = 100_000;

    /// <summary>Number of concurrent COPY writers draining the buffer.</summary>
    public int WriterCount { get; set; } = 2;

    public int MaxBatchSize { get; set; } = 5_000;

    public int MaxBatchDelayMs { get; set; } = 500;
}
