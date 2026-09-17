namespace Telemetry.Ingestion;

public sealed class IngestionMetrics
{
    private long _received;
    private long _rejected;
    private long _dropped;
    private long _written;
    private long _failed;

    public long Received => Interlocked.Read(ref _received);
    public long Rejected => Interlocked.Read(ref _rejected);
    public long Dropped => Interlocked.Read(ref _dropped);
    public long Written => Interlocked.Read(ref _written);
    public long Failed => Interlocked.Read(ref _failed);

    public void OnReceived() => Interlocked.Increment(ref _received);
    public void OnRejected() => Interlocked.Increment(ref _rejected);
    public void OnDropped() => Interlocked.Increment(ref _dropped);
    public void OnWritten(int count) => Interlocked.Add(ref _written, count);
    public void OnFailed(int count) => Interlocked.Add(ref _failed, count);
}
