using System.Threading.Channels;

namespace Telemetry.Core;

public static class BatchReader
{
    /// <summary>
    /// Waits for at least one item, then keeps collecting until the batch is full or
    /// <paramref name="maxWait"/> elapses. Large batches under load, low latency when quiet.
    /// Returns an empty list only when the channel is completed and drained.
    /// </summary>
    public static async Task<List<T>> ReadBatchAsync<T>(
        ChannelReader<T> reader, int maxBatchSize, TimeSpan maxWait, CancellationToken cancellationToken)
    {
        var batch = new List<T>(maxBatchSize);
        if (!await reader.WaitToReadAsync(cancellationToken))
            return batch;

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(maxWait);

        try
        {
            while (batch.Count < maxBatchSize)
            {
                while (batch.Count < maxBatchSize && reader.TryRead(out var item))
                    batch.Add(item);

                if (batch.Count >= maxBatchSize || !await reader.WaitToReadAsync(timeout.Token))
                    break;
            }
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            // maxWait elapsed: flush what we have.
        }

        return batch;
    }
}
