using System.Diagnostics;
using System.Threading.Channels;
using Telemetry.Core;

namespace Telemetry.Tests;

public class BatchReaderTests
{
    [Fact]
    public async Task Returns_full_batch_immediately_when_enough_items_are_buffered()
    {
        var channel = Channel.CreateUnbounded<int>();
        for (var i = 0; i < 250; i++)
            channel.Writer.TryWrite(i);

        var stopwatch = Stopwatch.StartNew();
        var batch = await BatchReader.ReadBatchAsync(channel.Reader, 100, TimeSpan.FromSeconds(5), CancellationToken.None);

        Assert.Equal(Enumerable.Range(0, 100), batch);
        Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(1));
    }

    [Fact]
    public async Task Flushes_partial_batch_after_max_wait()
    {
        var channel = Channel.CreateUnbounded<int>();
        channel.Writer.TryWrite(1);
        channel.Writer.TryWrite(2);

        var batch = await BatchReader.ReadBatchAsync(channel.Reader, 100, TimeSpan.FromMilliseconds(100), CancellationToken.None);

        Assert.Equal([1, 2], batch);
    }

    [Fact]
    public async Task Collects_items_that_arrive_during_the_wait()
    {
        var channel = Channel.CreateUnbounded<int>();
        channel.Writer.TryWrite(1);

        var readTask = BatchReader.ReadBatchAsync(channel.Reader, 3, TimeSpan.FromSeconds(5), CancellationToken.None);
        await Task.Delay(50);
        channel.Writer.TryWrite(2);
        channel.Writer.TryWrite(3);

        Assert.Equal([1, 2, 3], await readTask);
    }

    [Fact]
    public async Task Returns_remaining_items_then_empty_after_completion()
    {
        var channel = Channel.CreateUnbounded<int>();
        channel.Writer.TryWrite(42);
        channel.Writer.Complete();

        Assert.Equal([42], await BatchReader.ReadBatchAsync(channel.Reader, 10, TimeSpan.FromSeconds(5), CancellationToken.None));
        Assert.Empty(await BatchReader.ReadBatchAsync(channel.Reader, 10, TimeSpan.FromSeconds(5), CancellationToken.None));
    }

    [Fact]
    public async Task Honors_caller_cancellation()
    {
        var channel = Channel.CreateUnbounded<int>();
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => BatchReader.ReadBatchAsync(channel.Reader, 10, TimeSpan.FromSeconds(5), cts.Token));
    }
}
