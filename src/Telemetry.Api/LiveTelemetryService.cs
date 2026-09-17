using System.Collections.Concurrent;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Options;
using MQTTnet;
using MQTTnet.Client;
using Telemetry.Core;

namespace Telemetry.Api;

public sealed class TelemetryHub : Hub;

/// <summary>Latest reading per device plus a running message counter.</summary>
public sealed class LiveTelemetryCache
{
    private long _messages;

    public ConcurrentDictionary<string, TelemetryReading> Latest { get; } = new();

    public long Messages => Interlocked.Read(ref _messages);

    public void Update(TelemetryReading reading)
    {
        Latest[reading.DeviceId] = reading;
        Interlocked.Increment(ref _messages);
    }
}

/// <summary>
/// Subscribes to all device telemetry (independently of the ingestion pipeline) and pushes a
/// throttled snapshot to dashboard clients once per second, so the browser is never flooded.
/// </summary>
public sealed class LiveTelemetryService(
    LiveTelemetryCache cache,
    IHubContext<TelemetryHub> hub,
    IOptions<MqttSettings> settings,
    ILogger<LiveTelemetryService> logger) : BackgroundService
{
    private const int MaxDevicesInSnapshot = 500;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var client = new MqttFactory().CreateMqttClient();
        client.ApplicationMessageReceivedAsync += e =>
        {
            if (TelemetryParser.TryParse(e.ApplicationMessage.Topic, e.ApplicationMessage.PayloadSegment, out var reading))
                cache.Update(reading);
            return Task.CompletedTask;
        };

        var options = new MqttClientOptionsBuilder()
            .WithTcpServer(settings.Value.Host, settings.Value.Port)
            .WithClientId($"api-live-{Guid.NewGuid():N}")
            .WithCleanSession()
            .Build();

        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
        long lastMessages = 0;

        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken))
            {
                if (!client.IsConnected)
                {
                    try
                    {
                        await client.ConnectAsync(options, stoppingToken);
                        await client.SubscribeAsync(TelemetryTopics.AllDevices, cancellationToken: stoppingToken);
                        logger.LogInformation("Live feed connected to MQTT {Host}:{Port}", settings.Value.Host, settings.Value.Port);
                    }
                    catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
                    {
                        logger.LogWarning("Live feed MQTT connection failed: {Message}", ex.Message);
                        continue;
                    }
                }

                var messages = cache.Messages;
                var devices = cache.Latest.Values
                    .OrderBy(r => r.DeviceId, StringComparer.Ordinal)
                    .Take(MaxDevicesInSnapshot)
                    .ToArray();

                await hub.Clients.All.SendAsync("snapshot", new
                {
                    messagesPerSecond = messages - lastMessages,
                    deviceCount = cache.Latest.Count,
                    devices,
                }, stoppingToken);

                lastMessages = messages;
            }
        }
        catch (OperationCanceledException)
        {
        }
    }
}
