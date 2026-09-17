using System.Threading.Channels;
using Microsoft.Extensions.Options;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Formatter;
using Telemetry.Core;

namespace Telemetry.Ingestion;

/// <summary>
/// Subscribes to device telemetry through an MQTT shared subscription and pushes validated
/// readings into the bounded channel. The MQTT callback never touches the database.
/// </summary>
public sealed class MqttIngestionService(
    ChannelWriter<TelemetryReading> queue,
    IngestionMetrics metrics,
    IOptions<MqttSettings> settings,
    ILogger<MqttIngestionService> logger) : BackgroundService
{
    private readonly IMqttClient _client = new MqttFactory().CreateMqttClient();

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _client.ApplicationMessageReceivedAsync += e =>
        {
            metrics.OnReceived();
            if (!TelemetryParser.TryParse(e.ApplicationMessage.Topic, e.ApplicationMessage.PayloadSegment, out var reading))
                metrics.OnRejected();
            else if (!queue.TryWrite(reading))
                metrics.OnDropped(); // Buffer full: shed load instead of blocking the MQTT client.
            return Task.CompletedTask;
        };

        var options = new MqttClientOptionsBuilder()
            .WithTcpServer(settings.Value.Host, settings.Value.Port)
            .WithClientId($"ingestion-{Environment.MachineName}-{Guid.NewGuid():N}")
            .WithProtocolVersion(MqttProtocolVersion.V500)
            .WithCleanSession()
            .Build();

        while (!stoppingToken.IsCancellationRequested)
        {
            if (!_client.IsConnected)
            {
                try
                {
                    await _client.ConnectAsync(options, stoppingToken);
                    await _client.SubscribeAsync(
                        new MqttClientSubscribeOptionsBuilder().WithTopicFilter(TelemetryTopics.SharedIngestion).Build(),
                        stoppingToken);
                    logger.LogInformation("Connected to MQTT {Host}:{Port}, subscribed to {Topic}",
                        settings.Value.Host, settings.Value.Port, TelemetryTopics.SharedIngestion);
                }
                catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
                {
                    logger.LogWarning("MQTT connection failed: {Message}. Retrying in 3s", ex.Message);
                }
            }

            try
            {
                await Task.Delay(TimeSpan.FromSeconds(3), stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_client.IsConnected)
            await _client.DisconnectAsync(cancellationToken: cancellationToken);

        // Signal the writers that no more data is coming so they can drain and exit.
        queue.TryComplete();
        await base.StopAsync(cancellationToken);
    }

    public override void Dispose()
    {
        _client.Dispose();
        base.Dispose();
    }
}
