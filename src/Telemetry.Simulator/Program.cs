using System.Diagnostics;
using Microsoft.Extensions.Configuration;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Protocol;
using Telemetry.Core;

// Simulates a fleet of sensors publishing telemetry over MQTT.
// Configure with env vars, e.g. Simulator__Devices=500 Simulator__RatePerDevice=10 Mqtt__Host=localhost
var config = new ConfigurationBuilder()
    .AddEnvironmentVariables()
    .AddCommandLine(args)
    .Build();

var host = config["Mqtt:Host"] ?? "localhost";
var port = config.GetValue("Mqtt:Port", 1883);
var deviceCount = config.GetValue("Simulator:Devices", 200);
var ratePerDevice = config.GetValue("Simulator:RatePerDevice", 5.0);

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };
AppDomain.CurrentDomain.ProcessExit += (_, _) => cts.Cancel();

var devices = Enumerable.Range(1, deviceCount).Select(i => new SimulatedDevice($"sensor-{i:D4}")).ToArray();
var client = new MqttFactory().CreateMqttClient();
var options = new MqttClientOptionsBuilder()
    .WithTcpServer(host, port)
    .WithClientId($"simulator-{Guid.NewGuid():N}")
    .WithCleanSession()
    .Build();

const int ticksPerSecond = 10;
var messagesPerTick = deviceCount * ratePerDevice / ticksPerSecond;
Console.WriteLine($"Simulating {deviceCount} devices at {ratePerDevice}/s each (~{deviceCount * ratePerDevice:F0} msg/s) -> {host}:{port}");

using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(1000 / ticksPerSecond));
var carry = 0.0;
var next = 0;
long sent = 0;
var reportClock = Stopwatch.StartNew();

try
{
    while (await timer.WaitForNextTickAsync(cts.Token))
    {
        if (!client.IsConnected)
        {
            try
            {
                await client.ConnectAsync(options, cts.Token);
                Console.WriteLine("Connected to MQTT broker");
            }
            catch (Exception ex) when (!cts.IsCancellationRequested)
            {
                Console.WriteLine($"MQTT connection failed: {ex.Message}");
                await Task.Delay(TimeSpan.FromSeconds(2), cts.Token);
                continue;
            }
        }

        carry += messagesPerTick;
        var toSend = (int)carry;
        carry -= toSend;

        for (var i = 0; i < toSend; i++)
        {
            var device = devices[next];
            next = (next + 1) % devices.Length;

            var message = new MqttApplicationMessageBuilder()
                .WithTopic(TelemetryTopics.ForDevice(device.Id))
                .WithPayload(TelemetryParser.Serialize(device.NextReading()))
                .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtMostOnce)
                .Build();

            await client.PublishAsync(message, cts.Token);
            sent++;
        }

        if (reportClock.Elapsed >= TimeSpan.FromSeconds(10))
        {
            Console.WriteLine($"{DateTime.UtcNow:HH:mm:ss} sent={sent} rate={sent / reportClock.Elapsed.TotalSeconds:F0}/s");
            sent = 0;
            reportClock.Restart();
        }
    }
}
catch (OperationCanceledException)
{
}
finally
{
    if (client.IsConnected)
        await client.DisconnectAsync();
    client.Dispose();
}

/// <summary>Random-walk sensor so charts look like real data rather than noise.</summary>
internal sealed class SimulatedDevice(string id)
{
    private double _temperature = 20 + Random.Shared.NextDouble() * 10;
    private double _humidity = 35 + Random.Shared.NextDouble() * 30;
    private double _battery = 60 + Random.Shared.NextDouble() * 40;

    public string Id { get; } = id;

    public TelemetryReading NextReading()
    {
        _temperature = Math.Clamp(_temperature + (Random.Shared.NextDouble() - 0.5) * 0.4, -20, 60);
        _humidity = Math.Clamp(_humidity + (Random.Shared.NextDouble() - 0.5) * 0.8, 5, 95);
        _battery = _battery <= 5 ? 100 : _battery - Random.Shared.NextDouble() * 0.01;
        return new TelemetryReading(Id, DateTimeOffset.UtcNow, _temperature, _humidity, _battery);
    }
}
