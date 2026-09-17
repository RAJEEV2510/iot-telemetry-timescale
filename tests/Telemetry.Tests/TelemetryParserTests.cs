using System.Text;
using Telemetry.Core;

namespace Telemetry.Tests;

public class TelemetryParserTests
{
    private static byte[] Json(string json) => Encoding.UTF8.GetBytes(json);

    [Fact]
    public void Parses_valid_payload()
    {
        var ok = TelemetryParser.TryParse(
            "devices/sensor-0001/telemetry",
            Json("""{"ts":1700000000000,"temperature":21.5,"humidity":40.2,"battery":97.1}"""),
            out var reading);

        Assert.True(ok);
        Assert.Equal("sensor-0001", reading.DeviceId);
        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(1700000000000), reading.Time);
        Assert.Equal(21.5, reading.Temperature);
        Assert.Equal(40.2, reading.Humidity);
        Assert.Equal(97.1, reading.Battery);
    }

    [Fact]
    public void Round_trips_through_serialize()
    {
        var original = new TelemetryReading("sensor-7", DateTimeOffset.FromUnixTimeMilliseconds(1700000000123), 25.25, 55.5, 80);

        Assert.True(TelemetryParser.TryParse(TelemetryTopics.ForDevice("sensor-7"), TelemetryParser.Serialize(original), out var parsed));
        Assert.Equal(original, parsed);
    }

    [Theory]
    [InlineData("devices/sensor-1/status")]
    [InlineData("devices//telemetry")]
    [InlineData("sensors/sensor-1/telemetry")]
    [InlineData("devices/sensor-1/telemetry/extra")]
    public void Rejects_unexpected_topics(string topic)
    {
        Assert.False(TelemetryParser.TryParse(topic, Json("""{"ts":1,"temperature":20,"humidity":50,"battery":90}"""), out _));
    }

    [Theory]
    [InlineData("not json")]
    [InlineData("{}")]
    [InlineData("""{"ts":0,"temperature":20,"humidity":50,"battery":90}""")]
    [InlineData("""{"ts":1,"temperature":999,"humidity":50,"battery":90}""")]
    [InlineData("""{"ts":1,"temperature":20,"humidity":-1,"battery":90}""")]
    [InlineData("""{"ts":1,"temperature":20,"humidity":50,"battery":101}""")]
    public void Rejects_invalid_payloads(string payload)
    {
        Assert.False(TelemetryParser.TryParse("devices/sensor-1/telemetry", Json(payload), out _));
    }

    [Fact]
    public void Shared_subscription_targets_same_topics_as_direct_subscription()
    {
        Assert.EndsWith(TelemetryTopics.AllDevices, TelemetryTopics.SharedIngestion);
        Assert.StartsWith("$share/", TelemetryTopics.SharedIngestion);
    }
}
