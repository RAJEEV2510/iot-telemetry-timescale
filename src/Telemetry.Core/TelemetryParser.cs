using System.Text.Json;

namespace Telemetry.Core;

/// <summary>
/// Parses and validates device payloads. Invalid messages are rejected rather than stored,
/// so a single faulty sensor cannot pollute the time-series data.
/// </summary>
public static class TelemetryParser
{
    public static bool TryParse(string topic, ReadOnlySpan<byte> payload, out TelemetryReading reading)
    {
        reading = default;
        if (!TelemetryTopics.TryGetDeviceId(topic, out var deviceId))
            return false;

        TelemetryPayload? body;
        try
        {
            body = JsonSerializer.Deserialize(payload, TelemetryJsonContext.Default.TelemetryPayload);
        }
        catch (JsonException)
        {
            return false;
        }

        if (body is null || body.Ts <= 0)
            return false;
        if (!IsInRange(body.Temperature, -50, 150) || !IsInRange(body.Humidity, 0, 100) || !IsInRange(body.Battery, 0, 100))
            return false;

        reading = new TelemetryReading(
            deviceId,
            DateTimeOffset.FromUnixTimeMilliseconds(body.Ts),
            body.Temperature,
            body.Humidity,
            body.Battery);
        return true;
    }

    public static byte[] Serialize(TelemetryReading reading) =>
        JsonSerializer.SerializeToUtf8Bytes(
            new TelemetryPayload(reading.Time.ToUnixTimeMilliseconds(), reading.Temperature, reading.Humidity, reading.Battery),
            TelemetryJsonContext.Default.TelemetryPayload);

    private static bool IsInRange(double value, double min, double max) =>
        double.IsFinite(value) && value >= min && value <= max;
}
