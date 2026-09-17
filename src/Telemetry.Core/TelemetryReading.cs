namespace Telemetry.Core;

/// <summary>A single sensor reading published by a device.</summary>
public readonly record struct TelemetryReading(
    string DeviceId,
    DateTimeOffset Time,
    double Temperature,
    double Humidity,
    double Battery);
