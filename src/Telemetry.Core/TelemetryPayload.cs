using System.Text.Json.Serialization;

namespace Telemetry.Core;

/// <summary>Wire format: {"ts": unixMillis, "temperature": 21.5, "humidity": 40.2, "battery": 97.1}</summary>
public sealed record TelemetryPayload(
    [property: JsonPropertyName("ts")] long Ts,
    [property: JsonPropertyName("temperature")] double Temperature,
    [property: JsonPropertyName("humidity")] double Humidity,
    [property: JsonPropertyName("battery")] double Battery);

// Source-generated serializer: no reflection on the hot ingestion path.
[JsonSerializable(typeof(TelemetryPayload))]
internal sealed partial class TelemetryJsonContext : JsonSerializerContext;
