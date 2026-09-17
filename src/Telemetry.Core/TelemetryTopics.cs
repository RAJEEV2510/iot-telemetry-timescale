namespace Telemetry.Core;

public static class TelemetryTopics
{
    /// <summary>Topic filter matching telemetry from every device.</summary>
    public const string AllDevices = "devices/+/telemetry";

    /// <summary>
    /// Shared subscription: MQTT brokers load-balance messages across every subscriber in the
    /// group, so ingestion can be scaled horizontally by running more instances.
    /// </summary>
    public const string SharedIngestion = "$share/ingestion/devices/+/telemetry";

    public static string ForDevice(string deviceId) => $"devices/{deviceId}/telemetry";

    /// <summary>Extracts the device id from "devices/{id}/telemetry".</summary>
    public static bool TryGetDeviceId(string topic, out string deviceId)
    {
        deviceId = string.Empty;
        var parts = topic.Split('/');
        if (parts.Length != 3 || parts[0] != "devices" || parts[2] != "telemetry" || parts[1].Length == 0)
            return false;

        deviceId = parts[1];
        return true;
    }
}
