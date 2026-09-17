namespace Telemetry.Api;

public sealed record DeviceReading(string DeviceId, DateTime Time, double Temperature, double Humidity, double Battery);

public sealed record MinuteAggregate(
    DateTime Bucket, double AvgTemperature, double MaxTemperature, double AvgHumidity, double MinBattery, long Readings);

public sealed record StorageStats(
    long ApproximateRows, long ReadingsLastMinute, long ActiveDevices, long HypertableBytes, long Chunks, long CompressedChunks);

internal static class Queries
{
    public const string LatestPerDevice = """
        SELECT DISTINCT ON (device_id)
               device_id AS DeviceId, time AS Time, temperature AS Temperature,
               humidity AS Humidity, battery AS Battery
        FROM telemetry
        WHERE time > now() - INTERVAL '10 minutes'
        ORDER BY device_id, time DESC
        """;

    public const string RawReadings = """
        SELECT device_id AS DeviceId, time AS Time, temperature AS Temperature,
               humidity AS Humidity, battery AS Battery
        FROM telemetry
        WHERE device_id = @deviceId AND time > now() - make_interval(mins => @minutes)
        ORDER BY time DESC
        LIMIT 2000
        """;

    public const string MinuteAggregates = """
        SELECT bucket AS Bucket, avg_temperature AS AvgTemperature, max_temperature AS MaxTemperature,
               avg_humidity AS AvgHumidity, min_battery AS MinBattery, readings AS Readings
        FROM telemetry_1m
        WHERE device_id = @deviceId AND bucket > now() - make_interval(hours => @hours)
        ORDER BY bucket
        """;

    public const string Stats = """
        SELECT
            approximate_row_count('telemetry') AS ApproximateRows,
            (SELECT count(*) FROM telemetry WHERE time > now() - INTERVAL '1 minute') AS ReadingsLastMinute,
            (SELECT count(DISTINCT device_id) FROM telemetry WHERE time > now() - INTERVAL '1 minute') AS ActiveDevices,
            COALESCE(hypertable_size('telemetry'), 0) AS HypertableBytes,
            (SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name = 'telemetry') AS Chunks,
            (SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name = 'telemetry' AND is_compressed) AS CompressedChunks
        """;
}
