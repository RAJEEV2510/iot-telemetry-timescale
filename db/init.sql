-- TimescaleDB schema for high-volume device telemetry.
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS telemetry (
    time        TIMESTAMPTZ      NOT NULL,
    device_id   TEXT             NOT NULL,
    temperature DOUBLE PRECISION NOT NULL,
    humidity    DOUBLE PRECISION NOT NULL,
    battery     DOUBLE PRECISION NOT NULL
);

-- Hypertable: data is transparently partitioned into 1-day chunks, so inserts and
-- time-bounded queries only touch the recent chunks instead of one huge table.
SELECT create_hypertable('telemetry', 'time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

-- Serves "latest reading per device" and per-device range queries.
CREATE INDEX IF NOT EXISTS ix_telemetry_device_time ON telemetry (device_id, time DESC);

-- Native compression: segment by device so each compressed batch holds one device's
-- ordered series, which compresses well and stays fast to filter by device.
ALTER TABLE telemetry SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'device_id',
    timescaledb.compress_orderby = 'time DESC'
);
SELECT add_compression_policy('telemetry', INTERVAL '2 days', if_not_exists => TRUE);

-- Raw data retention: old chunks are dropped whole (no expensive DELETEs).
SELECT add_retention_policy('telemetry', INTERVAL '30 days', if_not_exists => TRUE);

-- Continuous aggregate: 1-minute rollups maintained incrementally in the background.
-- materialized_only = false merges in not-yet-materialized recent data (real-time aggregation).
CREATE MATERIALIZED VIEW IF NOT EXISTS telemetry_1m
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT
    time_bucket(INTERVAL '1 minute', time) AS bucket,
    device_id,
    avg(temperature) AS avg_temperature,
    max(temperature) AS max_temperature,
    avg(humidity)    AS avg_humidity,
    min(battery)     AS min_battery,
    count(*)         AS readings
FROM telemetry
GROUP BY bucket, device_id
WITH NO DATA;

SELECT add_continuous_aggregate_policy('telemetry_1m',
    start_offset      => INTERVAL '1 hour',
    end_offset        => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute',
    if_not_exists     => TRUE);

-- Rollups are kept far longer than raw data.
SELECT add_retention_policy('telemetry_1m', INTERVAL '365 days', if_not_exists => TRUE);
