#!/usr/bin/env bash
set -euo pipefail

CLICKHOUSE_HOST="${CLICKHOUSE_HOST:-clickhouse}"
CLICKHOUSE_PORT="${CLICKHOUSE_PORT:-9000}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-admin}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-admin123}"
CLICKHOUSE_DATABASE="${CLICKHOUSE_DATABASE:-aviation}"

ch() {
  clickhouse-client \
    --host "$CLICKHOUSE_HOST" \
    --port "$CLICKHOUSE_PORT" \
    --user "$CLICKHOUSE_USER" \
    --password "$CLICKHOUSE_PASSWORD" \
    --multiquery \
    --query "$1"
}

wait_for_otel_logs() {
  echo "Waiting for ${CLICKHOUSE_DATABASE}.otel_logs to exist (created by otel-collector clickhouseexporter)..."
  for i in $(seq 1 60); do
    if clickhouse-client \
      --host "$CLICKHOUSE_HOST" \
      --port "$CLICKHOUSE_PORT" \
      --user "$CLICKHOUSE_USER" \
      --password "$CLICKHOUSE_PASSWORD" \
      --query "SELECT count() FROM system.tables WHERE database='${CLICKHOUSE_DATABASE}' AND name='otel_logs'" \
      | grep -q '^1$'; then
      echo "otel_logs found."
      return 0
    fi
    sleep 2
  done

  echo "Timed out waiting for otel_logs." >&2
  return 1
}

main() {
  wait_for_otel_logs

  # Create aviation analytics tables/views that depend on otel_logs.
  clickhouse-client \
    --host "$CLICKHOUSE_HOST" \
    --port "$CLICKHOUSE_PORT" \
    --user "$CLICKHOUSE_USER" \
    --password "$CLICKHOUSE_PASSWORD" \
    --multiquery <<SQL
CREATE DATABASE IF NOT EXISTS ${CLICKHOUSE_DATABASE};

USE ${CLICKHOUSE_DATABASE};

-- Reference tables (optional but useful for enrichment + avoids client errors if queried)
CREATE TABLE IF NOT EXISTS airlines
(
  icao LowCardinality(String),
  iata LowCardinality(String),
  name String,
  country LowCardinality(String)
)
ENGINE = MergeTree()
ORDER BY icao;

CREATE TABLE IF NOT EXISTS airports
(
  icao LowCardinality(String),
  iata LowCardinality(String),
  name String,
  city String,
  country LowCardinality(String),
  latitude Float64,
  longitude Float64
)
ENGINE = MergeTree()
ORDER BY icao;

CREATE TABLE IF NOT EXISTS flight_positions
(
    timestamp DateTime64(3) CODEC(Delta, ZSTD),
    last_contact DateTime64(3) CODEC(Delta, ZSTD),

    icao24 String CODEC(ZSTD),
    callsign String CODEC(ZSTD),

    longitude Float64 CODEC(Gorilla, ZSTD),
    latitude Float64 CODEC(Gorilla, ZSTD),

    baro_altitude Float32 CODEC(Gorilla, ZSTD),
    geo_altitude Float32 CODEC(Gorilla, ZSTD),

    velocity Float32 CODEC(Gorilla, ZSTD),
    true_track Float32 CODEC(Gorilla, ZSTD),
    vertical_rate Float32 CODEC(Gorilla, ZSTD),

    on_ground UInt8 CODEC(ZSTD),
    spi UInt8 CODEC(ZSTD),

    origin_country LowCardinality(String) CODEC(ZSTD),
    position_source LowCardinality(Nullable(String)) CODEC(ZSTD),
    category LowCardinality(Nullable(String)) CODEC(ZSTD),

    altitude_ft Float32 MATERIALIZED baro_altitude * 3.28084,
    speed_kmh Float32 MATERIALIZED velocity * 3.6,
    speed_knots Float32 MATERIALIZED velocity * 1.94384,

    geohash String MATERIALIZED geohashEncode(longitude, latitude, 6),

    date Date MATERIALIZED toDate(timestamp),
    hour UInt8 MATERIALIZED toHour(timestamp),

    INDEX idx_icao24 icao24 TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_callsign callsign TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_country origin_country TYPE set(0) GRANULARITY 4,
    INDEX idx_geohash geohash TYPE bloom_filter(0.01) GRANULARITY 4
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (icao24, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;

-- Materialize OpenSky JSON (stored in otel_logs.Body) into flight_positions.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_flight_positions_from_otel_logs
TO flight_positions
AS
SELECT
    toDateTime64(JSONExtractFloat(Body, 'timestamp') / 1000.0, 3) AS timestamp,
    toDateTime64(JSONExtractFloat(Body, 'last_contact') / 1000.0, 3) AS last_contact,

    JSONExtractString(Body, 'icao24') AS icao24,
    JSONExtractString(Body, 'callsign') AS callsign,

    JSONExtractFloat(Body, 'longitude') AS longitude,
    JSONExtractFloat(Body, 'latitude') AS latitude,

    toFloat32(JSONExtractFloat(Body, 'baro_altitude')) AS baro_altitude,
    toFloat32(JSONExtractFloat(Body, 'geo_altitude')) AS geo_altitude,

    toFloat32(JSONExtractFloat(Body, 'velocity')) AS velocity,
    toFloat32(JSONExtractFloat(Body, 'true_track')) AS true_track,
    toFloat32(JSONExtractFloat(Body, 'vertical_rate')) AS vertical_rate,

    toUInt8(JSONExtractInt(Body, 'on_ground')) AS on_ground,
    toUInt8(JSONExtractInt(Body, 'spi')) AS spi,

    ifNull(nullIf(JSONExtractString(Body, 'origin_country'), ''), 'Unknown') AS origin_country,
    nullIf(JSONExtractString(Body, 'position_source'), '') AS position_source,
    nullIf(JSONExtractString(Body, 'category'), '') AS category
FROM otel_logs
WHERE
    JSONHas(Body, 'timestamp')
    AND JSONHas(Body, 'last_contact')
    AND JSONHas(Body, 'icao24')
    AND JSONHas(Body, 'longitude')
    AND JSONHas(Body, 'latitude');

CREATE MATERIALIZED VIEW IF NOT EXISTS flights_hourly_by_country
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (origin_country, hour)
AS
SELECT
    toStartOfHour(timestamp) AS hour,
    origin_country,
    count() AS total_positions,
    uniq(icao24) AS unique_flights,
    avg(altitude_ft) AS avg_altitude_ft,
    avg(speed_kmh) AS avg_speed_kmh,
    countIf(on_ground = 1) AS grounded_count,
    countIf(on_ground = 0) AS airborne_count
FROM flight_positions
GROUP BY hour, origin_country;

CREATE MATERIALIZED VIEW IF NOT EXISTS active_flights
ENGINE = ReplacingMergeTree(timestamp)
ORDER BY icao24
AS
SELECT
    timestamp,
    icao24,
    callsign,
    origin_country,
    longitude,
    latitude,
    altitude_ft,
    speed_kmh,
    true_track,
    on_ground,
    last_contact
FROM flight_positions;
SQL

  echo "ClickHouse views/tables ensured."
}

main "$@"
