-- =============================================
-- OpenSky Flight Tracker - ClickHouse schema
-- OTel Collector -> ClickHouse (otel_logs) -> MV -> MergeTree (final)
-- =============================================

CREATE DATABASE IF NOT EXISTS aviation;

USE aviation;

-- Note:
-- Data tables/views for the pipeline (otel_logs -> flight_positions) are created
-- by the runtime bootstrap script:
--   scripts/bootstrap_clickhouse_views.sh
