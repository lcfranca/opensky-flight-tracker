#!/usr/bin/env bash
set -euo pipefail

# Requires docker compose v2

echo "[1/3] Checking containers..."
docker compose ps

echo "[2/4] Verifying OTel tables exist in ClickHouse..."
docker compose exec -T clickhouse clickhouse-client --query \
  "SELECT name FROM system.tables WHERE database='aviation' AND name IN ('otel_logs','otel_traces') ORDER BY name"

echo "[3/4] Querying ClickHouse (row count last 15m)..."
docker compose exec -T clickhouse clickhouse-client --query \
  "SELECT count() AS rows_15m FROM aviation.flight_positions WHERE timestamp >= now() - INTERVAL 15 MINUTE"

echo "[4/4] Top countries (last 15m)..."
docker compose exec -T clickhouse clickhouse-client --query \
  "SELECT origin_country, uniq(icao24) AS flights FROM aviation.flight_positions WHERE timestamp >= now() - INTERVAL 15 MINUTE AND on_ground = 0 GROUP BY origin_country ORDER BY flights DESC LIMIT 10"

echo "[5/7] Sanity: future timestamps (should be 0)..."
docker compose exec -T clickhouse clickhouse-client --query \
  "SELECT countIf(timestamp > now()) AS future_rows FROM aviation.flight_positions"

echo "[6/7] Sanity: last 5m vs last 30m (5m must be <= 30m)..."
docker compose exec -T clickhouse clickhouse-client --query \
  "SELECT countIf(timestamp >= now() - INTERVAL 5 MINUTE) AS c5, countIf(timestamp >= now() - INTERVAL 30 MINUTE) AS c30 FROM aviation.flight_positions WHERE timestamp <= now()"

echo "[7/7] Reference tables exist (airlines/airports)..."
docker compose exec -T clickhouse clickhouse-client --query \
  "SELECT name, engine FROM system.tables WHERE database='aviation' AND name IN ('airlines','airports') ORDER BY name"
