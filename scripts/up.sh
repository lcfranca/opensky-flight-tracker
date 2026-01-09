#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing required command: $1" >&2
    exit 1
  }
}

require_cmd docker
require_cmd ss

compose_ports() {
  # Extract published ports from docker compose config output.
  docker compose config 2>/dev/null | awk '/published:/{print $2}' | sed 's/"//g' | sort -n | uniq
}

stop_containers_using_port() {
  local port="$1"
  local ids
  ids=$(docker ps --format '{{.ID}} {{.Ports}}' | awk -v p=":${port}->" '$0 ~ p {print $1}')
  if [[ -n "$ids" ]]; then
    echo "Stopping Docker containers using port ${port}: ${ids}" >&2
    docker stop $ids >/dev/null
  fi
}

assert_ports_free() {
  local ports
  ports=$(compose_ports)
  if [[ -z "$ports" ]]; then
    echo "Could not infer published ports from docker compose config." >&2
    exit 1
  fi

  while read -r port; do
    [[ -z "$port" ]] && continue

    # If port is in use, try to stop docker containers using it.
    if ss -ltn "( sport = :${port} )" | grep -q LISTEN; then
      stop_containers_using_port "$port"
    fi

    # If still in use, error out (unless FORCE_KILL=1).
    if ss -ltn "( sport = :${port} )" | grep -q LISTEN; then
      if [[ "${FORCE_KILL:-0}" == "1" ]]; then
        local pids
        pids=$(ss -ltnp "( sport = :${port} )" | awk 'NR>1{print $NF}' | sed -E 's/.*pid=([0-9]+).*/\1/' | grep -E '^[0-9]+$' | sort -u || true)
        if [[ -n "$pids" ]]; then
          echo "Killing processes on port ${port}: ${pids}" >&2
          kill -9 $pids || true
        fi
      else
        echo "Port ${port} is already in use. Set FORCE_KILL=1 to kill listener processes." >&2
        ss -ltnp "( sport = :${port} )" || true
        exit 1
      fi
    fi
  done <<< "$ports"
}

wait_http() {
  local url="$1"
  local name="$2"
  echo "Waiting for ${name} at ${url}..." >&2
  for i in $(seq 1 60); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  echo "Timed out waiting for ${name}." >&2
  return 1
}

main() {
  # Optional flags:
  #   --replay    : replay previously captured OpenSky items (real data) at a controlled rate
  #   --reset     : remove volumes (wipes ClickHouse/Grafana data)
  local reset="0"
  local mode_override=""
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --replay)
        mode_override="replay"
        shift
        ;;
      --opensky)
        mode_override="opensky"
        shift
        ;;
      --reset)
        reset="1"
        shift
        ;;
      *)
        echo "Unknown option: $1" >&2
        exit 2
        ;;
    esac
  done

  if [[ -n "$mode_override" ]]; then
    export PRODUCER_MODE="$mode_override"
    echo "Producer mode override: ${PRODUCER_MODE}" >&2
  fi

  echo "[1/6] Bringing down any existing stack..." >&2
  if [[ "$reset" == "1" ]]; then
    docker compose down -v --remove-orphans >/dev/null 2>&1 || true
  else
    docker compose down --remove-orphans >/dev/null 2>&1 || true
  fi

  echo "[2/6] Checking for port conflicts..." >&2
  assert_ports_free

  echo "[3/6] Starting stack..." >&2
  docker compose up -d --build

  echo "[4/6] Bootstrapping ClickHouse views (otel_logs -> flight_positions)..." >&2
  docker compose up -d clickhouse-bootstrap

  echo "[5/6] Waiting for Grafana..." >&2
  wait_http "http://localhost:3000/login" "Grafana"

  echo "[6/6] Ready." >&2
  echo "Grafana: http://localhost:3000 (user: admin, pass: GF_SECURITY_ADMIN_PASSWORD from .env)"
  echo "Kafka UI: http://localhost:8090"
}

main "$@"
