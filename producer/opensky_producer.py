#!/usr/bin/env python3
"""OpenSky Network Flight Tracker - Kafka Producer.

- Polls OpenSky `states/all` endpoint.
- Transforms to a JSON schema compatible with ClickHouse Kafka Engine (JSONEachRow).
- Applies simple delta filtering to reduce duplicate inserts.

Notes:
- OpenSky free limits can be very low (e.g., 100 req/day anonymous). Use creds and/or lower frequency.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import random
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

try:
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    _OTEL_AVAILABLE = True
except Exception:
    _OTEL_AVAILABLE = False


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("opensky-producer")


class OpenSkyRateLimitedError(RuntimeError):
    def __init__(self, retry_after_seconds: int):
        super().__init__(f"OpenSky rate-limited (429); retry_after_seconds={retry_after_seconds}")
        self.retry_after_seconds = retry_after_seconds


OPENSKY_API_URL = "https://opensky-network.org/api/states/all"

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "flight-positions")
KAFKA_COMPRESSION = os.getenv("KAFKA_COMPRESSION", "gzip").strip() or "gzip"

PRODUCER_MODE = os.getenv("PRODUCER_MODE", "opensky").strip().lower() or "opensky"

# Controlled ingestion (real OpenSky data): optionally pace publishing to Kafka to
# produce a near-real-time stream instead of bursty inserts.
PUBLISH_RATE_PER_SECOND = int(os.getenv("PUBLISH_RATE_PER_SECOND", "0") or "0")

# Optional capture/replay of REAL OpenSky data (useful when OpenSky rate-limits).
# Capture writes the exact produced JSON items (one per line) to disk.
CAPTURE_ENABLED = os.getenv("CAPTURE_ENABLED", "0").strip() in {"1", "true", "TRUE", "yes", "YES"}
CAPTURE_DIR = os.getenv("CAPTURE_DIR", "/data/opensky_capture").strip() or "/data/opensky_capture"

# Replay publishes previously captured items at a controlled rate, stamping fresh
# timestamps so Grafana shows a live stream. Data content remains real.
REPLAY_DIR = os.getenv("REPLAY_DIR", CAPTURE_DIR).strip() or CAPTURE_DIR
REPLAY_MESSAGES_PER_SECOND = int(os.getenv("REPLAY_MESSAGES_PER_SECOND", "200") or "200")
REPLAY_LOOP = os.getenv("REPLAY_LOOP", "1").strip() in {"1", "true", "TRUE", "yes", "YES"}

# Dedup control (fingerprint-based). Keep enabled by default for real OpenSky.
DISABLE_DEDUP = os.getenv("DISABLE_DEDUP", "0").strip() in {"1", "true", "TRUE", "yes", "YES"}

OPENSKY_USERNAME = os.getenv("OPENSKY_USERNAME", "").strip()
OPENSKY_PASSWORD = os.getenv("OPENSKY_PASSWORD", "").strip()

# OAuth2 client credentials (required for accounts created since mid-March 2025)
OPENSKY_CLIENT_ID = os.getenv("OPENSKY_CLIENT_ID", "").strip()
OPENSKY_CLIENT_SECRET = os.getenv("OPENSKY_CLIENT_SECRET", "").strip()
OPENSKY_TOKEN_URL = (
    os.getenv(
        "OPENSKY_TOKEN_URL",
        "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token",
    ).strip()
    or "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
)

POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "10"))
REQUEST_TIMEOUT_SECONDS = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "30"))

BUDGET_STATE_PATH = os.getenv("BUDGET_STATE_PATH", "/data/opensky_budget.json").strip() or "/data/opensky_budget.json"

# Sliding-window budget for OpenSky calls (24h). Allows fast polling during short sessions,
# without exceeding free-tier daily limits when running for long periods.
MAX_REQUESTS_PER_24H = int(os.getenv("MAX_REQUESTS_PER_24H", "0") or "0")
if MAX_REQUESTS_PER_24H <= 0:
    # Heuristic defaults (can be overridden via env).
    MAX_REQUESTS_PER_24H = 400 if (OPENSKY_USERNAME and OPENSKY_PASSWORD) else 100

# Optional bounding box query params (min_lat,max_lat,min_lon,max_lon)
BBOX_MIN_LAT = os.getenv("BBOX_MIN_LAT")
BBOX_MAX_LAT = os.getenv("BBOX_MAX_LAT")
BBOX_MIN_LON = os.getenv("BBOX_MIN_LON")
BBOX_MAX_LON = os.getenv("BBOX_MAX_LON")


stats: Dict[str, Any] = {
    "start_time": time.time(),
    "api_calls": 0,
    "errors": 0,
    "sent": 0,
    "dedup_dropped": 0,
}

# in-memory delta cache
_last_fp_by_icao24: Dict[str, str] = {}


def _capture_write_items(items: List[Dict[str, Any]], batch_time_ms: int) -> None:
    if not CAPTURE_ENABLED or not items:
        return

    try:
        out_dir = Path(CAPTURE_DIR)
        out_dir.mkdir(parents=True, exist_ok=True)
        tmp_path = out_dir / f"{batch_time_ms}.jsonl.tmp"
        final_path = out_dir / f"{batch_time_ms}.jsonl"

        with tmp_path.open("w", encoding="utf-8") as f:
            for item in items:
                f.write(json.dumps(item, ensure_ascii=False))
                f.write("\n")
        tmp_path.replace(final_path)
    except Exception as e:
        logger.warning("Failed to capture OpenSky batch: %s", e)


def _iter_capture_items(capture_dir: str) -> Any:
    base = Path(capture_dir)
    if not base.exists():
        return

    for p in sorted(base.glob("*.jsonl")):
        try:
            with p.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        yield json.loads(line)
                    except Exception:
                        continue
        except Exception:
            continue


def _send_items_paced(
    producer: KafkaProducer,
    items: List[Dict[str, Any]],
    rate_per_second: int,
    run_once: bool,
) -> None:
    if not items:
        return

    if rate_per_second <= 0:
        sent = 0
        for item in items:
            if DISABLE_DEDUP or should_emit(item):
                key = item.get("icao24")
                producer.send(KAFKA_TOPIC, key=key, value=item)
                sent += 1
        producer.flush(timeout=10)
        stats["sent"] += sent
        return

    interval_s = 1.0 / float(rate_per_second)
    next_send = time.time()
    sent = 0
    for item in items:
        if DISABLE_DEDUP or should_emit(item):
            key = item.get("icao24")
            producer.send(KAFKA_TOPIC, key=key, value=item)
            sent += 1

        next_send += interval_s
        sleep_s = next_send - time.time()
        if sleep_s > 0:
            time.sleep(sleep_s)

    producer.flush(timeout=10)
    stats["sent"] += sent

    if run_once:
        return


def run_replay(producer: KafkaProducer, run_once: bool) -> None:
    if REPLAY_MESSAGES_PER_SECOND <= 0:
        raise RuntimeError("Replay mode requires REPLAY_MESSAGES_PER_SECOND>0")

    logger.info(
        "Replay mode enabled; dir=%s messages_per_second=%s loop=%s",
        REPLAY_DIR,
        REPLAY_MESSAGES_PER_SECOND,
        REPLAY_LOOP,
    )

    interval_s = 1.0 / float(REPLAY_MESSAGES_PER_SECOND)
    rng = random.Random(1337)

    while True:
        any_sent = 0
        next_send = time.time()

        for item in _iter_capture_items(REPLAY_DIR) or []:
            now_ms = int(time.time() * 1000)
            item["timestamp"] = now_ms
            item["last_contact"] = now_ms
            item["position_source"] = "REPLAY"

            if DISABLE_DEDUP or should_emit(item):
                key = item.get("icao24")
                producer.send(KAFKA_TOPIC, key=key, value=item)
                any_sent += 1

            next_send += interval_s
            sleep_s = next_send - time.time()
            if sleep_s > 0:
                time.sleep(sleep_s)
            else:
                # Small jitter to prevent a tight loop when system is overloaded.
                time.sleep(min(0.01 + rng.random() * 0.02, 0.05))

        producer.flush(timeout=10)
        stats["sent"] += any_sent
        uptime = int(time.time() - stats["start_time"])
        logger.info("replay uptime=%ss sent=%s total_sent=%s", uptime, any_sent, stats["sent"])

        if run_once:
            logger.info("RUN_ONCE enabled; exiting after one replay pass.")
            return

        if not REPLAY_LOOP:
            logger.warning("Replay completed; REPLAY_LOOP=0, exiting.")
            return

        logger.info("Replay completed; looping in 2s...")
        time.sleep(2)


def _setup_otel() -> Optional[trace.Tracer]:
    if not _OTEL_AVAILABLE:
        return None

    endpoint = os.getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "").strip() or os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "").strip()
    if not endpoint:
        return None

    # OTLP HTTP trace exporter expects a full /v1/traces endpoint.
    if endpoint.startswith("http") and not endpoint.rstrip("/").endswith("/v1/traces"):
        endpoint = endpoint.rstrip("/") + "/v1/traces"

    service_name = os.getenv("OTEL_SERVICE_NAME", "opensky-producer")
    resource = Resource.create({"service.name": service_name})

    provider = TracerProvider(resource=resource)
    processor = BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint))
    provider.add_span_processor(processor)

    trace.set_tracer_provider(provider)
    return trace.get_tracer("opensky-producer")


TRACER = _setup_otel()


def _auth_tuple() -> Optional[Tuple[str, str]]:
    if OPENSKY_USERNAME and OPENSKY_PASSWORD:
        return (OPENSKY_USERNAME, OPENSKY_PASSWORD)
    return None


_oauth_token_cache: Dict[str, Any] = {"token": None, "expires_at": 0.0}


def _get_oauth_token(force_refresh: bool = False) -> Optional[str]:
    if not (OPENSKY_CLIENT_ID and OPENSKY_CLIENT_SECRET):
        return None

    now = time.time()
    token = _oauth_token_cache.get("token")
    expires_at = float(_oauth_token_cache.get("expires_at") or 0.0)

    if token and not force_refresh and now < expires_at:
        return str(token)

    try:
        r = requests.post(
            OPENSKY_TOKEN_URL,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={
                "grant_type": "client_credentials",
                "client_id": OPENSKY_CLIENT_ID,
                "client_secret": OPENSKY_CLIENT_SECRET,
            },
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        r.raise_for_status()
        data = r.json()
        access_token = data.get("access_token")
        expires_in = int(data.get("expires_in") or 1800)
        if not access_token:
            raise RuntimeError("No access_token in token response")

        # Refresh a bit early to avoid edge-expiry 401s.
        _oauth_token_cache["token"] = access_token
        _oauth_token_cache["expires_at"] = time.time() + max(60, expires_in - 30)
        return str(access_token)
    except Exception as e:
        logger.error("Failed to obtain OpenSky OAuth token: %s", e)
        return None


def create_kafka_producer() -> KafkaProducer:
    last_err: Optional[Exception] = None
    for attempt in range(1, 11):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[s.strip() for s in KAFKA_BOOTSTRAP_SERVERS.split(",") if s.strip()],
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                compression_type=KAFKA_COMPRESSION,
                linger_ms=20,
                batch_size=32768,
                acks="all",
                retries=5,
                request_timeout_ms=30000,
            )
            logger.info("Connected to Kafka: %s", KAFKA_BOOTSTRAP_SERVERS)
            return producer
        except Exception as e:
            last_err = e
            logger.warning("Kafka connection failed (attempt %s/10): %s", attempt, e)
            time.sleep(min(2 * attempt, 15))

    raise RuntimeError(f"Failed to create Kafka producer: {last_err}")


def fetch_opensky() -> Optional[Dict[str, Any]]:
    params: Dict[str, Any] = {}
    if all(v is not None and v != "" for v in (BBOX_MIN_LAT, BBOX_MAX_LAT, BBOX_MIN_LON, BBOX_MAX_LON)):
        params.update(
            {
                "lamin": BBOX_MIN_LAT,
                "lamax": BBOX_MAX_LAT,
                "lomin": BBOX_MIN_LON,
                "lomax": BBOX_MAX_LON,
            }
        )

    try:
        _budget_wait_if_needed()

        # Count all outbound requests against the budget.
        _budget_record_call()

        auth = _auth_tuple()
        token = _get_oauth_token(force_refresh=False)

        headers: Dict[str, str] = {}
        request_auth = None
        if token:
            headers["Authorization"] = f"Bearer {token}"
        elif auth:
            request_auth = auth

        r = requests.get(
            OPENSKY_API_URL,
            headers=headers or None,
            auth=request_auth,
            params=params,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )

        # If we used a Bearer token and got 401, refresh token once and retry.
        if r.status_code == 401 and token:
            logger.warning("OpenSky returned 401 with Bearer token; refreshing token and retrying once.")
            token2 = _get_oauth_token(force_refresh=True)
            if token2:
                _budget_record_call()
                r = requests.get(
                    OPENSKY_API_URL,
                    headers={"Authorization": f"Bearer {token2}"},
                    params=params,
                    timeout=REQUEST_TIMEOUT_SECONDS,
                )

        # If Basic Auth returned 401 but OAuth client credentials exist, switch to OAuth.
        if r.status_code == 401 and auth and not token and (OPENSKY_CLIENT_ID and OPENSKY_CLIENT_SECRET):
            logger.warning("OpenSky returned 401 with Basic Auth; trying OAuth2 client credentials.")
            token2 = _get_oauth_token(force_refresh=True)
            if token2:
                _budget_record_call()
                r = requests.get(
                    OPENSKY_API_URL,
                    headers={"Authorization": f"Bearer {token2}"},
                    params=params,
                    timeout=REQUEST_TIMEOUT_SECONDS,
                )

        # Handle rate limit/backpressure
        if r.status_code == 429:
            retry_after = int(r.headers.get("Retry-After", "30"))
            raise OpenSkyRateLimitedError(retry_after_seconds=retry_after)

        r.raise_for_status()
        stats["api_calls"] += 1
        return r.json()

    except OpenSkyRateLimitedError:
        # Let caller decide how to back off (and how to behave under --once).
        raise
    except Exception as e:
        stats["errors"] += 1
        logger.error("OpenSky fetch error: %s", e)
        return None


def _budget_load() -> List[int]:
    try:
        with open(BUDGET_STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        calls = data.get("calls", [])
        if isinstance(calls, list):
            return [int(x) for x in calls if isinstance(x, (int, float, str))]
    except FileNotFoundError:
        return []
    except Exception:
        return []
    return []


def _budget_save(calls: List[int]) -> None:
    try:
        os.makedirs(os.path.dirname(BUDGET_STATE_PATH), exist_ok=True)
        with open(BUDGET_STATE_PATH, "w", encoding="utf-8") as f:
            json.dump({"calls": calls[-5000:]}, f)
    except Exception:
        # Budget is a safety belt; do not crash producer if persistence fails.
        return


def _budget_prune(calls: List[int], now_s: int) -> List[int]:
    cutoff = now_s - 24 * 60 * 60
    return [t for t in calls if t >= cutoff]


def _budget_wait_if_needed() -> None:
    if MAX_REQUESTS_PER_24H <= 0:
        return

    now_s = int(time.time())
    calls = _budget_prune(_budget_load(), now_s)
    if len(calls) < MAX_REQUESTS_PER_24H:
        _budget_save(calls)
        return

    oldest = min(calls)
    wait_s = max(1, (oldest + 24 * 60 * 60) - now_s)
    logger.warning(
        "OpenSky budget exhausted (calls_24h=%s max=%s). Sleeping %ss to stay within limits.",
        len(calls),
        MAX_REQUESTS_PER_24H,
        wait_s,
    )
    time.sleep(min(wait_s, 3600))


def _budget_record_call() -> None:
    if MAX_REQUESTS_PER_24H <= 0:
        return
    now_s = int(time.time())
    calls = _budget_prune(_budget_load(), now_s)
    calls.append(now_s)
    _budget_save(calls)


def transform_state_vector(state: List[Any], api_time_s: int) -> Optional[Dict[str, Any]]:
    # OpenSky indices:
    # 0: icao24, 1: callsign, 2: origin_country, 3: time_position, 4: last_contact,
    # 5: lon, 6: lat, 7: baro_alt, 8: on_ground, 9: velocity, 10: true_track,
    # 11: vertical_rate, 12: sensors, 13: geo_alt, 14: squawk, 15: spi, 16: position_source, 17: category
    if not state or len(state) < 17:
        return None

    icao24 = (state[0] or "").strip()
    if not icao24:
        return None

    lon = state[5]
    lat = state[6]
    if lon is None or lat is None:
        return None

    time_pos_s = state[3] if state[3] is not None else api_time_s
    last_contact_s = state[4] if state[4] is not None else time_pos_s

    position_source_map = {0: "ADS-B", 1: "ASTERIX", 2: "MLAT", 3: "FLARM"}

    item: Dict[str, Any] = {
        # unix ms (expected by init.sql MV)
        "timestamp": int(float(time_pos_s) * 1000),
        "last_contact": int(float(last_contact_s) * 1000),

        "icao24": icao24,
        "callsign": (state[1] or "").strip(),
        "origin_country": state[2] or "Unknown",

        "longitude": float(lon),
        "latitude": float(lat),

        "baro_altitude": float(state[7]) if state[7] is not None else 0.0,
        "geo_altitude": float(state[13]) if state[13] is not None else 0.0,

        "velocity": float(state[9]) if state[9] is not None else 0.0,
        "true_track": float(state[10]) if state[10] is not None else 0.0,
        "vertical_rate": float(state[11]) if state[11] is not None else 0.0,

        "on_ground": 1 if state[8] else 0,
        "spi": 1 if state[15] else 0,

        "position_source": position_source_map.get(state[16]) if state[16] is not None else None,
        "category": str(state[17]) if len(state) > 17 and state[17] is not None else None,
    }

    return item


def _fingerprint(item: Dict[str, Any]) -> str:
    coarse = {
        "lon": round(float(item.get("longitude", 0.0)), 4),
        "lat": round(float(item.get("latitude", 0.0)), 4),
        "alt": round(float(item.get("baro_altitude", 0.0)), 1),
        "spd": round(float(item.get("velocity", 0.0)), 1),
        "trk": round(float(item.get("true_track", 0.0)), 0),
        "gnd": int(item.get("on_ground", 0)),
    }
    raw = json.dumps(coarse, sort_keys=True).encode("utf-8")
    return hashlib.sha1(raw).hexdigest()


def should_emit(item: Dict[str, Any]) -> bool:
    icao24 = item.get("icao24")
    if not icao24:
        return False

    fp = _fingerprint(item)
    prev = _last_fp_by_icao24.get(icao24)
    if prev == fp:
        stats["dedup_dropped"] += 1
        return False

    _last_fp_by_icao24[icao24] = fp
    return True


def main() -> None:
    logger.info("Starting producer; mode=%s; topic=%s; interval=%ss", PRODUCER_MODE, KAFKA_TOPIC, POLL_INTERVAL_SECONDS)

    run_once = "--once" in sys.argv or os.getenv("RUN_ONCE", "0").strip() in {"1", "true", "TRUE", "yes", "YES"}

    producer = create_kafka_producer()

    if PRODUCER_MODE in {"replay"}:
        run_replay(producer=producer, run_once=run_once)
        return

    backoff_s = 5
    rate_limit_backoff_s = 0
    consecutive_429 = 0
    pending: List[Dict[str, Any]] = []
    next_fetch_at = 0.0
    last_publish_log = 0.0
    while True:
        started = time.time()

        # Fetch snapshot when due.
        if started >= next_fetch_at:
            if TRACER:
                span_ctx = TRACER.start_as_current_span("poll_and_publish")
            else:
                span_ctx = None

            try:
                if span_ctx:
                    span_ctx.__enter__()

                try:
                    payload = fetch_opensky()
                except OpenSkyRateLimitedError as e:
                    stats["errors"] += 1
                    consecutive_429 += 1
                    if run_once:
                        logger.error(
                            "RUN_ONCE enabled but OpenSky returned 429. "
                            "Provide OPENSKY_USERNAME/OPENSKY_PASSWORD or try again later. "
                            "Retry-After=%ss",
                            e.retry_after_seconds,
                        )
                        raise SystemExit(2) from e

                    retry_after = max(1, int(e.retry_after_seconds))
                    if rate_limit_backoff_s <= 0:
                        rate_limit_backoff_s = max(30, retry_after)
                    else:
                        rate_limit_backoff_s = min(max(rate_limit_backoff_s * 2, retry_after), 1800)

                    if consecutive_429 in {3, 10, 30} and not _auth_tuple():
                        logger.warning(
                            "Still receiving 429 without credentials. "
                            "Set OPENSKY_USERNAME/OPENSKY_PASSWORD (recommended) or expect sparse data due to rate limits."
                        )

                    sleep_s = int(rate_limit_backoff_s * (0.9 + random.random() * 0.2))
                    logger.warning(
                        "OpenSky rate-limited (429). Backing off for %ss (retry_after=%ss, consecutive_429=%s)",
                        sleep_s,
                        retry_after,
                        consecutive_429,
                    )
                    next_fetch_at = time.time() + max(1, sleep_s)
                    payload = None

                if not payload:
                    if run_once:
                        logger.error(
                            "RUN_ONCE enabled but no payload was fetched (error/backoff). "
                            "Provide OPENSKY_USERNAME/OPENSKY_PASSWORD or try again later."
                        )
                        raise SystemExit(1)

                    backoff_s = min(backoff_s * 2, 60)
                else:
                    backoff_s = 5
                    rate_limit_backoff_s = 0
                    consecutive_429 = 0

                    api_time_s = int(payload.get("time") or int(time.time()))
                    states = payload.get("states") or []

                    batch_items: List[Dict[str, Any]] = []
                    for st in states:
                        item = transform_state_vector(st, api_time_s=api_time_s)
                        if item:
                            batch_items.append(item)

                    pending.extend(batch_items)
                    _capture_write_items(batch_items, batch_time_ms=int(time.time() * 1000))

                    logger.info(
                        "Fetched OpenSky snapshot: states=%s queued=%s publish_rate=%s/s capture=%s",
                        len(states),
                        len(pending),
                        PUBLISH_RATE_PER_SECOND,
                        CAPTURE_ENABLED,
                    )

                    if run_once:
                        _send_items_paced(producer, pending, PUBLISH_RATE_PER_SECOND, run_once=True)
                        return

                next_fetch_at = time.time() + float(POLL_INTERVAL_SECONDS)

            finally:
                if span_ctx:
                    span_ctx.__exit__(None, None, None)

        # Publish continuously (paced) between fetches.
        if pending:
            if PUBLISH_RATE_PER_SECOND <= 0:
                _send_items_paced(producer, pending, 0, run_once=False)
                pending.clear()
            else:
                # Emit at most one second worth of items per loop to keep pacing stable.
                n = min(len(pending), PUBLISH_RATE_PER_SECOND)
                chunk = pending[:n]
                del pending[:n]
                _send_items_paced(producer, chunk, PUBLISH_RATE_PER_SECOND, run_once=False)

        now = time.time()
        if now - last_publish_log >= 10:
            last_publish_log = now
            logger.info("publisher pending_queue=%s sent_total=%s", len(pending), stats["sent"])

        # Avoid busy loop.
        time.sleep(0.05)


if __name__ == "__main__":
    main()
