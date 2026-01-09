# OpenSky Flight Tracker (Kafka → OTel → ClickHouse → Grafana)

Stack local (Docker Compose) que coleta estados de voo da OpenSky, publica no Kafka, processa via OpenTelemetry Collector e persiste no ClickHouse, com dashboards no Grafana.

## Arquitetura

- OpenSky API → producer → Kafka (`flight-positions`)
- OTel Collector (receiver Kafka) → ClickHouse (`aviation.otel_logs`)
- ClickHouse MV → `aviation.flight_positions` (tabela tipada para consulta)
- Grafana → ClickHouse

## Pré-requisitos

- Docker + Docker Compose v2
- (Recomendado) credenciais OpenSky

## Configuração (obrigatória)

1) Crie o arquivo de ambiente:

```bash
cp .env.example .env
```

2) Preencha ao menos:

- `CLICKHOUSE_USER` e `CLICKHOUSE_PASSWORD`
- `GF_SECURITY_ADMIN_PASSWORD`

OpenSky:

- Para autenticação clássica: `OPENSKY_USERNAME` e `OPENSKY_PASSWORD`
- Se você receber `401` mesmo com user/senha, use OAuth2 client-credentials:
	- `OPENSKY_CLIENT_ID` e `OPENSKY_CLIENT_SECRET`

## Subir o stack

Recomendado (faz check de portas e roda o bootstrap):

```bash
./scripts/up.sh
```

Modo replay (reproduz dados reais capturados anteriormente):

```bash
./scripts/up.sh --replay
```

Reset (remove volumes e apaga dados do ClickHouse/Grafana):

```bash
./scripts/up.sh --reset
```

## UIs

- Grafana: http://localhost:3000 (user `admin`, senha `GF_SECURITY_ADMIN_PASSWORD`)
- Kafka UI: http://localhost:8090
- ClickHouse HTTP: http://localhost:8123

## Dashboards (provisionados)

- OpenSky - Flights Overview
- OpenSky - Event Stream
- OpenSky - Pipeline Health

## Ingestão controlada (dados reais)

Para evitar burst a cada poll, o producer suporta “paced publishing”:

- `PUBLISH_RATE_PER_SECOND` (ex: `200`) distribui a publicação no Kafka ao longo do tempo.

### Capture + Replay (dados reais)

Útil quando você atingir `429` (rate limit) e quiser manter um fluxo contínuo a partir de dados reais previamente capturados:

- Capture:
	- `PRODUCER_MODE=opensky`
	- `CAPTURE_ENABLED=1`
	- `CAPTURE_DIR=/data/opensky_capture`

- Replay:
	- `PRODUCER_MODE=replay`
	- `REPLAY_DIR=/data/opensky_capture`
	- `REPLAY_MESSAGES_PER_SECOND=200`
	- `REPLAY_LOOP=1`

## Operação e validação

- Logs do producer:

```bash
docker compose logs -f opensky-producer
```

- Teste básico do pipeline:

```bash
./scripts/test_pipeline.sh
```

## Troubleshooting (ClickHouse/Grafana)

- `401` OpenSky: use OAuth2 (`OPENSKY_CLIENT_ID`/`OPENSKY_CLIENT_SECRET`) no producer.
- `429` OpenSky: aumente `POLL_INTERVAL_SECONDS`, reduza `BBOX_*` e/ou use capture+replay.
- ClickHouse/Grafana `max_execution_time`: este repo configura o profile `default` via `configs/clickhouse/profiles.xml` para compatibilidade com drivers/datasource.

## Estrutura do repositório

- `docker-compose.yml`: stack completa
- `configs/otel/otel-collector-config.yaml`: pipeline Kafka → ClickHouse
- `configs/clickhouse/init.sql`: bootstrap mínimo do DB
- `scripts/bootstrap_clickhouse_views.sh`: cria MV e tabelas derivadas
- `configs/grafana/*`: provisioning de datasource e dashboards
- `producer/opensky_producer.py`: coleta OpenSky + publica no Kafka
