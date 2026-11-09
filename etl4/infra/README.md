# ETL4 Infrastructure

Docker Compose stack plus provisioning for the ADS-B pipeline: Kafka (ingest bus), Postgres (results), Prometheus + Grafana (observability), Spark master and two workers, node-exporter.

## What's in this folder

- `docker-compose.yml` — eight services; Spark 3.5.3, Kafka 3.8.0 (KRaft), Postgres 16, Prometheus, Grafana, node-exporter.
- `prometheus.yml` — scrape config for node-exporter, live/replay producers (`host.docker.internal:8000`/`8001`), Spark master/workers.
- `create_tables.sql` — DDL for `pipeline_runs`, `eval_results`, `deviation_events`.
- `create_topics.sh` — idempotent Kafka topic creation (`adsb.raw`, `adsb.dlq`).
- `bring_up_stack.ps1` — one-shot bring-up and health poll (this file documents it).
- `grafana/provisioning/` — datasources and dashboards auto-loaded on Grafana start. Dashboard JSON lives at `etl4/dashboard/` and is bind-mounted into the container.

## Prerequisites

- Docker Desktop running (WSL2 backend on Windows).
- Local ports free: 3000 (Grafana), 4040, 5432 (Postgres), 7077, 8080, 8081 (Spark), 9090 (Prometheus), 9092 (Kafka), 9100 (node-exporter).
- PowerShell for the bring-up script; Git Bash for `create_topics.sh`.
- Optional: set `POSTGRES_PASSWORD` in your shell (default is `change_me`, matched in both `postgres` and `grafana` services).

## Bring the stack up

```powershell
.\etl4\infra\bring_up_stack.ps1
```

The script is idempotent. Re-running against a healthy stack verifies each service and exits fast. It applies the DDL (`CREATE TABLE IF NOT EXISTS`) and creates topics (`--if-not-exists`), then prints a URL summary.

## Service inventory

| Service | Image | Local URL | Purpose |
| --- | --- | --- | --- |
| kafka | `apache/kafka:3.8.0` | `localhost:9092` | Ingest bus, KRaft, one broker |
| postgres | `postgres:16` | `localhost:5432` (db `etl4`, user `etl4`) | Pipeline run and evaluation results |
| prometheus | `prom/prometheus:latest` | <http://localhost:9090> | Metric scraper |
| grafana | `grafana/grafana:latest` | <http://localhost:3000> | Dashboards (auto-provisioned) |
| spark-master | `apache/spark:3.5.3` | <http://localhost:8080> | Standalone master UI + `/metrics/master/prometheus` |
| spark-worker-1 | `apache/spark:3.5.3` | <http://localhost:8081> | Worker UI + `/metrics/prometheus` |
| spark-worker-2 | `apache/spark:3.5.3` | (internal only) | Second worker, no external port |
| node-exporter | `prom/node-exporter:latest` | <http://localhost:9100/metrics> | Host CPU/mem/disk/net |

## Grafana first login

Default `admin` / `admin` (`GF_SECURITY_ADMIN_PASSWORD`, override via `GRAFANA_PASSWORD`). Change the password after first login under *Profile -> Change password*. Datasources and the dashboard provider re-provision on every container start.

## Troubleshooting

- **"Docker is not running"** — start Docker Desktop and re-run the script.
- **Port conflict (9092/5432/9090/3000/8080/8081)** — `Get-NetTCPConnection -LocalPort 9092` finds the holder; stop it or change the published-port side of the mapping.
- **Grafana dashboards empty** — no traffic yet. Start the replay producer (`python etl4/ingest/replay_producer.py --path data/bronze/raw_states --speed max`) or the live producer (`python etl4/ingest/live_producer.py`) and allow one scrape interval (15 s).
- **Spark PrometheusServlet shows no data** — `spark.ui.prometheus.enabled=true` wires the driver/executor surface reliably; the standalone master/worker sinks may also need a `metrics.properties` bind-mount registering `*.sink.prometheusServlet.class=org.apache.spark.metrics.sink.PrometheusServlet` (version-dependent; flagged in Round 3).
- **Postgres password mismatch** — `postgres` and `grafana` both read `${POSTGRES_PASSWORD}`. If they diverge, `docker compose down -v` wipes `pgdata` so the next bring-up reseeds with the current value.

## Tear down

```powershell
docker compose -f etl4\infra\docker-compose.yml down       # stop, keep volumes
docker compose -f etl4\infra\docker-compose.yml down -v    # also wipe pgdata
```
