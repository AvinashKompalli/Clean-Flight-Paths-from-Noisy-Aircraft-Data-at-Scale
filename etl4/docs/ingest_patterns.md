# Ingest patterns — characterisation, not a leaderboard

This project ships five distinct ingest paths. They exist to **characterise
different DE patterns** against the same ADS-B dataset — not to race them.
Throughput numbers below are bounded by factors outside the pipeline
(API quotas, network egress, dataset size), so read them as shape, not
ranking.

| # | Source         | Pattern     | Code entrypoint                                           | Typical throughput envelope   | Bounded by                              | When to reach for it                       |
|---|----------------|-------------|-----------------------------------------------------------|-------------------------------|-----------------------------------------|--------------------------------------------|
| 1 | OpenSky REST   | streaming   | `etl4/ingest/live_producer.py`                            | ~4 events / s (steady)        | OpenSky OAuth2 quota                    | Live monitoring, freshness demo            |
| 2 | OpenSky S3     | batch       | `etl4/screpper.py --repo opensky`                         | ~0.2–1 GB / min (network)     | HTTPS throughput, archive layout        | Historical backfill, full-day slices       |
| 3 | Zenodo         | batch       | `etl4/screpper.py --repo zenodo`                          | 200 MB per release, bursty    | Zenodo API quota, single HTTPS GET      | Flightlist versioned datasets              |
| 4 | Synthetic      | generative  | `etl4/ingest/synthetic.py`                                | ~5M rows / min (CPU)          | CPU, numpy allocation                   | Offline testing, ground-truth validation   |
| 5 | Bronze replay  | stream      | `etl4/ingest/replay_producer.py`                          | ~1M events / min at speed=max | Kafka produce latency, disk read        | Reproducing live pipelines from history    |

## Orchestration

- **Streaming (paths 1, 5)** — native Kafka producers, no external
  orchestrator. Prometheus scrapes their counters.
- **Batch (paths 2, 3, 4)** — Apache Airflow 2.9 in standalone mode.
  Three DAGs:
  - `opensky_s3_batch` — daily, pulls prior day
  - `zenodo_versioned_pull` — monthly, pulls latest release
  - `nightly_vault_rebuild` — bronze → vault → marts → Postgres
- **Vault rebuild (path 6)** — chained BashOperators under Airflow.

Why Airflow only for batch? Streaming producers are long-running services
that don't fit a DAG model well; they're owned by systemd / docker, not
by an orchestrator. Mixing them in Airflow is a common anti-pattern.

## Metrics

Every ingest path writes one row to `ingest_runs` (Postgres) on completion
via the `ingest_run()` context manager in
`etl4/orchestration/ingest_metrics.py`. Columns: `source`, `pattern`,
`started_at`, `wall_seconds`, `rows_out`, `bytes_out`, `status`.

Streaming paths additionally emit Prometheus counters
(`records_sent_total`, `kafka_produce_latency_seconds`).

Dashboard `05_ingest_comparison` merges both sources: Postgres for batch
telemetry, Prometheus for streaming rates.

## Caveats

1. "Throughput" across paths is **not comparable** — they are bounded by
   different external factors (see table).
2. `rows_out` for Zenodo is the parquet-converted row count, not the raw
   CSV; it's smaller (dedup + date-cast).
3. Live producer `bytes_out` is 0 — Kafka records are small, we didn't
   instrument wire-size.
4. Airflow runs batch DAGs **inside its container**, mounting the project
   source read-write at `/opt/project`. Writes go to the host's
   `data/bronze/` just like direct invocations.
