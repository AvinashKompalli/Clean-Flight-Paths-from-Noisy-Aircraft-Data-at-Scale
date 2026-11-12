# Clean Flight Paths from Noisy Aircraft Data at Scale

Scalable kinematic flight-trajectory reconstruction from noisy ADS-B data: accuracy vs. cost measured at scale on real OpenSky feeds.

## What is this?

Three reconstruction methods — (A) time-gap segmentation, (B) kinematic segmentation on speed/altitude-rate/turn-rate, and (C) a 6-state constant-velocity Kalman smoother — are run against the same cleaned ADS-B feed under four controlled noise profiles (N0-N3) and compared on one `(method x noise)` matrix.

Central research question (verbatim from `plan.md` §1):

> *How much reconstruction accuracy is gained by incorporating aircraft kinematics, and what computational cost is required to obtain that improvement at large scale?*

## Snapshot

- Validated on a 2-hour OpenSky S3 slice from 2022-06-27: **~4.36M raw state vectors**, 14,107 unique aircraft after a fresh Zenodo flightlist pull.
- Full 3-method x 4-noise matrix completes in **~90 minutes** on a laptop (single-node pandas; Spark promotion drafted).
- **20 invariant tests** green under `pytest etl4/tests/test_pipeline.py -v`.
- **5 Grafana dashboards** provisioned: ingest throughput, host/Spark system resources, pipeline results, flight paths, ingest-path comparison.
- **Data Vault 2.0 raw vault + marts** persisted to Postgres (`dim_aircraft`, `dim_airport`, `fact_flight`, `fact_trajectory_enriched`, `fact_experiment_result`).

## Prerequisites

### Host software

| Tool | Version | Notes |
| --- | --- | --- |
| Docker Desktop / Engine + Compose v2 | 24.x or newer | Hosts Postgres, Kafka, Grafana, Prometheus, Spark 1+2, Airflow |
| Python | **3.11** (pinned) | Validated in CI; the maintainer's laptop also runs 3.14.2 successfully |
| conda or venv | any recent | Only needed to isolate the Python 3.11 env |
| Git | 2.30+ | Any modern version works |
| Git LFS | 3.0+ | Only needed for `opensky_live_data.csv`; `airports.csv` is plain text as of commit `ef3a95f` |
| Bash | from Git for Windows / MSYS | Used by `etl4/infra/create_topics.sh` on Windows |
| PowerShell 5+ | ships with Windows 11 | Primary shell for the example commands below |

### Python packages (installed via `pip install -r etl4/requirements.txt`)

All pinned at the `>=` baseline below; the pipeline is validated against the latest minor at the time of writing.

| Package | Validated version | Role |
| --- | --- | --- |
| `pandas` | 3.0.1 | Core dataframe; nullable dtypes required |
| `numpy` | 2.4.2 | Vectorised math |
| `pyarrow` | 24.0.0 | Parquet I/O and footer-only row counts |
| `psycopg[binary]` | 3.3.8 | Postgres driver (COPY FROM STDIN, autocommit paths) |
| `confluent-kafka` | 2.x | Producer for the live ingest path |
| `pyopensky` | 2.x | OpenSky S3 historical ingest wrapper |
| `prometheus-client` | 0.20+ | Producer-side metrics scraped by Prometheus |
| `duckdb` | 1.x | Ad-hoc parquet probing in test suite |
| `python-dotenv` | 1.x | `.env` loader |
| `pyyaml` | 6.x | Config |
| `requests` | 2.31+ | OAuth2 refresh for the live REST path |
| `pytest` | 9.x | Test runner (20 invariants in `etl4/tests/test_pipeline.py`) |

### Dockerised services

| Image | Version | Exposed port | Role |
| --- | --- | --- | --- |
| `postgres` | 16 (16.15) | 5432 | Analytics store (marts, `pipeline_runs`, `eval_results`, `ingest_runs`, `fact_experiment_result`) |
| `grafana/grafana` | 10.4.5 | 3000 | Five auto-provisioned dashboards |
| `prom/prometheus` | latest | 9090 | Scrapes node-exporter + producer metrics |
| `prom/node-exporter` | latest | 9100 | Host resource metrics |
| `apache/kafka` | 3.8.0 | 9092 | KRaft mode (no Zookeeper) |
| `apache/spark` | 3.5.3 | 7077, 8080, 4040, 8081 | Master + 2 workers (optional; single-node pandas path also works) |
| `apache/airflow` | 2.9.3-python3.11 | 8085 | Standalone; hosts three DAGs (`zenodo_versioned_pull`, `opensky_live_stream`, `nightly_vault_rebuild`) |

Spin everything up with:
```powershell
docker compose -f etl4/infra/docker-compose.yml up -d
docker compose -f etl4/infra/airflow-compose.yml up -d    # optional, Airflow DAGs
```

## Quickstart

```powershell
# 1. Environment
conda create -n etl4 python=3.11 -y
conda activate etl4
pip install -r etl4/requirements.txt

# 2. Infra (Kafka, Postgres, Prometheus, Grafana, Spark 1+2, Airflow)
docker compose -f etl4/infra/docker-compose.yml up -d
bash etl4/infra/create_topics.sh
```

### Option A - one command, with a UI (recommended)

```powershell
python -m etl4.control.control_panel
```

Opens <http://localhost:8090> in your browser. One **Run End-to-End** button drives
the full chain (ingest -> bronze -> vault -> marts -> Postgres sink -> noise-method
matrix -> `fact_experiment_result` eval mart), live-streams stage output, then
surfaces the five Grafana dashboards as clickable cards.

### Option B - one command, headless

```powershell
python etl4/run_e2e.py                                 # zenodo + N0..N3, ~100 min
python etl4/run_e2e.py --profiles N0                   # smoke, ~10 min
python etl4/run_e2e.py --only matrix eval_mart         # just the experiment + mart
python etl4/run_e2e.py --source live --live-limit 2000 # live REST stream (secondary)
```

Stages: `infra schema ingest bronze vault marts sink matrix eval_mart`. Any subset
via `--only` / `--skip`.

### Option C - hand-drive the stages

```powershell
python -m etl4.orchestration.ingest_runner --source zenodo --latest
python -m etl4.vault.bronze_flightlists
python -m etl4.vault.raw_vault
python -m etl4.vault.marts
python -m etl4.vault.postgres_sink
python etl4/bench/run_matrix.py --bronze data/bronze/raw_states --source zenodo --profiles N0 N1 N2 N3
```

Grafana at <http://localhost:3000> (admin / admin by default) auto-loads all five
dashboards. Airflow at <http://localhost:8085> hosts three DAGs
(`opensky_s3_batch`, `zenodo_versioned_pull`, `nightly_vault_rebuild`) that drive
the same stages on a schedule.

## Repo map

| Path | What lives there |
| --- | --- |
| `plan.md` | Authoritative hand-off-grade specification |
| `CLAUDE.md` | AI-assistant operating contract |
| `etl4/` | Pipeline source (ingest, clean, reconstruct, bench, vault, tests) |
| `etl4/run_e2e.py` | One-command end-to-end runner (9 stages, `--only` / `--skip` aware) |
| `etl4/control/` | Stdlib-only HTTP control panel (`python -m etl4.control.control_panel`) |
| `etl4/orchestration/` | Airflow DAGs + per-path ingest telemetry |
| `etl4/vault/` | Data Vault 2.0 raw vault, marts, and Postgres COPY sink |
| `etl4/docs/final_report.md` | Course write-up and headline results |
| `etl4/docs/decisions.md` | Append-only ADR log |
| `etl4/dashboard/` | Grafana 10.4 dashboard JSON (01..05) |
| `etl4/infra/` | Docker Compose, Prometheus config, Postgres DDL |
| `data/` | Gitignored working data (regenerated from source) |
| `software/` | Gitignored native Kafka install |

## Where to go from here

- Full specification: `plan.md`
- Experimental write-up with numbers: `etl4/docs/final_report.md`
- Design decisions and rationale: `etl4/docs/decisions.md`
- Test suite: `pytest etl4/tests/test_pipeline.py -v`

---

*DA 231o - Data Engineering at Scale, Indian Institute of Science, Bangalore.*
