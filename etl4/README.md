# ETL4: ADS-B Trajectory Reconstruction

This repo holds Avinash's parts: ingest, replay, benchmark, infra.

## Setup (no OpenSky access needed)

1. Create the env:
   `conda create -n opensky python=3.12 -y`
   `conda activate opensky`
   `pip install -r requirements.txt`
2. Copy `.env.example` to `.env`. Keep it out of Git.
3. Make test data: `python ingest/synthetic.py --hours 1`
4. Run the test: `pytest -q`
5. Start the stack: `docker compose -f infra/docker-compose.yml up -d`
6. Make topics: `bash infra/create_topics.sh`
7. Replay test data: `python ingest/replay_producer.py --speed 10`
   (you write the TODO parts first)

## Switch to real data

When Trino access is approved, set `SOURCE=trino` in `.env`.
Then run `python ingest/ingest_opensky_day.py --date YYYY-MM-DD`.

## Layout

| Folder | Content |
|---|---|
| ingest | Trino ingest, synthetic data, replay producer |
| infra | Docker Compose, Prometheus, topic script |
| config | Thresholds and paths. One file. |
| tests | Unit tests |
| docs | decisions.md |
| data | Local data. Not in Git. |

Folders for other owners (clean, reconstruct, evaluate, dags, dashboard, bench)
are empty placeholders.

## Not in the stack yet

Spark and Airflow 3. Add them after the first four services run.
