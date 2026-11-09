# Clean Flight Paths from Noisy Aircraft Data at Scale

Scalable kinematic flight-trajectory reconstruction from noisy ADS-B data: accuracy vs. cost measured at scale on real OpenSky feeds.

## What is this?

Three reconstruction methods — (A) time-gap segmentation, (B) kinematic segmentation on speed/altitude-rate/turn-rate, and (C) a 6-state constant-velocity Kalman smoother — are run against the same cleaned ADS-B feed under four controlled noise profiles (N0-N3) and compared on one `(method x noise)` matrix.

Central research question (verbatim from `plan.md` §1):

> *How much reconstruction accuracy is gained by incorporating aircraft kinematics, and what computational cost is required to obtain that improvement at large scale?*

## Snapshot

- Validated on a 2-hour OpenSky S3 slice from 2022-06-27: **~4.36M raw state vectors**, 11,121 unique aircraft.
- Full 3-method x 4-noise matrix completes in **~90 minutes** on a laptop (single-node pandas; Spark promotion drafted).
- **16 invariant tests** green under `pytest etl4/tests/test_pipeline.py -v`.
- **3 Grafana dashboards** provisioned: ingest throughput, host/Spark system resources, pipeline results.

## Quickstart

```powershell
# 1. Environment
conda create -n etl4 python=3.11 -y
conda activate etl4
pip install -r etl4/requirements.txt

# 2. Infra (Kafka, Postgres, Prometheus, Grafana, Spark 1+2)
docker compose -f etl4/infra/docker-compose.yml up -d
bash etl4/infra/create_topics.sh

# 3. Download one day of S3 data (dry-run first, always)
python etl4/screpper.py --start-date 2022-06-27 --end-date 2022-06-27 --dry-run
python etl4/screpper.py --start-date 2022-06-27 --end-date 2022-06-27

# 4. Run the full experiment matrix
python etl4/bench/run_matrix.py --bronze data/bronze/raw_states --source s3 --profiles N0 N1 N2 N3
```

Grafana at <http://localhost:3000> (admin / admin by default) auto-loads all three dashboards.

## Repo map

| Path | What lives there |
| --- | --- |
| `plan.md` | Authoritative hand-off-grade specification |
| `CLAUDE.md` | AI-assistant operating contract |
| `etl4/` | Pipeline source (ingest, clean, reconstruct, bench, tests) |
| `etl4/docs/final_report.md` | Course write-up and headline results |
| `etl4/docs/decisions.md` | Append-only ADR log |
| `etl4/dashboard/` | Grafana 10.4 dashboard JSON |
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
