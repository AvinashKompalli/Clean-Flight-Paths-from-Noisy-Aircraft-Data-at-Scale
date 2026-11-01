# Scalable Kinematic Flight Trajectory Reconstruction from Noisy ADS-B Data: Accuracy–Cost Trade-offs at Scale

**Team ETL4** · DA 231o — Data Engineering at Scale · Indian Institute of Science, Bangalore

| Member | Email |
|---|---|
| Nadhiya R S | nadhiyar@iisc.ac.in |
| Kasi Annapurna Uppalapati | kasiannapur1@iisc.ac.in |
| Kompalli Avinash Bhargav | bavinash@iisc.ac.in |
| Sathyanarayanan C | sathyanaray3@iisc.ac.in |

**Authoritative proposal**: `ETL4 (3) (1).pdf` (project root)
**This document**: implementation plan derived from the proposal; last updated 2026-10-31.

---

## Central Research Question

> **How much reconstruction accuracy can be gained by incorporating aircraft kinematics, and what computational cost is required to obtain that improvement at large scale?**

The project will also identify significant deviations from a reference or expected flight path and generate a candidate corrected/plausible trajectory for analytical purposes.

---

## Table of Contents

1. [Problem Definition and Motivation](#1-problem-definition-and-motivation)
2. [Design Goals](#2-design-goals)
3. [Architecture](#3-architecture)
4. [Data Sources](#4-data-sources)
5. [Schema](#5-schema)
6. [Pipeline Stages](#6-pipeline-stages)
7. [Three Reconstruction Methods](#7-three-reconstruction-methods)
8. [Infrastructure](#8-infrastructure)
9. [Team Ownership](#9-team-ownership)
10. [Current Status](#10-current-status)
11. [Detailed TODOs](#11-detailed-todos)
12. [Experiment Plan](#12-experiment-plan)
13. [Evaluation Metrics](#13-evaluation-metrics)
14. [Timeline / Milestones](#14-timeline--milestones)
15. [Open Questions](#15-open-questions)
16. [How to Run](#16-how-to-run)
17. [Testing Strategy](#17-testing-strategy)
18. [Monitoring and Observability](#18-monitoring-and-observability)
19. [Known Constraints and Risks](#19-known-constraints-and-risks)
20. [References](#20-references)

---

## 1. Problem Definition and Motivation

Aircraft continuously broadcast Automatic Dependent Surveillance–Broadcast (ADS-B) observations containing aircraft identifiers, timestamps, latitude, longitude, altitude, velocity, heading, and vertical movement information. At large scale, these observations are difficult to convert into complete flight trajectories because the data may contain:

- Missing points
- Duplicates
- Irregular sampling
- Out-of-order observations

A common approach to flight reconstruction splits an aircraft's observations whenever the **time gap** between consecutive points exceeds a threshold. Although computationally inexpensive, this approach **does not consider whether the observed movement is physically consistent** with aircraft speed, heading, altitude, or vertical rate.

- Two observations separated by a **large time gap** may still belong to the same flight if the aircraft's motion is kinematically consistent.
- A **short-gap sequence** containing an implausible position jump may represent corrupted or noisy data.

The project investigates whether aircraft **kinematics** can improve reconstruction accuracy, and quantifies the computational cost of that improvement. It compares three methods under identical conditions (see Section 7).

---

## 2. Design Goals

The system is designed to:

| # | Goal |
|---|---|
| G1 | Ingest large-scale historical and streaming ADS-B observations from the OpenSky Network |
| G2 | Handle missing, duplicated, irregular, and out-of-order records |
| G3 | Reconstruct flights using both time-based and kinematic methods |
| G4 | Extract kinematic features: speed, heading, acceleration, turn rate, altitude change, vertical rate |
| G5 | Detect significant deviations from a reference or expected trajectory |
| G6 | Generate a smoothed or candidate-corrected trajectory when deviations or noisy observations are detected |
| G7 | Measure reconstruction accuracy together with processing cost and scalability |

---

## 3. Architecture

### 3.1 High-level data flow (per project proposal)

```
                        OpenSky Network
                              │
                              ▼
                           Apache Kafka
                              │
                              ▼
                 Apache Spark Structured Streaming
                              │
                              ▼
                   Cleaning + Deduplication
                              │
                              ▼
                      Event-Time Ordering
                              │
                              ▼
                  Kinematic Feature Extraction
                              │
            ┌─────────────────┼─────────────────────┐
            ▼                 ▼                     ▼
    Time-Gap Baseline   Kinematic Reconstr.   Kinematic + Smoothing
            │                 │                     │
            └─────────────────┼─────────────────────┘
                              ▼
                      Flight Reconstruction
                              │
                              ▼
                   Reference / Expected Path
                              │
                              ▼
                       Deviation Detection
                              │
                              ▼
             Candidate Path Correction / Reconstruction
                              │
                              ▼
                          Evaluation
```

### 3.2 Platform choices

| Platform | Purpose |
|---|---|
| **Apache Kafka** | Scalable ingestion, buffering, and replay of ADS-B observations |
| **Apache Spark Structured Streaming** | Distributed processing of streaming and historical trajectory data |
| **Spark DataFrames** | Cleaning, transformation, deduplication, feature engineering |
| **Spark SQL** | Temporal operations, windowing, aggregation, aircraft-wise processing |

Trajectory estimation operates on **aircraft-wise ordered sequences** (keyed by `icao24`). Spark provides the distributed execution required to process many aircraft and large historical datasets simultaneously. Airflow 3 orchestrates all stages (added after the four base services are stable).

---

## 4. Data Sources

### 4.1 OpenSky REST API (live streaming)

| Property | Value |
|---|---|
| Endpoint | `https://opensky-network.org/api/states/all` |
| Auth | OAuth2 client credentials (auto-refreshed every ~4.5 min) |
| Rate | 1 request / 10 s anonymous; 1 request / 5 s with account |
| Records per global poll | ~12,000–12,500 state vectors |
| Script | `etl4/ingest/live_producer.py` |
| Credentials | `OPENSKY_CLIENT_ID`, `OPENSKY_CLIENT_SECRET` in `.env` |

Supports regional bounding box via `--bounds west,south,east,north` (e.g. India: `68,8,97,37` gives ~150–200 aircraft per poll).

### 4.2 OpenSky Scientific Datasets (S3 historical)

| Property | Value |
|---|---|
| Bucket | `https://s3.opensky-network.org/data-samples` |
| Prefix | `states/YYYY-MM-DD/HH/states_YYYY-MM-DD-HH.csv.tar` |
| Public range | 2020-05-25 to 2022-06-27 (105 Mondays) |
| Format | Per-hour `.csv.tar` archives (~100–220 MB each) |
| Total | ~279 GB (CSV, all 105 days × 24 hours) |
| Script | `etl4/screpper.py` |

Pre-2020 (dot-prefix `states/.YYYY-MM-DD/`) is archived. Post-2022 requires Trino.

### 4.3 OpenSky Trino (NOT AVAILABLE — access rejected)

OpenSky denied the Trino account request on 2026-10-31. `ingest_opensky_day.py` remains in the repo as a dormant script; **do not plan around it**. Use the S3 historical source (Section 4.2) and REST API (Section 4.1) only.

### 4.4 Synthetic Data (controlled-noise experiments)

| Property | Value |
|---|---|
| Script | `etl4/ingest/synthetic.py` |
| Aircraft | 20 (configurable via `config.yaml`) |
| Step | 10 seconds |
| Noise profile | 3 % gaps, 0.2 % position jumps, 0.5 % duplicates |
| Output | Noisy bronze Parquet + clean ground-truth Parquet |

Synthetic data is also used for **controlled artificial-noise experiments** to evaluate robustness to missing, duplicated, and out-of-order observations (see Section 12).

### 4.5 Reference / Expected Trajectory Sources

Validation references are **benchmarks, not perfect ground truth** (they are also derived from observed flight data):

- OpenSky-derived flight records (where available)
- Historical trajectory corridors for repeated routes (great-circle approximations between aerodromes)
- Smoothed version of the same trajectory as a plausible reference when no external route is available
- Synthetic ground truth (for the synthetic-data experiments only)

---

## 5. Schema

Canonical schema shared by all stages (defined in `ingest/synthetic.py::COLUMNS`). Keyed by `(icao24, time)`.

| Field | Type | Source | Notes |
|---|---|---|---|
| `time` | int64 (unix s) | all | Observation / snapshot timestamp |
| `icao24` | string | all | 6-char hex transponder address. **Never cast to int** (loses leading zeros) |
| `lat` | float64 | all | Decimal degrees, WGS-84 |
| `lon` | float64 | all | Decimal degrees, WGS-84 |
| `velocity` | float64 | all | Ground speed, m/s |
| `heading` | float64 | all | True track, degrees clockwise from north |
| `vertrate` | float64 | all | Vertical rate, m/s (positive = climbing) |
| `callsign` | string | all | Flight identifier, trailing spaces stripped |
| `onground` | bool | all | `True` if on the ground |
| `alert` | bool | REST→False | Not exposed by REST API; always `False` for live records |
| `spi` | bool | all | Special Position Identification bit |
| `squawk` | string | all | 4-digit octal transponder code. **String** (leading zeros matter) |
| `baroaltitude` | float64 | all | Barometric altitude, metres |
| `geoaltitude` | float64 | all | GPS / geometric altitude, metres |
| `lastposupdate` | float64 | all | Unix timestamp of last position fix |
| `lastcontact` | float64 | all | Unix timestamp of last contact |
| `hour` | int64 | all | `(time // 3600) * 3600` — Parquet partition key |

### Kinematic feature schema (added by stage 6.4)

| Field | Type | Formula / notes |
|---|---|---|
| `dt` | float64 | `time[i] − time[i−1]` per icao24 (seconds) |
| `dist_m` | float64 | Haversine distance between consecutive points, metres |
| `speed_est` | float64 | `dist_m / dt` — compare to broadcast `velocity` for consistency |
| `accel` | float64 | `(velocity[i] − velocity[i−1]) / dt`, m/s² |
| `turn_rate` | float64 | `(heading[i] − heading[i−1]) / dt`, deg/s (wrapped to ±180°) |
| `alt_rate` | float64 | `(baroaltitude[i] − baroaltitude[i−1]) / dt`, m/s |
| `kinematic_state` | struct | `[x, y, z, vx, vy, vz]` for Kalman filter — see Section 7.3 |

### Type-safety rules

- `icao24` and `squawk` are **always strings**.
- `onground`, `alert`, `spi` are **booleans**, not CSV strings.
- `time` and `hour` are **int64**; all other timestamps are float64 (nullable).

### Parquet layout

```
data/bronze/raw_states/  → raw state vectors (from screpper or synthetic)
data/silver/clean/       → deduplicated, validated (stage 6.2)
data/silver/features/    → with kinematic features (stage 6.4)
data/gold/flights/       → reconstructed flights, one row per flight (stage 7)
data/gold/trajectories/  → one row per second per aircraft (stage 7)
data/gold/deviations/    → flagged deviation events (stage 6.7)
```

All layers partitioned by `date=YYYY-MM-DD/hour=H/` for Spark predicate pushdown.

---

## 6. Pipeline Stages

Pipeline follows the architecture in Section 3.1 exactly. Each sub-section below corresponds to one box in the diagram.

### 6.1 Ingest

**Responsibility**: Get state vectors into Kafka `adsb.raw` with correct schema and `key=icao24`.

| Script | Mode | Status |
|---|---|---|
| `live_producer.py` | Live REST API → Kafka (OAuth2) | Done |
| `screpper.py` | S3 historical → bronze Parquet | Done |
| `replay_producer.py` | Bronze Parquet → Kafka (speed factor) | Framework, TODOs remain |
| `ingest_opensky_day.py` | Trino → bronze Parquet | Framework, TODOs remain |
| `synthetic.py` | Synthetic generator → bronze Parquet + ground truth | Done |

### 6.2 Cleaning + Deduplication

**Location**: `clean/` (Spark Structured Streaming job)
**Input**: `adsb.raw` or `data/bronze/raw_states/`
**Output**: `data/silver/clean/`

| Task | Description |
|---|---|
| Exact-duplicate removal | Drop rows with identical `(icao24, time, lat, lon)` |
| Schema enforcement | Assert COLUMNS present; cast types; drop rows with null `icao24` |
| Range checks | Reject `lat ∉ [−90, 90]`, `lon ∉ [−180, 180]`, `baroaltitude ∉ [−500, 15000]` |
| Null-position filter | Drop rows with null `lat` or `lon` for trajectory work |
| Callsign / squawk normalisation | Strip whitespace, upper-case, null-fill empty strings |
| Position-jump flag | Flag rows where straight-line distance / Δt > 340 m/s (max cruising speed) |
| Metric emission | `clean_rows_in_total`, `clean_rows_out_total`, `clean_duplicates_dropped_total` |

### 6.3 Event-Time Ordering

**Responsibility**: Guarantee per-aircraft chronological order despite out-of-order arrivals.

| Task | Description |
|---|---|
| Watermark | Spark `withWatermark("time", "60 seconds")` to bound late arrivals |
| Sorted aircraft window | For each `icao24`, apply window function ordered by `time` |
| Late-record handling | Records arriving beyond the watermark go to `adsb.dlq` with reason `"late"` |
| Monotonic check | Assert `time[i] ≥ time[i−1]` after sort; log violations |

Event-time ordering is a separate stage because it operates on the clean stream and is required before any kinematic difference can be computed.

### 6.4 Kinematic Feature Extraction

**Location**: `clean/features.py` (Spark job)
**Input**: Clean, ordered stream
**Output**: `data/silver/features/`

Compute per-record features (schema in Section 5):

- `dt`, `dist_m` from consecutive rows per `icao24`
- `speed_est` (position-derived) vs. `velocity` (broadcast)
- `accel` (velocity-derived)
- `turn_rate` (heading-derived, wrapped to ±180°)
- `alt_rate` (altitude-derived)
- `kinematic_state` vector `[x, y, z, vx, vy, vz]` for Section 7.3 input

These features drive the kinematic reconstruction methods and the deviation detector.

### 6.5 Flight Reconstruction

See **Section 7** — three parallel methods compared under identical conditions.

### 6.6 Reference / Expected Path

**Responsibility**: Produce a plausible reference trajectory for each reconstructed flight, used by the deviation detector and evaluator.

Priority order of reference sources:

1. **Flight-plan or filed-route record** (if available in OpenSky flight records)
2. **Great-circle route** between inferred departure and arrival aerodromes (nearest-airport lookup from `data/reference/airports.csv`)
3. **Historical trajectory corridor** — median path across prior days for the same `callsign` route
4. **Smoothed self-reference** — Kalman-smoothed version of the same trajectory as a plausible local reference
5. **Synthetic ground truth** — only for the controlled synthetic-noise runs

### 6.7 Deviation Detection

**Location**: `reconstruct/deviations.py`
**Input**: Reconstructed flight + reference path
**Output**: `data/gold/deviations/`

| Measure | Definition | Flag threshold |
|---|---|---|
| **Cross-track distance** | Perpendicular distance from reference path | > 5 km sustained for ≥ 60 s |
| **Along-track lag** | Signed distance along the reference path vs. expected | TBD |
| **Altitude deviation** | `baroaltitude − reference altitude` | > 300 m sustained for ≥ 60 s |
| **Residual** | `‖observed − Kalman-predicted‖` from Section 7.3 | > 3 σ |

Each deviation event is written as one row: `(icao24, flight_id, start_time, end_time, type, max_distance, peak_residual)`.

### 6.8 Candidate Path Correction

**Location**: `reconstruct/correction.py`
**Trigger**: Deviation flag OR Kalman residual threshold crossed
**Output**: Corrected trajectory column alongside the raw one in `data/gold/trajectories/`

| Task | Description |
|---|---|
| Noisy-point suppression | When Kalman residual exceeds threshold, replace observed point with filter prediction |
| Gap interpolation | Linear interpolation of lat, lon, baroaltitude for gaps ≤ 120 s |
| Discontinuity bridge | For gaps 120–600 s: use great-circle interpolation along reference path if available |
| Flag | Mark each corrected point with `corrected=True` so downstream knows which are observed vs. inferred |

### 6.9 Evaluation

See **Section 13** — full metrics table.

---

## 7. Three Reconstruction Methods

All three operate on the kinematic-feature stream (Section 6.4) and produce `(icao24, flight_id, segment_start, segment_end, trajectory)` records. Comparing them under identical input is the **central experiment** of this project.

### 7.1 Method A — Time-Gap Baseline

**Rule**: Consecutive observations are grouped into the same flight unless the time gap exceeds a threshold (default 30 min).

| Property | Value |
|---|---|
| Features used | `time` only |
| Compute cost | Low (one pass, no joins) |
| Expected to miss | Long-gap continuations where motion is consistent |
| Expected to catch | Simple, obvious breaks |

### 7.2 Method B — Kinematic Reconstruction

**Rule**: A segmentation break is declared when the next observation is **kinematically implausible** given current motion.

Features used: position, velocity, heading, altitude, vertical rate, acceleration, turn rate.

Boundary conditions (ANY triggers a break):

| Condition | Threshold |
|---|---|
| On-ground transition | `onground` flips from False→True→False with gap > 10 min |
| Position jump | implied `speed_est` > 340 m/s |
| Altitude jump | `|alt_rate|` > 50 m/s |
| Heading reversal | `|turn_rate|` > 30 deg/s sustained |
| Callsign change | `callsign` changes while airborne |

Compute cost: Medium (requires feature lag joins per aircraft).

### 7.3 Method C — Kinematic Reconstruction with Smoothing (Kalman Filter)

**Rule**: Same as Method B, but the trajectory itself is smoothed by a Kalman filter. Noisy observations are replaced by filter predictions when the residual exceeds threshold.

**State vector**:

```
x_t = [x_t, y_t, z_t, v_x, v_y, v_z]^T
```

- `(x, y, z)` — position in local ENU frame (metres) centred on each flight's first observation
- `(vx, vy, vz)` — velocity (m/s)

**Transition model** (constant velocity):

```
x_{t+1} = F x_t + w,  F = [[I₃, Δt·I₃], [0, I₃]]
```

**Measurement model** (position-only observation):

```
z_t = H x_t + v,  H = [I₃, 0]
```

Process-noise covariance `Q` and measurement-noise covariance `R` tuned on synthetic data (initial values documented in `reconstruct/kalman.py`).

**Residual threshold**: `‖z_t − H x̂_{t|t-1}‖ > 3·√tr(R)` ⇒ replace observation with prediction; emit deviation event.

Compute cost: High (per-aircraft stateful filter, cannot easily parallelise within a single flight).

### 7.4 Expected cost / accuracy ordering (hypothesis to test)

| Method | Advantage | Expected cost |
|---|---|---|
| Time-gap | Simple and fast | **Low** |
| Kinematic | Uses aircraft motion | **Medium** |
| Kinematic + smoothing | Better trajectory quality | **High** |

The project tests whether the accuracy gain from B→C justifies the cost at 1-day regional and 7-day global scales.

---

## 8. Infrastructure

### 8.1 Docker Compose services (current)

| Service | Image | Port | Purpose |
|---|---|---|---|
| `etl4-kafka` | `apache/kafka:3.8.0` | 9092 | Message bus (KRaft mode, no ZooKeeper) |
| `etl4-postgres` | `postgres:16` | 5432 | Metrics, evaluation results, pipeline state |
| `etl4-prometheus` | `prom/prometheus:latest` | 9090 | Time-series metric collection |
| `etl4-grafana` | `grafana/grafana:latest` | 3000 | Dashboard visualisation |

### 8.2 Services to add

| Service | When | Purpose |
|---|---|---|
| Spark master + 2 workers (3.5) | Week 2 | Clean, feature extraction, reconstruction, correction |
| Spark history server | Week 2 | Inspect completed job DAGs |
| Airflow 3 | Week 4 | DAG orchestration of all pipeline stages |
| node-exporter | Week 2 | Host CPU, RAM, disk, network |
| Kafka JMX exporter | Week 2 | Topic offsets, consumer lag |

### 8.3 Kafka topics

| Topic | Partitions | Replication | Key | Purpose |
|---|---|---|---|---|
| `adsb.raw` | 3 (local) / 12 (cloud) | 1 | `icao24` | All inbound state vectors |
| `adsb.dlq` | 1 | 1 | `icao24` | Malformed, out-of-range, and late records |

### 8.4 Prometheus scrape targets

Current: Prometheus self-scrape only.

To add (`infra/prometheus.yml`):

| Target | Port | What it exports |
|---|---|---|
| node-exporter | 9100 | CPU, RAM, disk, network |
| live_producer | 8001 | `live_records_sent_total`, `live_token_refreshes_total`, `live_active_aircraft` |
| replay_producer | 8000 | `replay_records_sent_total` |
| Spark master | 4040 | Job stages, executor metrics, **shuffle read/write bytes** |
| Kafka JMX exporter | 7071 | Topic offsets, consumer group lag |

### 8.5 PostgreSQL schema

```sql
CREATE TABLE pipeline_runs (
    run_id       SERIAL PRIMARY KEY,
    run_date     DATE,
    source       TEXT,           -- 'live' | 's3' | 'trino' | 'synthetic'
    method       TEXT,           -- 'time_gap' | 'kinematic' | 'kinematic_smoothing'
    stage        TEXT,
    rows_in      BIGINT,
    rows_out     BIGINT,
    wall_secs    FLOAT,
    cpu_secs     FLOAT,
    shuffle_mb   FLOAT,
    started_at   TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE eval_results (
    run_id       INT REFERENCES pipeline_runs(run_id),
    metric       TEXT,            -- 'segment_f1' | 'cross_track_rmse' | ...
    value        FLOAT,
    recorded_at  TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE deviation_events (
    event_id     SERIAL PRIMARY KEY,
    run_id       INT REFERENCES pipeline_runs(run_id),
    icao24       TEXT,
    flight_id    TEXT,
    start_time   BIGINT,
    end_time     BIGINT,
    type         TEXT,            -- 'cross_track' | 'altitude' | 'residual'
    max_distance FLOAT,
    peak_residual FLOAT
);
```

### 8.6 Environment variables (`.env`)

| Variable | Default | Purpose |
|---|---|---|
| `SOURCE` | `live` | Active source: `synthetic`, `live`, `trino` |
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Kafka broker |
| `POSTGRES_USER` | `etl4` | DB user |
| `POSTGRES_PASSWORD` | *(set)* | DB password |
| `POSTGRES_DB` | `etl4` | DB name |
| `GRAFANA_PASSWORD` | *(set)* | Grafana admin password |
| `OPENSKY_CLIENT_ID` | *(set)* | OAuth2 client ID |
| `OPENSKY_CLIENT_SECRET` | *(set)* | OAuth2 client secret |

---

## 9. Team Ownership

Based on the proposal, no formal per-member responsibility split is prescribed. Working assignments below are tentative and open to discussion. **All schema changes require sign-off from all four members** (recorded in `docs/decisions.md`).

| Area | Tentative owner | Status |
|---|---|---|
| Ingest, replay, infrastructure, benchmarks | Kompalli Avinash Bhargav | Primary — see Current Status |
| Spark cleaning, event-time ordering, deduplication | TBD | Not started |
| Kinematic feature extraction + Methods A/B | TBD | Not started |
| Kalman filter (Method C), deviation detection, correction | TBD | Not started |
| Reference-path construction, airports lookup | TBD | Not started |
| Evaluation metrics, dashboards, final report | TBD (all) | Not started |

---

## 10. Current Status

### Done

| Item | Notes |
|---|---|
| `screpper.py` | S3 download with dry-run, date range, correct `.csv.tar` handling |
| `live_producer.py` | OAuth2, Kafka producer, DLQ, bounding box, rate-limiting — **keeps ALL 17 raw fields** |
| `synthetic.py` | 20 aircraft, controlled noise, ground truth output |
| `replay_producer.py` | TODOs complete: time-controlled sleep, JSON serialisation, Kafka produce, Prometheus counter |
| `ingest_opensky_day.py` | TODOs complete; dormant (Trino rejected) |
| `clean/clean.py` | `project_raw()` for REST/S3/synthetic sources + dedup + range checks + position-jump flag |
| `clean/features.py` | Event-time ordering + kinematic features (dt, dist_m, speed_est, accel, turn_rate, alt_rate) |
| `reconstruct/time_gap.py` | **Method A** — time-gap baseline |
| `reconstruct/kinematic.py` | **Method B** — kinematic reconstruction (gap + jump + alt + turn + callsign breaks) |
| `reconstruct/kalman.py` | **Method C** — 6-state constant-velocity Kalman filter in ENU; residual-based deviation signal |
| `reconstruct/reference.py` | Great-circle route (slerp), nearest-airport lookup, cross-track distance |
| `reconstruct/deviations.py` | Cross-track + residual event detection, min-duration filtering |
| `reconstruct/correction.py` | Kalman-based outlier replacement + short-gap interpolation |
| `evaluate/metrics.py` | Position RMSE, segmentation P/R/F1, boundary error, coverage, continuity, deviation P/R/F1 |
| `bench/noise.py` | N0–N3 noise profiles (gaps, duplicates, out-of-order, jumps) |
| `bench/run_experiment.py` | Single-run harness: (bronze × method × noise) → metrics row → `data/eval/runs.csv` |
| `bench/run_matrix.py` | Full 3-method × 4-noise matrix driver → `data/eval/matrix.csv` |
| `run_pipeline.py` | Top-level orchestrator |
| `data/reference/airports.csv` | Seed list: 30 Indian + 19 major world airports |
| `infra/create_tables.sql` | PostgreSQL DDL for `pipeline_runs`, `eval_results`, `deviation_events` |
| `infra/docker-compose.yml` | Added Spark master + 2 workers + node-exporter |
| `infra/prometheus.yml` | Added node, live_producer, replay_producer, Spark scrape targets |
| `tests/test_pipeline.py` | 17 tests covering clean, features, 3 methods, reference, deviations, metrics, end-to-end — **all passing** |
| `.env` + pyopensky config | OAuth2 credentials set |
| `docs/decisions.md` | Schema change log started |

### Verified end-to-end on synthetic data (2 h, 20 aircraft, 14 025 rows)

Full 3×4 matrix (3 methods × 4 noise profiles) completed in 85 s wall time. Headline accuracy-vs-cost signal is clear:
- **time_gap**: 2.5× faster but always 20 flights → under-segments
- **kinematic**: catches 86 flights at N0, 331 at N3 (more fragmentation under noise)
- **kinematic_smoothing**: ~70% overhead on top of kinematic

### Remaining work

| Item | Blocker |
|---|---|
| Docker Desktop install | User installing — pending |
| Spin up Compose stack | Needs Docker |
| Create Postgres tables | Needs Docker running |
| Promote pandas pipeline to Spark DataFrame API | Needs Spark container — API 1:1 compatible |
| Add Prometheus metrics inside clean/features/reconstruct stages | Low priority — producers already instrumented |
| Airflow 3 DAG wiring | Later — current `run_pipeline.py` is sufficient for experiments |
| Grafana panels | Needs Docker + Prometheus data |
| Scale run E2 (1-day India from S3) | Needs ~5 GB S3 download via screpper |
| Scale run E4 (7-day global from S3) | Needs ~20 GB S3 download via screpper |
| Final report | Depends on E2/E4 numbers |

The pandas pipeline is complete and fully tested. Moving to Spark + Kafka is a mechanical rewrite — all transformations are already pure DataFrame functions.

---

## 11. Detailed TODOs

### 11.1 `ingest/replay_producer.py`

```python
# TODO 1 — time-controlled sleep
target = wall_start + (row.time - t_first) / speed
time.sleep(max(0.0, target - time.time()))

# TODO 2 — JSON-safe serialisation
import math
d = row._asdict()
safe = {k: (None if isinstance(v, float) and math.isnan(v) else v) for k, v in d.items()}
value = json.dumps(safe)

# TODO 3 — produce to Kafka
producer.produce(topic, key=row.icao24, value=value)
producer.poll(0)
sent += 1

# TODO 4 — Prometheus counter
from prometheus_client import Counter, start_http_server
records_sent = Counter("replay_records_sent_total", "Records produced to Kafka")
start_http_server(8000)   # before the main loop
# inside loop:
records_sent.inc()
```

### 11.2 `ingest/ingest_opensky_day.py`

```python
# TODO 1 — fetch one hour from Trino
df = trino.history(start, stop, bounds=bounds)
# Rename columns to match COLUMNS if needed

# TODO 2 — print counts
print(f"hour {h}: rows={len(df):,}  icao24={df['icao24'].nunique():,}")

# TODO 3 — write Parquet
out.parent.mkdir(parents=True, exist_ok=True)
df.assign(hour=int(start.timestamp())).to_parquet(out, index=False)
```

### 11.3 `infra/prometheus.yml` — add scrape targets

```yaml
scrape_configs:
  - job_name: node
    static_configs: [{ targets: ["node-exporter:9100"] }]
  - job_name: live_producer
    static_configs: [{ targets: ["host.docker.internal:8001"] }]
  - job_name: replay_producer
    static_configs: [{ targets: ["host.docker.internal:8000"] }]
  - job_name: spark
    static_configs: [{ targets: ["spark-master:4040"] }]
```

### 11.4 `infra/docker-compose.yml` — add services

```yaml
  node-exporter:
    image: prom/node-exporter:latest
    ports: ["9100:9100"]

  spark-master:
    image: bitnami/spark:3.5
    ports: ["8080:8080", "4040:4040", "7077:7077"]
    environment: { SPARK_MODE: master }

  spark-worker-1:
    image: bitnami/spark:3.5
    environment:
      SPARK_MODE: worker
      SPARK_MASTER_URL: spark://spark-master:7077
    depends_on: [spark-master]

  spark-worker-2:
    image: bitnami/spark:3.5
    environment:
      SPARK_MODE: worker
      SPARK_MASTER_URL: spark://spark-master:7077
    depends_on: [spark-master]
```

### 11.5 PostgreSQL setup

```bash
docker compose up -d postgres
docker exec -i etl4-postgres psql -U etl4 -d etl4 < infra/create_tables.sql
```

Create `infra/create_tables.sql` from the DDL in Section 8.5.

### 11.6 `live_producer.py` — add Prometheus metrics

```python
from prometheus_client import Counter, Gauge, start_http_server
records_sent    = Counter("live_records_sent_total",    "Records sent to adsb.raw")
records_dropped = Counter("live_records_dropped_total", "Records sent to DLQ")
token_refreshes = Counter("live_token_refreshes_total", "OAuth2 refreshes")
active_aircraft = Gauge  ("live_active_aircraft",       "Aircraft in last poll")
start_http_server(8001)  # before the main loop
```

### 11.7 Kalman filter module (`reconstruct/kalman.py`)

Skeleton:
```python
class KalmanTrajectoryFilter:
    def __init__(self, dt, process_noise=1.0, measurement_noise=10.0): ...
    def predict(self): ...
    def update(self, measurement): ...
    def residual(self, measurement) -> float: ...
```

### 11.8 Reference path helpers (`reconstruct/reference.py`)

```python
def great_circle_route(dep_lat, dep_lon, arr_lat, arr_lon, n_points=100): ...
def nearest_airport(lat, lon, airports_df) -> str: ...   # ICAO code
def cross_track_distance(point, route_segment) -> float: ...   # metres
```

Airports reference list: `data/reference/airports.csv` (ICAO code, lat, lon, name) — one-time download from OurAirports or OpenFlights.

---

## 12. Experiment Plan

Experiments progressively increase dataset size from a single day to multiple days or weeks of ADS-B observations. Each method (Section 7) runs under identical input conditions.

### 12.1 Scale experiments

| Run | Dataset | Region | Expected rows | Purpose |
|---|---|---|---|---|
| E1 | 1 day, synthetic (20 aircraft) | — | ~7 M | End-to-end sanity, baseline for RMSE |
| E2 | 1 day, S3 historical | **India** (68,8,97,37) | ~5–10 M | Regional baseline |
| E3 | 1 day, S3 historical | Global | ~50–100 M | Daily global scale |
| E4 | 7 days, S3 historical | Global | ~400 M–1 B | Full scale run |
| E5 | Live stream | Global | continuous | Streaming latency measurement |

### 12.2 Controlled artificial-noise experiments

Inject noise into **synthetic** data and measure robustness. Noise profiles:

| Profile | Gaps | Duplicates | Out-of-order | Jumps |
|---|---|---|---|---|
| N0 (baseline) | 0 % | 0 % | 0 % | 0 % |
| N1 (light) | 2 % | 0.5 % | 1 % | 0.1 % |
| N2 (moderate) | 5 % | 1 % | 3 % | 0.5 % |
| N3 (heavy) | 10 % | 2 % | 10 % | 1 % |

Each profile runs through all three reconstruction methods and reports the full metric suite.

### 12.3 Cost benchmark

For each (dataset × method) combination, record:

- Wall-clock time (end-to-end)
- CPU-seconds (sum across Spark executors)
- GB processed
- Shuffle read/write bytes
- Peak memory per executor
- Scaling efficiency: speedup vs. executor count (1, 2, 4 workers)

---

## 13. Evaluation Metrics

### 13.1 System-level performance metrics

| Metric | Definition |
|---|---|
| **Throughput** | Records processed per second (per stage) |
| **End-to-end processing time** | Wall time from ingest to gold trajectory write |
| **Streaming latency** | p50 / p95 / p99 of (processing_time − event_time) |
| **CPU utilisation** | % CPU across Spark executors |
| **Memory utilisation** | Peak resident set size per executor |
| **Shuffle / partitioning overhead** | Shuffle read + write bytes per stage |
| **Scaling efficiency** | T(1 worker) / (N · T(N workers)) — ideal = 1 |

### 13.2 Feature / trajectory quality metrics

| Metric | Definition |
|---|---|
| **Flight segmentation precision** | TP / (TP + FP) of flight boundaries |
| **Flight segmentation recall** | TP / (TP + FN) of flight boundaries |
| **Flight segmentation F1** | Harmonic mean of precision and recall |
| **Flight boundary time error** | Mean \|t_predicted_boundary − t_true_boundary\| (seconds) |
| **% correctly matched flights** | Reconstructed flights that match a reference flight within tolerance |
| **Trajectory coverage** | Reconstructed length / reference length |
| **Trajectory continuity** | 1 − (number of unexpected breaks) / (total expected points) |
| **Position/path reconstruction error** | RMSE of (lat, lon) at observed timestamps |
| **Cross-track RMSE** | RMSE of perpendicular distance from reference path |
| **Deviation detection precision** | TP / (TP + FP) of flagged deviation events |
| **Deviation detection recall** | TP / (TP + FN) of flagged deviation events |
| **Robustness** | Ratio of accuracy under noise profile Nk vs. baseline N0 |

### 13.3 Headline result (what the final report must show)

A table comparing all three methods on E2 (1-day regional) and E4 (7-day global), each cell containing: **wall time**, **segmentation F1**, **cross-track RMSE**, **deviation detection F1**, **shuffle GB**. This directly answers the central research question.

---

## 14. Timeline / Milestones

Project proposal mandates **8 weeks**:

| Week | Milestone (from proposal) | Implementation deliverables |
|---|---|---|
| **1** | Data acquisition, schema analysis, baseline pipeline | ✅ screpper.py, live_producer.py, synthetic.py done. Finish replay_producer.py TODOs. Confirm Docker stack. |
| **2** | Kafka ingestion and Spark preprocessing | Add Spark + node-exporter to Compose. Implement ingest_opensky_day.py. Wire Prometheus metrics. First Spark job reads bronze and writes silver. |
| **3** | Cleaning, deduplication, event-time ordering | Spark Structured Streaming job: schema enforcement, dedup, watermark, event-time sort. Emit `clean_*` metrics. |
| **4** | Time-gap and kinematic reconstruction methods | Implement Methods A and B. Compute kinematic features (Section 6.4). Compare on E1 (synthetic). |
| **5** | Smoothing/state estimation and trajectory deviation detection | Implement Kalman filter (Method C). Cross-track distance + residual-based deviation detection. |
| **6** | Candidate path correction and reference comparison | Reference-path builder (great-circle, airports lookup). Candidate correction producer. Deviation events written to `data/gold/deviations/`. |
| **7** | Large-scale experiments and performance evaluation | Run E2, E3, E4. Collect benchmark table (wall time, F1, RMSE, shuffle GB). |
| **8** | Final analysis, visualization, report, presentation | Grafana dashboard final layout. Write project report. Prepare presentation slides. |

---

## 15. Open Questions

| # | Question | Decision needed by | Owner |
|---|---|---|---|
| Q1 | Regional bounding box for E2? Recommend India (68, 8, 97, 37). | Week 2 | All |
| Q2 | 7-day window for E4? Recommend 2022-06-20 to 2022-06-26 (last full week in public S3). | Week 2 | All |
| Q3 | Spark 3.5 or 4.0? Recommend pinning 3.5 now; upgrade only if a 4.0-only feature is needed. | Week 1 | All |
| Q4 | Time-gap threshold for Method A? Common defaults: 30 min. Confirm. | Week 3 | Reconstruction owner |
| Q5 | Kalman process-noise `Q` and measurement-noise `R` initial values? Tune on synthetic first. | Week 5 | Smoothing owner |
| Q6 | Gap-fill ceiling: interpolate ≤ 120 s or ≤ 60 s? Affects RMSE and output size. | Week 3 | Clean owner |
| Q7 | Airflow 3 local executor or Celery for the lab submission? Local is simpler. | Week 4 | DAG owner |
| Q8 | Where to source airports reference list? OurAirports CSV ok? | Week 6 | Reference owner |

---

## 16. How to Run

### Prerequisites

```bash
conda create -n opensky python=3.12 -y
conda activate opensky
pip install -r etl4/requirements.txt
cp etl4/.env.example etl4/.env   # credentials already written to .env
```

### Start the infrastructure stack

```bash
docker compose -f etl4/infra/docker-compose.yml up -d
bash etl4/infra/create_topics.sh
```

### Verify with synthetic data

```bash
python etl4/ingest/synthetic.py --hours 1
pytest etl4/tests/ -q
```

### Live ingest to Kafka

```bash
python etl4/ingest/live_producer.py                            # global, 10 s
python etl4/ingest/live_producer.py --bounds 68,8,97,37 --interval 5   # India
python etl4/ingest/live_producer.py --limit 1000               # test run
```

### Download historical S3 data

```bash
python etl4/screpper.py --download --dry-run                   # size estimate
python etl4/screpper.py --download                             # all available (~279 GB)
python etl4/screpper.py --download --start-date 2022-06-20 --end-date 2022-06-26
python etl4/screpper.py --convert  --start-date 2022-06-20 --end-date 2022-06-26
```

### Replay bronze Parquet into Kafka

```bash
# Implement TODOs in replay_producer.py first, then:
python etl4/ingest/replay_producer.py --speed 10
```

### Trino historical ingest

```bash
# SOURCE=trino in .env; TODOs implemented in ingest_opensky_day.py
python etl4/ingest/ingest_opensky_day.py --date 2022-06-27
```

### Access services

| Service | URL | Credentials |
|---|---|---|
| Grafana | http://localhost:3000 | admin / (see `.env` `GRAFANA_PASSWORD`) |
| Prometheus | http://localhost:9090 | none |
| Spark UI | http://localhost:8080 | none (after Spark added) |
| Spark Job UI | http://localhost:4040 | none (while a job runs) |

---

## 17. Testing Strategy

### Current

| Test | File | Checks |
|---|---|---|
| Synthetic bronze end-to-end | `tests/test_smoke.py` | Parquet written, schema correct, 20 unique aircraft, lat ∈ [−90, 90] |

### To add

| Test | Scope | Priority |
|---|---|---|
| `test_live_producer_schema` | Mock REST response → assert all COLUMNS in Kafka message | High |
| `test_replay_producer_speed` | Replay at 10× → assert records arrive in event-time order | High |
| `test_clean_dedup` | Duplicates in → zero duplicates out | High |
| `test_clean_position_jump` | 500 km/min row → flagged | High |
| `test_event_time_order` | Shuffled rows in → monotonic out, late rows → DLQ | High |
| `test_kinematic_features` | Known input → check `dt`, `dist_m`, `speed_est`, `turn_rate` | High |
| `test_method_a_gap` | 35-min gap → segmentation break | High |
| `test_method_b_jump` | Position-impossible row → segmentation break | High |
| `test_kalman_converges` | Noisy straight line → smoothed path within 0.001° RMSE | High |
| `test_kalman_residual` | Inject spike → residual > threshold → deviation flagged | High |
| `test_cross_track_distance` | Known route and offset point → correct metres | Medium |
| `test_deviation_precision_recall` | Synthetic truth events → precision + recall ≥ 0.9 | Medium |
| `test_correction_fills_gap` | 60-s gap → corrected points marked `corrected=True` | Medium |
| `test_scraper_tar_reader` | Real `.csv.tar` → DataFrame matches schema | Medium |

Run all tests:
```bash
pytest etl4/tests/ -v
```

---

## 18. Monitoring and Observability

### Prometheus metrics plan

| Metric | Type | Emitter | Purpose |
|---|---|---|---|
| `live_records_sent_total` | Counter | live_producer | Ingest throughput |
| `live_records_dropped_total` | Counter | live_producer | DLQ rate |
| `live_token_refreshes_total` | Counter | live_producer | Auth health |
| `live_active_aircraft` | Gauge | live_producer | Real-time aircraft count |
| `replay_records_sent_total` | Counter | replay_producer | Replay throughput |
| `clean_rows_in_total` | Counter | clean job | Raw rows entering |
| `clean_rows_out_total` | Counter | clean job | Clean rows written |
| `clean_duplicates_dropped_total` | Counter | clean job | Dedup effectiveness |
| `clean_position_jumps_flagged_total` | Counter | clean job | Noise prevalence |
| `late_records_total` | Counter | event-time stage | Out-of-order arrival rate |
| `kinematic_features_computed_total` | Counter | feature stage | Feature throughput |
| `reconstruction_segments_total{method}` | Counter | reconstruction | Segment count per method |
| `kalman_residual_bucket` | Histogram | Kalman filter | Residual distribution |
| `deviation_events_total{type}` | Counter | deviation detector | Deviation frequency by type |
| `corrections_applied_total` | Counter | correction stage | How often correction kicks in |
| `pipeline_stage_seconds` | Histogram | all stages | End-to-end latency |
| `spark_shuffle_bytes` | Gauge | Spark exporter | Shuffle overhead |

### Grafana dashboard panels

1. Ingest throughput — `rate(live_records_sent_total[1m])`
2. Kafka consumer lag — per group, per topic
3. DLQ rate — `rate(live_records_dropped_total[1m])`
4. Clean ratio — `clean_rows_out_total / clean_rows_in_total`
5. Active aircraft (live) — `live_active_aircraft`
6. Token refresh health — `rate(live_token_refreshes_total[1h])`
7. Kalman residual distribution — histogram heatmap
8. Deviation events by type — `rate(deviation_events_total[5m]) by (type)`
9. Pipeline stage latency — `histogram_quantile(0.95, pipeline_stage_seconds)`
10. Shuffle bytes per stage — benchmark comparison
11. Scaling efficiency chart — speedup vs. workers (populated after E2/E4 runs)
12. Node CPU / RAM / disk — from node-exporter

---

## 19. Known Constraints and Risks

| Risk | Likelihood | Mitigation |
|---|---|---|
| OpenSky API rate limit hit | Medium | `--interval 10` anonymous or 5 with credentials; monitor DLQ |
| S3 download fills disk (~279 GB) | High | Always `--dry-run` first; use `--start-date/--end-date` for subsets |
| Trino access not approved in time | Medium | Fallback: S3 historical via screpper.py; live API via live_producer |
| Spark memory exhaustion on E4 (7-day global) | Medium | Partition by day; process one day at a time; tune `spark.executor.memory` |
| Kalman filter divergence on sparse tracks | Medium | Reinitialise on long gaps; cap residual before use |
| `icao24` type corruption | Low (fixed) | Schema enforces `dtype="string"` everywhere |
| OpenSky credentials exposed in chat | Mitigated | Stored in gitignored `.env` and pyopensky settings; rotate at opensky-network.org if needed |
| Schema drift across team members | Low | All changes logged in `docs/decisions.md`; four-member sign-off required |
| `replay_producer.py` TODOs blocking downstream testing | High | Finish Week 1 (first priority) |
| Reference-path source unclear | Medium | Fall back chain in Section 6.6; synthetic ground truth always available for N0–N3 experiments |

---

## 20. References

Taken from the project proposal (`ETL4 (3) (1).pdf`).

1. M. Schäfer, M. Strohmeier, V. Lenders, I. Martinovic, and M. Wilhelm, "Bringing Up OpenSky: A Large-scale ADS-B Sensor Network for Research," in *Proc. 13th International Symposium on Information Processing in Sensor Networks (IPSN)*, 2014, pp. 83–94.
2. M. Strohmeier, M. Schäfer, R. Hans, and V. Lenders, "Crowdsourced Air Traffic Data from the OpenSky Network 2019–2020," *Earth System Science Data*, vol. 13, pp. 357–366, 2021.
3. X. Olive and C. Basora, "Detection and Identification of Significant Events in Historical Aircraft Trajectory Data," *Transportation Research Part C*, vol. 119, 102737, 2020.
4. M. Pretto et al., "Exploiting High-resolution ADS-B Data for Flight Operation Reconstruction Towards Environmental Impact Assessment," *Journal of Open Aviation Science*, vol. 1, no. 2, 2023.
5. OpenSky Network, "OpenSky REST API Documentation," 2026. https://openskynetwork.github.io/opensky-api/rest.html
6. OpenSky Network, "OpenSky Scientific Datasets," 2026. https://opensky-network.org/data/scientific
