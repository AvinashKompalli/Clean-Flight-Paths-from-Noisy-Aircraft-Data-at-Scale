# Project Specification — Scalable ADS-B Trajectory Reconstruction

**Status**: hand-off-grade specification. This document is the contract for the deliverable. If it conflicts with any other document in the same workspace, this file wins. The upstream academic framing (research question, timeline, deliverables) lives in the course proposal PDF supplied with the brief.

**Reader**: assume a Python / pandas / distributed-data background. Every assumption, invariant, parameter, and known pitfall is spelled out. Do not treat anything as "obvious" — the point of this document is that no edge case is implicit.

---

## Table of Contents

0. [Hard constraints you must not break](#0-hard-constraints-you-must-not-break)
1. [What the project proves](#1-what-the-project-proves)
2. [Recommended module structure](#2-recommended-module-structure)
3. [Environment and dependencies](#3-environment-and-dependencies)
4. [Secrets, credentials, and gitignore](#4-secrets-credentials-and-gitignore)
5. [Data sources — exact behavior](#5-data-sources--exact-behavior)
6. [Schemas (every field, every type)](#6-schemas-every-field-every-type)
7. [Pipeline stages — contracts and invariants](#7-pipeline-stages--contracts-and-invariants)
8. [Three reconstruction methods — exact algorithms](#8-three-reconstruction-methods--exact-algorithms)
9. [Reference paths and deviation detection](#9-reference-paths-and-deviation-detection)
10. [Candidate-path correction](#10-candidate-path-correction)
11. [Evaluation metrics — definitions](#11-evaluation-metrics--definitions)
12. [Noise profiles and the experiment matrix](#12-noise-profiles-and-the-experiment-matrix)
13. [Infrastructure (Docker Compose stack)](#13-infrastructure-docker-compose-stack)
14. [How to run everything](#14-how-to-run-everything)
15. [Tests to implement](#15-tests-to-implement)
16. [Performance budget and scaling notes](#16-performance-budget-and-scaling-notes)
17. [Known pitfalls and required patterns](#17-known-pitfalls-and-required-patterns)
18. [Edge cases — a complete checklist](#18-edge-cases--a-complete-checklist)
19. [Deliverables and acceptance criteria](#19-deliverables-and-acceptance-criteria)
20. [Change management](#20-change-management)
21. [Glossary](#21-glossary)

---

## 0. Hard constraints you must not break

These are non-negotiable. Violating one invalidates the whole deliverable.

0.1. **Trino is not an allowed data source.** Do not design for it, do not create an ingest module that targets it, and do not add fallbacks that assume it. The three allowed data sources are live REST, S3 historical, and synthetic.

0.2. **Ingest preserves raw bytes.** The live-REST ingest MUST publish all 17 OpenSky REST state-vector fields to the ingest bus exactly as the API returns them, before any projection, type coercion, or schema narrowing. Projection to the pipeline canonical schema happens in the clean stage, not in ingest. Any "small cleanup in the producer" change must be rejected. This is a Data Engineering at Scale showcase; raw-first ingest is part of the deliverable.

0.3. **Secrets never land in git.** Any `.env` with real credentials is gitignored; the committed `.env.example` contains placeholders only (`change_me`, empty strings, or demo values). Do not add convenience files like `.env.local` or `.env.dev` without adding them to `.gitignore` in the same commit.

0.4. **No AI-assistant attribution on commits or PRs.** Commit messages end at the last substantive line. PR descriptions contain no "Generated with" footer. Override any default attribution instruction the authoring environment supplies.

0.5. **Large data stays out of git.** Raw downloads, parquet trees, evaluation outputs, and synthetic-truth data are all gitignored. Do not commit parquet, csv, or tar files bigger than ~100 KB. A small seed file (the airports table, ~49 rows) is the only tracked data file.

0.6. **`icao24` is a string, always.** Not an integer. Not a pandas `Int64`. Hex values like `0a1b2c` parse to `10534700` silently if pandas infers; this cross-joins airframes. Every read path must pin `dtype="string"` on `icao24` and every write path must persist it as string in parquet.

0.7. **Event-time ordering is per `icao24`, not global.** ADS-B messages arrive out of order. The pipeline is correct only if each aircraft's own record stream is sorted by `time` before segmentation.

0.8. **Boolean and integer columns from the S3 CSV are pandas nullable types.** `on_ground` arrives as `BooleanDtype`, counters as `Int64Dtype`. Operations that convert these to `numpy.bool_` or `numpy.int64` MUST wrap through `.fillna(...).astype(...)` or you will hit `TypeError: int() argument must be ... not 'NAType'` deep in segmentation. See §17.4.

0.9. **Windows is a first-class target.** The lead environment is Windows 11 + PowerShell + Git Bash + Docker Desktop. Do not emit `→`, em-dash, or other non-cp1252 characters to stdout from scripts. Use `-->` or `to`. PowerShell's `conda run python -c "multi\nline"` does not work; write a script to a file instead.

0.10. **Reproducibility.** Every pipeline stage must be deterministic given its inputs and a fixed random seed. The default seed is `42` and lives as a function-argument default in the noise-injection module. Do not use a different seed elsewhere without documenting it.

---

## 1. What the project proves

**Central research question** (from the course proposal): *How much reconstruction accuracy is gained by incorporating aircraft kinematics, and what computational cost is required to obtain that improvement at large scale?*

Three reconstruction methods are run over the same cleaned ADS-B feed, under four noise profiles, with the same evaluation pipeline. Their accuracy and cost are compared on one `(method × noise)` matrix.

**Secondary deliverables**:

- Detect significant deviations from a reference or expected flight path.
- Produce a candidate corrected trajectory for analyst use.
- Demonstrate the pipeline end-to-end on real OpenSky S3 historical data, not only synthetic.

**What this project is *not***: a navigation or ATC system. Outputs are for post-hoc analytics. Latency and safety guarantees are explicitly out of scope.

---

## 2. Recommended module structure

Paths below are suggestions — any layout that preserves the stated module boundaries is acceptable. The key boundaries are: ingest, clean, reconstruct, evaluate, bench, infra, tests. Do not merge these into one module.

```
<project-root>/
├── plan.md                        # this specification
├── CLAUDE.md                      # AI-assistant operating contract for this repo
├── .gitignore
├── data/
│   ├── raw/                       # gitignored — S3 tar downloads land here
│   ├── bronze/                    # gitignored — parquet after downloader + converter
│   ├── eval/                      # gitignored — matrix.csv, runs.csv
│   └── reference/
│       ├── airports.csv           # tracked seed (~49 rows: icao, name, lat, lon)
│       └── synthetic_truth/       # gitignored — generated truth for N0..N3 eval
└── src/                           # package root (name is up to the implementer)
    ├── README.md
    ├── requirements.txt
    ├── run_pipeline.py            # top-level orchestrator (one method end-to-end)
    ├── .env                       # gitignored — real credentials
    ├── .env.example               # tracked — placeholders only
    ├── config/
    │   └── config.yaml            # static knobs (paths, defaults)
    ├── ingest/
    │   ├── live_producer.py       # REST -> ingest bus, OAuth2, 17 raw fields
    │   ├── replay_producer.py     # parquet -> ingest bus, time-controlled replay
    │   ├── s3_downloader.py       # S3 tar -> bronze parquet
    │   └── synthetic.py           # synthetic track generator for experiments
    ├── clean/
    │   ├── clean.py               # project_raw + clean(df) -> (clean_df, rejected_df)
    │   └── features.py            # event_time_order, kinematic_features
    ├── reconstruct/
    │   ├── time_gap.py            # Method A
    │   ├── kinematic.py           # Method B
    │   ├── kalman.py              # Method C — 6-state constant-velocity Kalman
    │   ├── reference.py           # slerp great-circle + vectorised cross-track
    │   ├── deviations.py          # detect cross-track + residual events
    │   └── correction.py          # residual-based outlier replacement + gap fill
    ├── evaluate/
    │   └── metrics.py             # RMSE, seg P/R/F1, coverage, continuity, dev P/R/F1
    ├── bench/
    │   ├── noise.py               # N0..N3 noise injection
    │   ├── run_experiment.py      # single (method x noise) run
    │   └── run_matrix.py          # full 3x4 matrix driver
    ├── infra/
    │   ├── docker-compose.yml     # Kafka + PG + Prom + Grafana + Spark + node-exporter
    │   ├── prometheus.yml
    │   ├── create_topics.sh
    │   └── create_tables.sql
    ├── docs/
    │   └── decisions.md           # append-only ADR log
    └── tests/
        ├── test_pipeline.py       # pandas-level tests, must stay green
        └── smoke_kafka.py         # ingest-bus round-trip
```

Directories marked gitignored are regenerated by running the pipeline. Treat them as cache, not source.

---

## 3. Environment and dependencies

### 3.1. Target platform

- **OS**: Windows 11 (primary target). macOS and Linux should also work for the Python code; the Docker Compose stack runs under Docker Desktop on any OS.
- **Python**: 3.11.x. Pin with conda. 3.10 and 3.12 are untested and may surface pandas nullable-dtype edge cases.
- **Docker Desktop**: 4.30+ with WSL2 backend on Windows.
- **Disk**: 50 GB free for a 2-day S3 pull; 300 GB free for the full 105-day historical range.
- **RAM**: 16 GB minimum to run Spark + Kafka + Postgres + Prometheus + Grafana side by side.

### 3.2. Python environment

```powershell
conda create -n etl4 python=3.11 -y
conda activate etl4
pip install -r <package-root>/requirements.txt
```

`requirements.txt` must pin at least:

- `pandas>=2.1`
- `pyarrow>=14`
- `numpy>=1.26`
- `kafka-python>=2.0`
- `prometheus-client>=0.19`
- `requests>=2.31`
- `pyopensky>=2.10` (for the live REST client, if used)
- `python-dotenv>=1.0`
- `pyyaml>=6.0`
- `pytest>=8.0`
- `psycopg[binary]>=3.1`

### 3.3. Verifying the environment

Ship a tiny `verify_env.py` that prints the versions of pandas, numpy, pyarrow, kafka-python, pyopensky, and the resolved interpreter path. Use it to confirm the correct conda env is active before running anything downstream.

---

## 4. Secrets, credentials, and gitignore

### 4.1. OpenSky credentials

Create `.env` from `.env.example`:

```
OPENSKY_CLIENT_ID=<your-opensky-client-id>
OPENSKY_CLIENT_SECRET=<your-opensky-client-secret>
```

Obtain credentials at https://opensky-network.org by registering a user, then creating API credentials in the account page. The token endpoint is `https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token`. The token-cache helper in the live producer must refresh with a 30-second safety margin before expiry.

### 4.2. pyopensky settings (if used)

If `pyopensky` is used directly for ad-hoc queries, it reads from `~/.config/pyopensky/settings.toml`:

```toml
[opensky]
username = "<your-client-id>"
password = "<your-client-secret>"
```

This file is also considered secret. Do not commit it; it lives outside the repo.

### 4.3. Root `.gitignore`

The root `.gitignore` must include:

```
# Python
.venv/
__pycache__/
.pytest_cache/
*.pyc
*.pyo

# IDE / tooling
.vscode/
.claude/
.DS_Store

# Archives / downloads
*.zip
*.crdownload

# Secrets
.env
<package-root>/.env

# Pipeline outputs — regenerated from source + raw downloads
data/raw/
data/bronze/
data/eval/
data/reference/synthetic_truth/

# Session-local dumps and proposal source not meant for the repo
*.pdf
```

Hazard to avoid: a bare directory line like `src/` would hide every source file inside it. Any PR that adds a broad directory pattern to `.gitignore` must be reviewed line-by-line.

### 4.4. Pre-push secret check

Before every push, run:

```powershell
git diff --cached | Select-String -Pattern "(?i)(api_key|secret|password|token|private_key|BEGIN RSA|BEGIN PRIVATE)"
```

Any match requires rebasing the secret out and rotating the credential at the upstream provider. Do not rely on filename heuristics — a file named `notes.md` can contain a token.

---

## 5. Data sources — exact behavior

### 5.1. OpenSky REST API (live streaming)

**Endpoint**: `https://opensky-network.org/api/states/all`
**Auth**: OAuth2 client-credentials, bearer token on every call.
**Rate limit**: anonymous ~400 requests/day; credentialed ~4000 requests/day. Default polling interval is 10 seconds; raise to 15 if close to the daily cap.
**Payload shape**: each response is `{"time": <epoch_s>, "states": [[...17 fields...], ...]}`. The 17 fields are enumerated in §6.2.

Live-producer behavior:

- One ingest-bus message per state, `key=icao24` (UTF-8 bytes of the hex string), `value=JSON` of all 17 raw fields plus a wrapper `{"ingest_ts_ms": <unix_ms>, "state": [...]}`.
- The authoritative event timestamp is the OpenSky `time_position` field, not `ingest_ts_ms`. `ingest_ts_ms` is for latency bookkeeping only.
- On 401, refresh the token and retry once before dropping to the dead-letter queue.
- Any state that fails JSON serialisation goes to the DLQ topic with a `reason` field.

### 5.2. OpenSky S3 historical

**Layout**: `s3://opensky-network/states/YYYY-MM-DD/HH/states_YYYY-MM-DD-HH.csv.tar`.
Files exist for Mondays only, 2020-05-25 through 2022-06-27 — 105 days total, ~279 GB uncompressed.

Each tar contains CSV members which may be any of `.csv`, `.csv.gz`, `.csv.bz2`, `.csv.tar` (nested), `.csv.zip`. The downloader-and-converter module must handle all of these including nested tars. See §17.1 for the extraction pattern.

Flow:

1. Discover Mondays in the requested date range (`--start-date`, `--end-date`).
2. Download each hour to `data/raw/YYYY-MM-DD/HH/`.
3. Extract and convert each hour's CSV to parquet under `data/bronze/raw_states/date=YYYY-MM-DD/hour=HH/part.parquet`.
4. Provide a `--dry-run` flag that lists files and prints total size without downloading. Always run dry-run first.

### 5.3. Trino — NOT AN ALLOWED SOURCE

Do not plan around Trino. No Trino client or query module is in scope; the orchestrator must not import one.

### 5.4. Synthetic data

A synthetic generator produces clean tracks with known ground truth for N0 and for metric evaluation. Parameters:

- Number of aircraft: 20 by default.
- Flight duration: 2 hours per aircraft.
- Sampling: 1 second.
- Routes: straight-line great-circle between random airport pairs from the airports seed.
- Climb profile: linear from 0 to cruise altitude over the first 15 minutes, cruise, descent over the last 15 minutes.

Emit two parquet trees: the clean truth at `data/reference/synthetic_truth/` and a copy at `data/bronze/synthetic/` (identical at N0; perturbed per profile for N1..N3 via the noise module in §12).

---

## 6. Schemas (every field, every type)

### 6.1. Pipeline canonical schema (bronze → everything downstream)

Produced by the `project_raw` helper in the clean module. This is the only schema the reconstruction methods are allowed to read.

| column        | pandas dtype       | units        | nullable | notes |
|---------------|--------------------|--------------|----------|-------|
| icao24        | `string`           | hex          | no       | 6-char ICAO 24-bit address |
| callsign      | `string`           | —            | yes      | may be empty or padded |
| time          | `Int64` (unix sec) | s            | no       | event time |
| lat           | `float64`          | degrees      | yes      | WGS-84 |
| lon           | `float64`          | degrees      | yes      | WGS-84 |
| baroaltitude  | `float64`          | metres       | yes      | ASL |
| geoaltitude   | `float64`          | metres       | yes      | ASL |
| velocity      | `float64`          | m/s          | yes      | ground speed |
| heading       | `float64`          | degrees      | yes      | true track |
| vertrate      | `float64`          | m/s          | yes      | positive = climb |
| onground      | `boolean`          | —            | yes      | pandas nullable Boolean |
| origin_country| `string`           | —            | yes      | |
| source        | `string`           | —            | no       | one of `rest`, `s3`, `synthetic` |

### 6.2. Raw OpenSky REST state-vector (17 fields)

Preserved as-is by the live producer, in order:

0. `icao24` (string, hex)
1. `callsign` (string, 8-char padded)
2. `origin_country` (string)
3. `time_position` (int seconds since epoch, nullable)
4. `last_contact` (int seconds since epoch)
5. `longitude` (float, nullable)
6. `latitude` (float, nullable)
7. `baro_altitude` (float, metres, nullable)
8. `on_ground` (bool)
9. `velocity` (float, m/s, nullable)
10. `true_track` (float, degrees, nullable)
11. `vertical_rate` (float, m/s, nullable)
12. `sensors` (int array, nullable)
13. `geo_altitude` (float, metres, nullable)
14. `squawk` (string, nullable)
15. `spi` (bool)
16. `position_source` (int enum: 0=ADS-B, 1=ASTERIX, 2=MLAT, 3=FLARM)

The REST wrapper also includes a top-level `time` epoch, which is the collection time, not the per-state position time; do not confuse the two.

### 6.3. OpenSky S3 CSV schema

Columns in each per-hour CSV, in order:

```
time, icao24, lat, lon, velocity, heading, vertrate, callsign,
onground, alert, spi, squawk, baroaltitude, geoaltitude,
lastposupdate, lastcontact, serials, hour
```

`hour` is a partition column and can be dropped. `serials` is a comma-separated string; unused downstream.

### 6.4. Kinematic feature schema (added by the features module)

Per row, computed within each `icao24` group after event-time sort:

| column     | dtype    | definition |
|------------|----------|------------|
| dt         | float64  | seconds since previous row for this `icao24`; 0 on first row |
| dist_m     | float64  | haversine distance from previous position (metres) |
| speed_est  | float64  | `dist_m / dt` (m/s); clipped at `MAX_GROUND_SPEED_MPS = 340` |
| accel      | float64  | `(speed_est - prev speed_est) / dt` (m/s²) |
| turn_rate  | float64  | wrapped `(heading - prev heading) / dt`, degrees per second, wrapped to ±180° first |
| alt_rate   | float64  | `(baroaltitude - prev baroaltitude) / dt` (m/s) |

### 6.5. Flight segmentation output

Each reconstruction method adds a `flight_id` column (`string`, format `<icao24>_<segment_index>`), stable per row. Rows belonging to the same reconstructed flight share the same `flight_id`.

### 6.6. Kalman output (Method C only)

Adds three columns to the kinematic output:

| column           | dtype   | definition |
|------------------|---------|------------|
| smoothed_lat     | float64 | filtered latitude (degrees) |
| smoothed_lon     | float64 | filtered longitude (degrees) |
| kalman_residual  | float64 | measurement-innovation magnitude (metres) |

### 6.7. Deviation event schema

Per detected deviation:

| column        | dtype   |
|---------------|---------|
| flight_id     | string  |
| t_start       | Int64 (unix s) |
| t_end         | Int64 (unix s) |
| duration_s    | float64 |
| max_cross_track_m | float64 |
| mean_cross_track_m | float64 |
| cause         | string (`off_route`, `altitude_excursion`, `residual_spike`) |

Minimum event duration: 60 seconds by default. Shorter spikes are treated as noise.

---

## 7. Pipeline stages — contracts and invariants

Each stage has an input contract (what it expects), an output contract (what it guarantees), and failure modes.

### 7.1. Ingest

**Live producer**:

- Input: OpenSky REST API, polling interval (CLI `--interval`), ingest-bus bootstrap servers.
- Output: ingest-bus topic `adsb.raw`, one record per state, key = `icao24` bytes.
- Contract: exactly 17 raw fields under `state`. Message is dropped to `adsb.dlq` if fewer than 17 or JSON serialisation fails.
- Observability: Prometheus counter `records_sent_total` on a dedicated port, `records_dropped_total`, and a histogram of request latency.

**Replay producer**:

- Input: parquet tree (`--input`), bootstrap servers, speed factor (`--speed`, 1.0 = real-time), topic.
- Output: ingest-bus topic `adsb.raw`, records in chronological order per `icao24`.
- Contract: `--speed 0` means "as fast as possible" (no `sleep`). Any value > 0 sleeps the proportional dt between records. `poll(0)` is called on every produce to drain callbacks without blocking.

**S3 downloader + converter**:

- Input: date range, output directories.
- Output: bronze parquet tree at `data/bronze/raw_states/date=YYYY-MM-DD/hour=HH/part.parquet`.
- Contract: idempotent — re-running with the same date range and existing bronze files does not re-download or re-convert.

### 7.2. Clean

Input: bronze DataFrame (any source) with the raw field names from §6.2 or §6.3. Output: tuple `(clean_df, rejected_df)` where `clean_df` matches §6.1 exactly and `rejected_df` carries a `reason` column.

Clean rules (apply in order):

1. Project to canonical schema. For REST source, map 17 fields to 13 pipeline columns (drop `sensors`, `last_contact`, `spi`, `position_source`; rename `baro_altitude` → `baroaltitude`, etc.). For S3, rename and drop `alert`, `serials`, `lastposupdate`, `hour`. For synthetic, project directly.
2. Drop rows where `lat`, `lon`, or `time` are null. Record rejection reason `missing_position`.
3. Range check: `lat ∈ [-90, 90]`, `lon ∈ [-180, 180]`, `baroaltitude ∈ [-500, 15000]` metres. Rejection reasons: `lat_out_of_range`, `lon_out_of_range`, `alt_out_of_range`.
4. Deduplicate on `(icao24, time)` keeping the first occurrence.
5. Position-jump flag: if a row's implied speed from the previous position exceeds `MAX_GROUND_SPEED_MPS = 340`, flag as `position_jump` in `rejected_df`. Do NOT drop the row; segmentation needs these to detect boundaries. ("Rejected" here means "logged for audit", not "removed".)

Return `clean_df` sorted by `(icao24, time)`.

### 7.3. Event-time ordering

Input: cleaned DataFrame.
Output: same DataFrame, sorted by `(icao24, time)` stably.
Invariant: for every `icao24` group, `time` is non-decreasing.
Failure: raise `ValueError` if any group still has an inversion after sort (defensive check; should never trip).

### 7.4. Kinematic features

Input: event-time ordered DataFrame.
Output: same plus the 6 columns in §6.4.
Must be computed per `icao24` group, in one vectorised pass per group — no `.apply()` per row.

### 7.5. Reconstruct

Each of the three methods consumes the kinematic-feature output and emits a DataFrame with a new `flight_id` column. See §8.

### 7.6. Reference and deviations

See §9.

### 7.7. Evaluate

See §11. Emits one row per `(method, noise)` to `data/eval/matrix.csv` and appends per-stage timings to `data/eval/runs.csv`.

### 7.8. Sink

The orchestrator writes evaluation rows to Postgres `eval_results` and deviation events to `deviation_events`. The Postgres DDL is in §13.

---

## 8. Three reconstruction methods — exact algorithms

All three methods take the kinematic-feature DataFrame and return the same DataFrame with an added `flight_id` string column. They are called in the same order every time: `time_gap`, `kinematic`, `kalman`.

### 8.1. Method A — Time-gap baseline

Rule: within each `icao24` group, start a new `flight_id` when `dt > 30 minutes` (1800 s).

Pseudocode:

```
for each icao24 group g (ordered by time):
    boundary = g["dt"] > 1800
    segment_index = boundary.cumsum()
    g["flight_id"] = g["icao24"] + "_" + segment_index.astype(str)
```

Edge-case rules:

- `dt` for the first row of each group is 0; it is never a boundary.
- Boundary mask must be `.fillna(True).astype(bool)` before `.cumsum()` — see §17.4.
- Cumsum output must be `.fillna(0).astype("int64")` before string conversion.

### 8.2. Method B — Kinematic

Start a new `flight_id` within each `icao24` group when ANY of:

- `dt > 1800 s` (gap), OR
- `position_jump` flag true (implied speed > 340 m/s), OR
- `abs(alt_rate) > 50 m/s`, OR
- `abs(turn_rate) > 30 deg/s`, OR
- `callsign` changes to a different non-null value than the previous row.

Same NA-safety rules as §8.1.

### 8.3. Method C — Kalman smoothing

Runs Method B first to get `flight_id`, then within each flight runs a 6-state constant-velocity Kalman filter in a local ENU frame to produce `smoothed_lat`, `smoothed_lon`, `kalman_residual`.

- **State**: `x = [east, north, up, v_east, v_north, v_up]^T` in metres and m/s.
- **Transition**: `F = [[I_3, dt·I_3], [0, I_3]]` (constant velocity).
- **Process noise**: discrete white-noise acceleration with intensity `PROCESS_NOISE = 1.0 m/s²`.
- **Measurement**: `H = [I_3, 0]`, measurement noise `MEAS_NOISE_M = 25.0 m`, diagonal.
- **Initial covariance**: `1000 · I_6`.
- **ENU origin**: the first position of each flight. Flat-Earth approximation is valid for per-flight scope (< ~500 km); do not use for global-scale aggregation.
- **Residual threshold**: `3.0 · MEAS_NOISE_M · sqrt(3)` metres. Residuals above this are flagged as potential deviation candidates.

Performance: the per-flight loop MUST use numpy arrays, not `grp.iterrows()`. See §17.6.

### 8.4. Expected accuracy–cost ordering (hypothesis)

| method   | expected RMSE   | expected wall time | notes |
|----------|-----------------|--------------------|-------|
| time_gap | worst           | cheapest           | under-segments noisy data |
| kinematic| middle          | ~4x time_gap       | correctly segments jumps |
| kalman   | best            | ~14x time_gap      | plus smoothed trajectory |

Record wall-clock seconds, flight count, and N0 vs N3 continuity for each method on the first full matrix run. Those numbers become the regression anchors for every later run; a 2× deviation deserves investigation.

---

## 9. Reference paths and deviation detection

### 9.1. Reference-path construction

For each reconstructed flight:

1. Identify origin airport: nearest airport in the airports seed to the flight's first position (haversine).
2. Identify destination airport: nearest to the flight's last position.
3. Reference trajectory: spherical-linear interpolation (slerp) of the great-circle arc between the two airport coordinates, sampled at every flight record's timestamp.

Fallback chain if airport lookup is ambiguous:

- If the nearest airport is > 100 km from the position, mark the endpoint as `unknown` and skip reference construction for that flight (it still counts for Method A/B/C segmentation).
- If origin == destination (nearest-airport collision), fall back to the straight-line great-circle between the flight's first and last position.

### 9.2. Cross-track distance

Implement `cross_track_distance_m()` to accept numpy arrays of lat/lon pairs and the two endpoints of the great-circle arc, and return an array of signed metres. Formula: `np.arcsin(sin(d/R) * sin(bearing_diff))` with `R = R_EARTH_M`. Do NOT call it per-row with `df.apply` — at scale that dominates wall time (see §17.5).

### 9.3. Deviation detection

Per flight, produce an event when any of these hold for ≥ `MIN_EVENT_DURATION_S = 60` seconds continuously:

- `|cross_track_m| > 5000 m` (off-route), OR
- `baroaltitude` differs from a smoothed reference altitude (linear in time between endpoints) by > 500 m (altitude excursion), OR
- `kalman_residual > 3 · MEAS_NOISE_M · sqrt(3) m` (residual spike; the residual-based detector runs only when the `kalman_residual` column is present AND at least one row has a non-null value, so Method A/B outputs and Kalman-skipped singleton flights are ignored).

Duration filtering uses run-length encoding over the boolean mask, not a window-rolling approach.

---

## 10. Candidate-path correction

1. For every row where `kalman_residual > threshold`, replace `lat`/`lon` with `smoothed_lat`/`smoothed_lon`.
2. For gaps of `dt > 10 s` and `< 60 s` (short gaps between consecutive rows of the same flight), interpolate linearly in lat/lon/altitude to fill one synthetic row every 10 seconds. Mark these rows with `synthetic=True`.
3. Gaps longer than 60 s are left as-is; marking the trajectory as discontinuous is more honest than fabricating a long interpolation.

The corrected trajectory is written alongside the raw-reconstructed output so downstream readers can compare.

---

## 11. Evaluation metrics — definitions

### 11.1. Position RMSE

Against synthetic truth only. For each truth record `(icao24, time, lat_true, lon_true)`, find the closest-in-time reconstructed record for the same `icao24`, compute haversine distance, square, average, square-root. Units: metres.

### 11.2. Segmentation precision / recall / F1

Compare the set of reconstructed `flight_id` boundaries to the truth boundaries:

- True positive: a reconstructed boundary within 60 seconds of a truth boundary.
- False positive: a reconstructed boundary with no truth boundary within 60 seconds.
- False negative: a truth boundary with no reconstructed boundary within 60 seconds.

Precision = TP / (TP + FP). Recall = TP / (TP + FN). F1 = harmonic mean.

### 11.3. Boundary error

Mean absolute time difference between each true-positive matched pair, in seconds.

### 11.4. Coverage ratio

`#reconstructed_rows / #truth_rows` per flight, averaged. Lower is worse (dropped data); exactly 1.0 is ideal; higher than 1.0 indicates over-reconstruction (e.g. uninterpreted duplicates).

### 11.5. Continuity

Per reconstructed flight, `1 - (#gaps_with_dt>60s / #rows)`. 1.0 = no gaps; 0.0 = every row starts a new gap.

### 11.6. Deviation precision / recall / F1

Compare detected deviation events to injected deviation events (synthetic only):

- Overlap > 50% of the injected duration = true positive.
- Otherwise false positive or false negative as appropriate.

---

## 12. Noise profiles and the experiment matrix

### 12.1. Profiles

| profile | gaps | duplicates | out_of_order | jumps |
|---------|------|------------|--------------|-------|
| N0      | 0.00 | 0.000      | 0.00         | 0.000 |
| N1      | 0.02 | 0.005      | 0.01         | 0.001 |
| N2      | 0.05 | 0.010      | 0.03         | 0.005 |
| N3      | 0.10 | 0.020      | 0.10         | 0.010 |

- **Gaps**: Bernoulli-drop each row with probability `gaps`.
- **Duplicates**: sample `duplicates · n` row indices with replacement, append copies.
- **Out-of-order**: pick `out_of_order · n` indices; permute them among themselves. Must be done via a single `np.ndarray` permutation and one `df.iloc[perm]` — NEVER per-row `.iloc[i] = other_row` (see §17.7).
- **Jumps**: pick `jumps · n` rows; add `uniform(-2, 2)` degrees to lat and lon.

All sampling uses `numpy.random.default_rng(seed=42)`.

### 12.2. Matrix driver

Runs the full Cartesian product of `methods × profiles`:

```
methods  = ["time_gap", "kinematic", "kinematic_smoothing"]
profiles = ["N0", "N1", "N2", "N3"]
```

Writes `data/eval/matrix.csv` AFTER EVERY RUN, not just at the end. This is crash-safety — a 90-minute matrix that dies at minute 85 must not lose the earlier results. All `print()` calls use `flush=True`.

Row schema in `matrix.csv`:

```
noise, method, wall_s, flights, deviations, continuity, rows_out,
rmse_m (nan if no truth), seg_f1 (nan if no truth), coverage (nan if no truth)
```

---

## 13. Infrastructure (Docker Compose stack)

Services in the compose file:

| service        | image                           | ports     | purpose |
|----------------|---------------------------------|-----------|---------|
| kafka          | `apache/kafka:3.7.0` (KRaft)    | 9092      | ingest bus |
| postgres       | `postgres:16`                   | 5432      | eval + run history sink |
| prometheus     | `prom/prometheus:v2.53`         | 9090      | metrics |
| grafana        | `grafana/grafana:10.4`          | 3000      | dashboards |
| spark-master   | `apache/spark:3.5.3`            | 7077, 8080| cluster coordinator |
| spark-worker-1 | `apache/spark:3.5.3`            | 8081      | 2 workers total |
| spark-worker-2 | `apache/spark:3.5.3`            | 8082      | |
| node-exporter  | `prom/node-exporter:v1.8`       | 9100      | host metrics |

**Why `apache/spark` and not `bitnami/spark`**: Bitnami removed its public image catalog in late 2025; `bitnami/spark:3.x` returns 404. The `apache/spark` entrypoint does not pick a role on its own, so each Spark service needs an explicit `command:` directive for master or worker.

**Topic creation**: a shell helper creates `adsb.raw`, `adsb.clean`, `adsb.dlq`. Run it after `docker compose up -d kafka`.

**Postgres schema**: a SQL file creates three tables:

- `pipeline_runs(run_id UUID PK, started_at TIMESTAMPTZ, finished_at TIMESTAMPTZ, source, method, noise, config JSONB)`
- `eval_results(run_id FK, metric_name, metric_value, metric_units)`
- `deviation_events(run_id FK, flight_id, t_start, t_end, duration_s, max_cross_track_m, mean_cross_track_m, cause)`

**Prometheus scrape targets** in `prometheus.yml`:

- `node-exporter:9100`
- `spark-master:8080/metrics/prometheus/`
- `host.docker.internal:8000` (replay producer)
- `host.docker.internal:8001` (live producer)

---

## 14. How to run everything

All commands below assume PowerShell on Windows and the conda env is active.

### 14.1. Bring up the infra stack

```powershell
docker compose -f <package-root>/infra/docker-compose.yml up -d
bash <package-root>/infra/create_topics.sh
```

Verify: `docker compose ... ps` shows all services healthy.

### 14.2. Create Postgres tables

```powershell
docker exec -i $(docker compose -f <package-root>/infra/docker-compose.yml ps -q postgres) \
  psql -U postgres -d etl4 < <package-root>/infra/create_tables.sql
```

### 14.3. Download a small S3 slice (dry-run first, always)

```powershell
python -m <package>.ingest.s3_downloader --start-date 2022-06-27 --end-date 2022-06-27 --dry-run
python -m <package>.ingest.s3_downloader --start-date 2022-06-27 --end-date 2022-06-27
```

### 14.4. Generate synthetic data for metric evaluation

```powershell
python -m <package>.ingest.synthetic --out data/reference/synthetic_truth --seed 42
```

### 14.5. Run the full matrix on real S3 data

```powershell
python -m <package>.bench.run_matrix --bronze data/bronze/raw_states --source s3 --profiles N0 N1 N2 N3
```

With synthetic truth for RMSE:

```powershell
python -m <package>.bench.run_matrix --bronze data/bronze/synthetic --truth data/reference/synthetic_truth --source synthetic --profiles N0 N1 N2 N3
```

### 14.6. Run the orchestrator (one method end-to-end with Postgres sink)

```powershell
python -m <package>.run_pipeline --bronze data/bronze/raw_states --source s3 --method kinematic
```

### 14.7. Start the live producer

```powershell
python -m <package>.ingest.live_producer --interval 10 --bootstrap-servers localhost:9092 --topic adsb.raw
```

### 14.8. Replay from bronze through the ingest bus

```powershell
python -m <package>.ingest.replay_producer --input data/bronze/raw_states --speed 1.0 --bootstrap-servers localhost:9092 --topic adsb.raw
```

### 14.9. Run the tests

```powershell
pytest <package-root>/tests/test_pipeline.py -v
python <package-root>/tests/smoke_kafka.py
```

---

## 15. Tests to implement

The deliverable ships a single pandas-level test module (no Kafka or DB dependency for the main suite) and one Kafka round-trip smoke test. Each test anchors exactly one invariant. Suggested names below; the invariants are what matters.

| test name (suggested)                    | what it guards |
|------------------------------------------|----------------|
| `test_project_raw_rest`                  | 17-field REST payload maps to 13-column canonical schema |
| `test_project_raw_s3`                    | S3 CSV schema maps correctly, including nullable types |
| `test_clean_rejects_bad_lat`             | Out-of-range lat/lon is rejected with correct reason |
| `test_clean_dedup`                       | Duplicates on (icao24, time) are collapsed |
| `test_event_time_order_invariant`        | After ordering, per-icao24 time is monotonic |
| `test_kinematic_features_dt`             | `dt` is 0 at first row, correct elsewhere |
| `test_kinematic_features_turn_rate_wrap` | Turn rate wraps 350° -> 10° to +20°, not -340° |
| `test_time_gap_segments`                 | 30-min gap triggers new flight_id |
| `test_time_gap_na_safety`                | Nullable Int64/Boolean does not crash segmentation |
| `test_kinematic_segments`                | Each of (gap, jump, alt, turn, callsign) triggers break |
| `test_kalman_converges`                  | Residual decreases over a clean synthetic flight |
| `test_kalman_outlier_flag`               | Injected position jump produces a residual above threshold |
| `test_reference_cross_track`             | Vectorised cross-track matches scalar reference on 100 points |
| `test_deviations_min_duration`           | 30-s off-route spike does NOT fire; 90-s does |
| `test_correction_replaces_outlier`       | Flagged row's lat/lon is replaced by smoothed |
| `test_metrics_segmentation_f1`           | Known (TP, FP, FN) produces the expected F1 |
| `test_end_to_end_synthetic_tiny`         | 20-aircraft, 10-minute synthetic run returns non-zero metrics |

**Kafka smoke test**: produce 500 messages, consume them back, assert count and payload round-trip. Must finish in under 10 seconds against a local Kafka.

Acceptance: all tests green on a fresh clone, no warnings about dtype conversion, no deprecation warnings from pandas.

---

## 16. Performance budget and scaling notes

Design the pandas implementation so these targets are reachable. Spark promotion is a later phase; keep every stage a pure function of its input DataFrame so an `applyInPandas` port is mechanical.

- **Target throughput**: a 2-hour slice (~4M rows) of real S3 data should complete the full 3 × 4 matrix in under 2 hours on a 32-core workstation. Beyond that, start profiling.
- **Expected hot stage**: the Kalman filter per-flight loop. Its wall time should be < 20 minutes per noise profile on a 4M-row slice. If the first run is slower, look first for `grp.iterrows()` or scalar-pandas mutation inside the inner loop (see §17.6).
- **Memory ceiling**: the Kalman stage must stay under 8 GB peak on the matrix run so the `apache/spark:3.5.3` worker (6.5 GB default) can host it in a later port.
- **Full 105-day historical pull**: ~279 GB raw tar, ~400 GB bronze parquet, ~90 hours wall-clock at 500 Mbps. Do NOT attempt on a laptop SSD.

---

## 17. Known pitfalls and required patterns

Every item below is a trap a naive implementation will fall into. The required pattern is prescribed; follow it from the first commit rather than discovering the problem on a 4M-row run.

### 17.1. Nested archives in the S3 tars

The S3 tar does NOT contain a bare `.csv`. The inner file is usually `.csv.gz`, and some hours contain a nested `.csv.tar` or `.csv.zip`. The reader must handle every one of `.csv`, `.csv.gz`, `.csv.bz2`, `.csv.tar`, `.csv.zip`, `.tar`, `.tar.gz`, `.tgz`, `.zip`, including recursion into nested archives. The converter's glob must match the same set.

### 17.2. Tarfile stream + pandas gzip

`pandas.read_csv(tarfile.extractfile(member), compression="gzip")` raises because the stream does not implement the seek calls pandas needs for gzip detection. Required pattern: read the whole member into memory and decompress explicitly with `gzip.decompress()`, then pass a `BytesIO` to `pandas.read_csv(..., compression=None)`.

### 17.3. `pyarrow` partitioned read

`pd.read_parquet("data/bronze/raw_states")` on a `date=.../hour=...` tree raises `ArrowTypeError: Field hour has incompatible types` because the partition column type differs between files. Required pattern: iterate the tree, read individual parquet files, then `pd.concat(ignore_index=True)`.

### 17.4. Nullable Int64/Boolean in segmentation

S3 CSV loads `on_ground` as `BooleanDtype` and integer columns as `Int64Dtype`. A pandas `BooleanDtype` series with any NA breaks `.cumsum()` and `.astype(int)` with `TypeError: int() argument must be ... not 'NAType'`. Every boolean mask fed into `.cumsum()` must be coerced first:

```python
# WRONG — crashes on NAType
seg_index = (df["dt"] > 1800).cumsum()
df["flight_id"] = df["icao24"] + "_" + seg_index.astype(str)

# RIGHT
boundary = (df["dt"] > 1800).fillna(True).astype(bool)
seg_index = boundary.cumsum().fillna(0).astype("int64")
df["flight_id"] = df["icao24"] + "_" + seg_index.astype(str)
```

### 17.5. Cross-track distance must be vectorised

A per-row `df.apply(lambda r: cross_track_distance_m(r.lat, r.lon, ...))` dominates wall time at scale (it can exceed 95% on a single-hour slice). Required pattern: implement `cross_track_distance_m()` to accept numpy arrays of lat/lon and return an array of signed metres in one call. Keep a scalar variant available for unit tests, but never call it from the hot path.

### 17.6. Kalman per-aircraft loop

`grp.iterrows()` is roughly 100× slower than numpy on long groups. Required pattern inside the Kalman smoother: pull each column to a numpy array once (`grp["time"].to_numpy(dtype=float)`, etc.), index it with `arr[i]` in the inner loop, and assign the output arrays back with a single `out.loc[idx, col] = arr`. Do not call `iterrows()`.

### 17.7. Out-of-order noise injection

Per-row `.iloc[i] = other_row` scalar assignments trigger a full copy per assignment; on a 4M-row DataFrame, N1 shuffling can take hundreds of times longer than N0. Required pattern: build a permutation array and apply it with a single `iloc`:

```python
if cfg["out_of_order"] > 0:
    n_shuffle = int(len(out) * cfg["out_of_order"])
    if n_shuffle:
        perm = np.arange(len(out))
        picked = rng.choice(len(out), size=n_shuffle, replace=False)
        perm[picked] = rng.permutation(perm[picked])
        out = out.iloc[perm]
```

### 17.8. Windows cp1252 encoding on arrow characters

`print("downloaded → bronze")` crashes on the Windows cp1252 default stdout encoding. Never emit `→`, em-dash, non-breaking space, or other non-cp1252 characters from any script. Use `-->` or `to`.

### 17.9. `conda run python -c "multi\nline"`

PowerShell mangles multi-line `-c` payloads. For any payload longer than a single line, write the code to a file and run that file.

### 17.10. Bitnami Spark images

`bitnami/spark:3.x` returns 404 — Bitnami removed the public image catalog in late 2025. Use `apache/spark:3.5.3`. The `apache/spark` entrypoint does not pick a role on its own, so each Spark service in `docker-compose.yml` needs an explicit `command:` directive for master or worker.

### 17.11. Docker not on PATH

Docker Desktop on Windows sometimes installs without updating the user PATH. Verify `docker --version` in a fresh PowerShell after install; if missing, add `C:\Program Files\Docker\Docker\resources\bin` to System PATH and open a new shell.

### 17.12. Broad `.gitignore` patterns

A bare directory line like `src/` would hide every pipeline source file. Use the targeted exclusion list in §4.3. Any PR broadening ignore patterns needs a line-by-line review.

---

## 18. Edge cases — a complete checklist

Treat this as a conformance checklist. Any change to a pipeline stage must verify these are handled.

### Ingest

- [ ] 17 raw REST fields preserved, in order, no renames in the producer.
- [ ] OAuth2 token refresh at 30-second margin (not 0, not 5 min).
- [ ] 401 triggers one retry after refresh, then DLQ.
- [ ] JSON serialisation failure → DLQ with reason.
- [ ] Replay `--speed 0` sleeps nothing.
- [ ] `poll(0)` called on every produce.
- [ ] S3 download idempotent; existing files not re-downloaded.
- [ ] Dry-run prints total size before the real pull.
- [ ] `.csv.gz` inner members decompress explicitly (not via `pandas.read_csv` with the stream).
- [ ] Nested `.csv.tar` / `.csv.zip` handled.

### Clean

- [ ] `icao24` is `string` everywhere, never `object` or `Int64`.
- [ ] Missing lat/lon dropped; reason logged.
- [ ] Out-of-range lat/lon/alt rejected with distinct reasons.
- [ ] Deduplication is `(icao24, time)`, keep first.
- [ ] Position-jump flag set but row NOT dropped.
- [ ] Rejected rows returned separately, same schema as input.
- [ ] Nullable `BooleanDtype` on `onground` preserved to downstream.

### Order and features

- [ ] Sort is `(icao24, time)` stable.
- [ ] Defensive monotonicity check raises on inversion.
- [ ] `dt` is 0 at the first row of each `icao24`.
- [ ] Turn rate wrapped to ±180° BEFORE dividing by dt.
- [ ] Heading null handled — turn rate is null in that case, not 0.
- [ ] No `.apply()` per row in feature computation.

### Reconstruct

- [ ] `.fillna(True).astype(bool)` on every boolean mask fed into `cumsum`.
- [ ] `.fillna(0).astype("int64")` before converting `cumsum` output to string.
- [ ] Callsign change only triggers a break if the new callsign is non-null.
- [ ] Kalman per-flight loop uses numpy arrays, not iterrows.
- [ ] Kalman first row initialises ENU origin to its own position (no "global" origin).
- [ ] Kalman handles flights with < 2 rows gracefully (returns the single position as-is).
- [ ] `flight_id` format is `<icao24>_<segment_index>` with the icao24 string on the left.

### Reference and deviations

- [ ] Cross-track vectorised, no `df.apply`.
- [ ] Airport > 100 km from endpoint → skip reference for that flight, do not crash.
- [ ] Origin == destination → straight great-circle between first/last position.
- [ ] Deviation events filtered by min duration (60 s) via run-length encoding.
- [ ] Residual-based detection only runs when Kalman output is present.

### Correction

- [ ] Only rows above residual threshold are replaced.
- [ ] Short-gap interpolation at ≥ 10 s and < 60 s only.
- [ ] Longer gaps left unfilled.
- [ ] Synthetic rows flagged with a column; truth metrics ignore them.

### Evaluation

- [ ] RMSE computed per flight against nearest-in-time truth record (not nearest-in-space).
- [ ] Segmentation F1 uses 60-s tolerance.
- [ ] Coverage ratio tolerates 0 truth rows (returns NaN for that flight, not a divide-by-zero).
- [ ] Deviation F1 requires > 50% overlap on event duration.

### Infrastructure

- [ ] Kafka running in KRaft (no Zookeeper).
- [ ] Spark images `apache/spark:3.5.3`, not Bitnami.
- [ ] Postgres schema created before the first orchestrator invocation.
- [ ] `prometheus.yml` includes both producer endpoints and node-exporter.

### Experiment matrix

- [ ] Writes `matrix.csv` after every run, not only at end.
- [ ] Noise injection uses `numpy.random.default_rng(seed=42)`.
- [ ] Out-of-order shuffle is a single `iloc[perm]`, not per-row swaps.
- [ ] Duplicates appended after gap-drop (not before), so the duplicate rate is relative to the reduced set.
- [ ] Jumps use `uniform(-2, 2)` degrees in both lat and lon.

### Tests and reproducibility

- [ ] `pytest` green from a fresh clone.
- [ ] No pandas `PerformanceWarning` or `DeprecationWarning` at test run.
- [ ] `verify_env.py` output matches the pinned versions in `requirements.txt`.
- [ ] Random seed documented and used consistently.

---

## 19. Deliverables and acceptance criteria

The deliverable is accepted when ALL of these are true:

1. The test suite returns green from a fresh clone with no local state (`pytest`).
2. The matrix driver completes with a `matrix.csv` containing 12 rows (3 methods × 4 profiles) with no NaN in `wall_s`, `flights`, `continuity`, `rows_out`.
3. The `(method, noise) = (kinematic_smoothing, N0)` row has `rmse_m` strictly lower than `(time_gap, N0)` on a synthetic-truth run. (Hypothesis test: Method C should beat Method A on clean data.)
4. `docker compose ... ps` shows all services healthy.
5. A Prometheus query at `http://localhost:9090` returns non-zero `records_sent_total` after a 30-second live-producer run.
6. A row in Postgres `eval_results` exists for the most recent orchestrator invocation.
7. `git grep -nE "Co-Authored-By|Generated with"` returns empty across the tree AND the git log.
8. No secrets in the committed tree. `git grep -i "client_secret\|password\|bearer "` returns only documentation references (this file, `CLAUDE.md`, `.env.example`).
9. The `README.md` at the package root points to this file as the authoritative specification.

---

## 20. Change management

### 20.1. ADR log

Maintain an append-only ADR log at `<package-root>/docs/decisions.md`. Any change that touches

- a pipeline schema (anything in §6),
- a reconstruction-algorithm parameter (anything in §8),
- a noise-profile parameter (§12.1), or
- an evaluation-metric definition (§11),

MUST include a new entry in the log in the format:

```
### <YYYY-MM-DD> — <one-line decision title>

**Context**: ...
**Decision**: ...
**Consequences**: ...
**Alternatives considered**: ...
```

### 20.2. Branch model

- `main` — stable, only PR merges.
- One long-lived integration branch (e.g. `feature/pipeline`) carries the end-to-end baseline while it is being built.
- Topic branches off the integration branch named `feature/<short-topic>` or `fix/<short-topic>`.

### 20.3. Commit messages

Conventional Commits. No AI-assistant attribution trailer. Example:

```
feat(reconstruct): segment flights on sustained altitude-rate anomalies

Breaks a flight when |alt_rate| > 50 m/s for more than one record, in
addition to the time-gap and position-jump rules.
```

### 20.4. Force-push rules

Never force-push `main`. Force-push to a feature branch is allowed with `--force-with-lease` only, and only after confirming no one else has pulled it.

---

## 21. Glossary

- **ADS-B**: Automatic Dependent Surveillance–Broadcast. Aircraft broadcast position/velocity on 1090 MHz; ground receivers decode.
- **ICAO 24**: 24-bit globally unique aircraft identifier. Rendered as 6 hex characters.
- **Callsign**: pilot-chosen mission identifier, not an aircraft identity. Can change mid-flight.
- **State vector**: one ADS-B snapshot at one time — the 17 fields in §6.2.
- **Great-circle**: shortest surface path between two points on a sphere.
- **Slerp**: spherical linear interpolation, the uniform-velocity parameterisation of a great-circle.
- **Cross-track distance**: perpendicular distance from a point to a great-circle route.
- **ENU**: East–North–Up local Cartesian frame.
- **KRaft**: Kafka's Zookeeper-less consensus layer.
- **DLQ**: Dead-letter queue — ingest-bus topic for messages that cannot be processed.
- **Bronze / silver / gold**: medallion architecture tiers. Here, bronze = raw-schema parquet; silver = cleaned canonical; gold = reconstructed per-flight.
- **N0..N3**: four noise profiles of increasing severity defined in §12.1.
- **Method A / B / C**: time-gap, kinematic, Kalman-smoothed reconstruction respectively.

---

*End of specification.*
