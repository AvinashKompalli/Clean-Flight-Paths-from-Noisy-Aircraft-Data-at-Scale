# Decisions

Record each schema change here. A change needs all four members.

| Date | Decision | Owner |
|---|---|---|
| 2026-10-26 | Repo scaffold created. Synthetic data used until Trino access is approved. | Avinash |

## Open questions
- Which regional bounding box for the 1-day regional run?
- Which 7-day window for the global run?
- Spark 3.5 or 4.0? Pin one in week 1.

---

## Architecture Decision Records

Retroactive log of the major architectural and parameter choices baked into the pipeline. Append-only.

### 2026-09-15 — Drop Trino; data sources are S3, REST, synthetic only

**Context**: The team requested Trino access through course staff to query the OpenSky Impala/Trino warehouse. The request was rejected and a re-request would not land before the ingest stage was due.
**Decision**: Three allowed data sources only — OpenSky live REST (`/api/states/all`), OpenSky S3 historical tarballs (`states_YYYY-MM-DD-HH.csv.tar`), and locally generated synthetic trajectories with ground truth. `etl4/ingest/ingest_opensky_day.py` stays on disk as a schema reference but is dormant and must not be imported by `run_pipeline.py` or `run_matrix.py`.
**Consequences**: The S3 downloader (`screpper.py`) carries the volume claim for the scale showcase; live REST is used only for the streaming demo; synthetic data provides the only ground-truth error metric. DLQ and retry logic were designed around the REST rate limit (4000 credits/day anonymous, 8000 authenticated).
**Alternatives considered**: Re-request Trino through the course staff (rejected — unknown timeline, blocks the project); REST-only ingest (rejected — too low-volume for a "scale" deliverable); use the OpenSky Python API's Impala shim (rejected — same credential wall as Trino).

### 2026-09-18 — Raw ingest; all projection happens in the clean stage

**Context**: This is a Data Engineering at Scale showcase. If the producer narrows the schema at ingest, schema drift in the upstream API is hidden and the DLQ loses meaning. The team debated letting the producer drop unused fields "to save bytes".
**Decision**: `etl4/ingest/live_producer.py` publishes all 17 raw OpenSky REST state-vector fields to Kafka exactly as the API returns them (array form, mixed nulls, no renames, no type coercion). `etl4/clean/clean.py::project_raw(df, source)` is the single point where the three source schemas (REST/S3/synthetic) converge to the pipeline canonical schema in `plan.md` §6.1.
**Consequences**: DLQ messages carry recoverable payloads; the clean stage can be replayed from raw if the canonical schema evolves; any new source extends `project_raw` with one more branch rather than touching the producer. Bytes-per-message is higher than a projected form but negligible against Kafka throughput.
**Alternatives considered**: Project in the producer (rejected — hides schema drift and couples ingest to downstream schema); project in a Kafka Streams / Flink stage between ingest and clean (rejected — overkill at this project's scale and adds a second JVM service).

### 2026-09-22 — Pandas-first implementation; Spark promotion deferred

**Context**: Fast iteration on the three reconstruction methods and four noise profiles mattered more than top-end scale in the first pass. Spark-from-day-one would slow the debug loop by an order of magnitude.
**Decision**: Every pipeline stage is a pure pandas function taking and returning a DataFrame, grouped by `icao24` where parallelism matters, structured so `applyInPandas` promotion is mechanical when needed. pandas 2.x nullable dtypes (`Int64`, `BooleanDtype`, `string`) are used throughout.
**Consequences**: The full 4.36M-row per-noise-profile matrix fits in memory on a 32-core workstation with ~64 GB RAM. Spark port is a documented follow-up, not a blocker on the deliverable. Vectorisation discipline (no `apply(axis=1)`, no `iterrows`, no per-row `.iloc[i] = ...`) is required because single-node pandas is the actual runtime, not a scaffold.
**Alternatives considered**: Spark-from-day-one (rejected — slower iteration; hard to debug a Kalman loop inside `mapInPandas`); Polars (rejected — team unfamiliar, and the `applyInPandas` promotion path is specifically pandas-shaped); Dask (rejected — same memory issues at this size with worse debuggability).

### 2026-09-28 — Pin `apache/spark:3.5.3` with explicit command directives

**Context**: `bitnami/spark:3.x` was the default reference image in most tutorials and was initially used in `docker-compose.yml`. Bitnami removed the public Spark image catalog in late 2025; `bitnami/spark:3.5` returns 404 and `docker compose up` broke on a clean machine.
**Decision**: `etl4/infra/docker-compose.yml` pins `apache/spark:3.5.3` for both master and worker. Each service carries an explicit `command:` directive (`/opt/spark/sbin/start-master.sh` and `start-worker.sh spark://spark-master:7077` respectively) because Apache's entrypoint does not pick a role on its own.
**Consequences**: Known-working stack with no third-party image dependency. Anyone forgetting the `command:` directive will see the container exit immediately — a known failure mode, documented in `plan.md` §13. Spark 3.5.3 matches PySpark on Python 3.11.
**Alternatives considered**: Build a custom Spark image from the Apache tarball (rejected — maintenance burden for no gain); downgrade to Spark 3.4 (rejected — older, no reason, and the `applyInPandas` surface area is smaller); use `jupyter/pyspark-notebook` (rejected — bundles too much and mixes concerns).

### 2026-10-05 — Native Kafka 3.7.0 on Windows for the local dev workflow

**Context**: The user wanted a single-node Kafka outside Docker for one development workflow (debugging consumer lag, inspecting log segments, and demonstrating broker internals as part of the DE-at-Scale deliverable). Docker-only didn't expose the log directory in a usable way on Windows.
**Decision**: Kafka 2.13-3.7.0 tarball extracted to `B:\IISC\DE\Proj\software\kafka_2.13-3.7.0` in KRaft single-node mode. Log directory mapped through `subst K: ...` to a short path to avoid Windows' 260-char path limit. Java 26 on `PATH`.
**Consequences**: Two patches to the stock Windows scripts were required and documented: `wmic` references replaced with `Get-CimInstance` (wmic was removed in Windows 11 24H2+), and the launch classpath routed through the `subst`'d short drive to stay under the 8191-char cmd line limit. The Docker Kafka in `docker-compose.yml` remains the default for the pipeline runs; the native install is for broker-internals demos only.
**Alternatives considered**: Docker-only Kafka (rejected — user preference and poor log-segment visibility from inside the container on Windows); WSL2 Kafka (rejected — adds a second filesystem and makes the Windows-first operating contract moot); Redpanda (rejected — the showcase is Kafka specifically).

### 2026-10-18 — Segmentation thresholds for Methods A and B

**Context**: The three reconstruction methods all need an answer to "where does one flight end and the next begin for the same `icao24`?". Thresholds had to catch real breaks (parked aircraft, flight cycles) without over-fragmenting under noise.
**Decision**: Method A (time-gap only): new flight on `dt > 1800 s`. Method B (kinematic): additionally flags `position_jump` when inferred speed between consecutive samples exceeds `340 m/s` (≈Mach 1 at cruise, higher than any commercial airframe), `|alt_rate| > 50 m/s`, `|turn_rate| > 30 deg/s`, or a non-null `callsign` change. Method C (Kalman) uses Method B's segmentation then smooths within each segment.
**Consequences**: Method A under-segments noisy data (~15k flights on the 4.36M-row daily slice) because a 30-minute gap can straddle a real cycle. Method B tracks noise-level, producing 102k–163k flights across N0–N3, which is the comparison the matrix is designed to expose. These thresholds are hard-coded as module-level constants and any change is an ADR event.
**Alternatives considered**: Density-based clustering (DBSCAN / HDBSCAN on `(lat, lon, time)`) (rejected — needs a tuned radius per phase of flight, and radius-vs-cruise-speed interaction is unstable); learned classifier (rejected — no labelled flight boundaries in OpenSky S3); global `dt` threshold without kinematic flags (rejected — this is the Method A degenerate case, kept as the baseline Method B must beat).

### 2026-10-26 — 6-state constant-velocity Kalman filter in per-flight local ENU

**Context**: Method C had to produce both smoothed positions and a principled outlier flag (not just an ad-hoc z-score). Had to pick: state dimensionality, coordinate frame, process-noise model, and residual gate.
**Decision**: State `x = [x, y, z, vx, vy, vz]` with constant-velocity transition, `PROCESS_NOISE = 1.0 m/s^2` (as variance on the acceleration disturbance term), `MEAS_NOISE_M = 25.0` (1-sigma measurement noise in metres), `RESIDUAL_SIGMA = 3.0` (gate for marking an observation as an outlier). Coordinate frame is local ENU, with the origin reset to the first-sample `(lat, lon, alt)` of each flight. Flights with fewer than 2 samples return their single position unchanged.
**Consequences**: Flat-Earth approximation is valid at per-flight scope (flights rarely exceed ~500 km at ADS-B update rates), which is why the ENU origin is per-flight; code must never aggregate ENU across flights. The per-flight loop uses numpy arrays over the filter state rather than `iterrows()` — a measured 3× speedup, documented in `plan.md` §17.
**Alternatives considered**: Unscented KF (rejected — nonlinearity of the ENU conversion is bounded and handled analytically); 9-state with acceleration (rejected — ADS-B 1 Hz velocity noise swamps any acceleration signal, filter becomes degenerate); global ECEF frame (rejected — more projection math for no accuracy gain at per-flight scope); IMM with multiple motion models (rejected — out of scope for the three-method comparison).

### 2026-11-02 — Noise profile parameters N0 through N3

**Context**: The experiment matrix compares three methods under a controlled stress ladder. Noise levels had to degrade continuity cleanly and reproducibly, with per-row injection probabilities that don't depend on aircraft density.
**Decision**: Four fixed profiles with the exact drop/duplicate/shuffle/position-jump probabilities tabulated in `plan.md` §12.1; N0 is identity, N3 is the worst case. Seed is `42` and lives as the default argument to `etl4/bench/noise.py::inject_noise(..., seed=42)`. Out-of-order shuffle is a single `df.iloc[perm]` on a numpy-generated permutation, never a per-row swap.
**Consequences**: Continuity (fraction of expected 1 Hz samples present) degrades monotonically from 0.998 at N0 to 0.898 at N3; the matrix is byte-reproducible across runs. The seed must not be overridden anywhere else in the pipeline; doing so silently decouples the noise draw from the evaluation draw.
**Alternatives considered**: Sampled per-aircraft rates (rejected — introduces variance that confounds method comparison, since Method B's `position_jump` is itself aircraft-sensitive); continuous noise levels on a slider (rejected — makes the matrix non-enumerable and defeats the "four cells per method" comparison the central research question is built around).

