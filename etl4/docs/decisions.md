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

### 2026-11-09 — Kalman outlier gating, warm-start, and generator hour-gap

**Context**: The matrix §19 criterion 3 initially failed because Method C's smoothed_lat/smoothed_lon tracked raw measurements exactly on the synthetic data. Diagnosis found three interacting issues: (1) no outlier rejection in update(); (2) cold-start with v=0 made legitimate second-sample updates look like 61σ outliers once a gate was added; (3) the synthetic generator drew fresh random (lat, lon, heading) per hour, producing 1000+ km teleports that no CV Kalman could bridge.
**Decision**: Three changes in one commit (6dbc705). (a) kalman.py gates measurement updates on mahal² > CHI2_GATE=16.27; residual returned is sqrt(mahal²). (b) kalman.py warm-starts velocity from the first two samples of each flight, with the vel-block prior tightened to 100. (c) kinematic.py gains a break_on_jump param (default True preserves Method B); Method C passes False so the Kalman gate decides on jumps. (d) synthetic.py inserts a 45-min gap between consecutive hour blocks, matching real ADS-B dark periods.
**Consequences**: Criterion 3 passes at N0 (3377.9 → 0.0 RMSE). Method C flight count becomes noise-independent (40). Method B semantics unchanged. At N2/N3, jumped/duped first samples still corrupt the warm-start; follow-up is jump-aware init selection.
**Alternatives considered**: RTS backward pass (rejected — bigger scope); accept forward-only Kalman and document criterion 3 fail (rejected — the fix was tractable); change CHI2_GATE threshold (rejected — the problem was init not gate aggressiveness); regenerate truth without the generator jumps (rejected — defeats the noise-stress narrative).

### 2026-11-09 — Align three plan-vs-code ambiguities from Worker C audit

**Context**: Test-coverage audit (`etl4/docs/test_coverage_audit.md`) flagged three cases where `plan.md` §18 and the code diverged. Picked the stronger invariant for each and aligned.
**Decision**: (1) `DEDUP_KEYS = ["icao24", "time"]` in `clean.py` — a noise-injected row with the same `(icao24, time)` but drifted `lat`/`lon` is still a duplicate, which the previous 4-column key let through. (2) `kinematic_features` fills first-row `dt` with `0` instead of NaN — simplifies downstream reasoning; Method A's `dt > 1800` boundary check is unchanged (both `NaN > 1800` and `0 > 1800` are `False`). (3) `plan.md` §9.3 now states the residual-based deviation detector runs only when `kalman_residual` is present **and** has at least one non-null value; code already behaved this way via `std == 0 → threshold 100 m`, prose was ambiguous.
**Consequences**: Dedup is slightly stricter and rejects a type of row that was previously surviving clean(); downstream row counts may drop by ~0.1 % under N1–N3 noise profiles. No test required an assertion flip (16/16 still green).
**Alternatives considered**: Align plan.md to the code (rejected for all three — the plan is the normative contract and the code was the drifted side). Add a configurable dedup-strictness flag (rejected — this is a schema invariant, not a tuning knob).

### 2026-11-13 - Partition contract (ym=YYYY-MM) and watermark-driven incremental refresh

**Context**: Downstream of bronze every stage is full-refresh. The vault re-reads all 48 monthly flightlists on each build (20 to 30 min wall time, around 20 GB of transient shards during the final dedup). Marts rebuild from the full vault. The Postgres sink is `TRUNCATE` + `COPY` of roughly 4M fact rows. `eval_mart` is `DROP TABLE` + `CREATE TABLE`. Adding one month of bronze triggers a complete re-computation of the other 47. This is the single largest source of wasted compute and the proximate cause of the ENOSPC failures seen on the 10 GB scale-out. The DE-at-scale deliverable (plan.md section 1) is not credible until the pipeline stops redoing identical work.

**Decision**: Adopt a partition contract keyed on the natural ingest cadence (one Zenodo file per calendar month) and a watermark registry in Postgres that lets every layer skip partitions it has already processed.

**1. Partition key**. Every parquet tree (silver and gold) adopts a single partition key `ym=YYYY-MM`, extracted from the source `day` column at the first point in the lineage where it is available. Rationale: Zenodo publishes one flightlist archive per month (plan.md sections 5.4 and 6.3), the S3 state-vector tarballs are dated per UTC day and group cleanly into months, and the eval matrix dimension of interest is "which month do these metrics describe". Finer granularity (per day) multiplies partition count by roughly 30x with no analytic payoff; coarser (per quarter or per year) breaks the one-Zenodo-file-equals-one-partition contract that keeps `ym` extraction trivial.

**2. Partition layout**. Partitioned per-month:

- `data/silver/vault/hub_flight/ym=YYYY-MM/part.parquet`
- `data/silver/vault/link_flight_aircraft/ym=YYYY-MM/part.parquet`
- `data/silver/vault/link_flight_route/ym=YYYY-MM/part.parquet`
- `data/silver/vault/sat_flight_summary/ym=YYYY-MM/part.parquet`
- `data/silver/vault/sat_state_observation/ym=YYYY-MM/part.parquet`
- `data/gold/marts/fact_flight/ym=YYYY-MM/part.parquet`
- `data/gold/marts/fact_trajectory_enriched/ym=YYYY-MM/part.parquet`

Non-partitioned (small, globally-unique dimension-like tables):

- `data/silver/vault/hub_aircraft/part.parquet`
- `data/silver/vault/hub_airport/part.parquet`
- `data/silver/vault/sat_aircraft_identity/part.parquet`
- `data/gold/marts/dim_aircraft/part.parquet`
- `data/gold/marts/dim_airport/part.parquet`

Hubs that model a global entity (an airframe is the same airframe across every month, an airport is the same airport) stay single-file. Duplicating them per month would trade constant-time downstream joins for a monthly union in every reader. Their size is bounded by the real-world population (well under 1M airframes, 18.6k airports), and the small compaction cost (step 4) is cheaper than forcing every mart and QA query to pay a union.

**3. Watermark registry in Postgres**. Added in `etl4/infra/create_tables.sql`:

```sql
CREATE TABLE IF NOT EXISTS pipeline_watermarks (
    layer       TEXT NOT NULL,
    table_name  TEXT NOT NULL,
    last_ym     TEXT NOT NULL,       -- 'YYYY-MM'
    row_count   BIGINT,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (layer, table_name)
);
CREATE INDEX IF NOT EXISTS idx_watermarks_layer ON pipeline_watermarks(layer);
```

One row per (layer, table). `layer` is one of `vault`, `marts`, `sink`, `matrix`, `eval_mart`. `last_ym` is the newest ym the writer has committed. `row_count` is an advisory total used by the row-count reconciliation guardrail (separate ADR), not a correctness gate here. Reads are a single primary-key lookup; writes happen inside the same transaction that materialises the partition for that layer.

**4. Per-layer incremental mechanism**.

*Vault*. `_chunked_flightlist_vault` queries the watermark for `(vault, hub_flight)` to get `last_ym`, lists bronze flightlist files, keeps only those whose extracted ym is strictly greater, and streams each one into its own `ym=YYYY-MM/part.parquet` for the five partitioned tables. Within-month dedup is sufficient and safe because hub, link, and sat natural keys resolve deterministically from the source rows (plan.md section 6.1): the same `(icao24, firstseen)` in two different months produces two different hashes by construction. The three non-partitioned dimension tables (hub_aircraft, hub_airport, sat_aircraft_identity) are rebuilt by a tiny compaction pass that reads only the new month's partitioned sats/links to extract new icao24 / airport / aircraft-identity rows, unions with the existing non-partitioned parquet, dedupes on the hash key, and rewrites the single file. Sizes are small enough (around 10 MB each) that rewriting is cheaper than tracking deltas. After both passes commit, the watermark advances to the new ym.

*Marts*. `vault/marts.py` reads only `ym > last_ym` partitions of the partitioned vault tables, joins against the non-partitioned dims in full (they are small), and writes `ym=YYYY-MM/part.parquet` into each target. No global recompute. pyarrow dataset reads with a partition filter mean unchanged months are never touched.

*Sink (Postgres)*. `postgres_sink.py` replaces `TRUNCATE` with per-partition `INSERT ... ON CONFLICT` per target. For `dim_aircraft`: `ON CONFLICT (hk_aircraft) DO UPDATE SET icao24=EXCLUDED.icao24, registration=EXCLUDED.registration, typecode=EXCLUDED.typecode`. For `fact_trajectory_enriched`: `ON CONFLICT (hk_aircraft, time) DO UPDATE SET lat=EXCLUDED.lat, lon=EXCLUDED.lon, baroaltitude=EXCLUDED.baroaltitude, velocity=EXCLUDED.velocity, heading=EXCLUDED.heading, callsign=EXCLUDED.callsign, typecode=EXCLUDED.typecode, registration=EXCLUDED.registration`. The primary key is already `(hk_aircraft, time)` (create_tables.sql line 64), so the clause coexists with the existing schema without a migration. The sink loads one month at a time, commits, updates the watermark, moves on.

*Matrix*. Add `ym TEXT` to `pipeline_runs` and a unique constraint `(ym, source, method, noise)`. Re-running the matrix for a (month, source, method, noise) tuple already measured is a no-op via `ON CONFLICT DO NOTHING`. This closes the current bug where each re-run accumulates duplicate rows and silently biases dashboard aggregates.

*eval_mart*. Converted from a materialised table to a plain SQL view over `pipeline_runs` left-joined to `eval_results`. Zero storage, always consistent with the run registry, incremental by definition. Dashboard latency stays well under a second because `pipeline_runs` is bounded by (ym x source x method x noise) and will not exceed a few thousand rows over the project horizon.

**5. Backfill and rewind primitive**. New module `etl4/orchestration/rewind.py` with one CLI:

```
python -m etl4.orchestration.rewind --layer vault --from 2022-03 --to 2022-05
```

Deletes `data/silver/vault/<table>/ym=2022-03/`, `ym=2022-04/`, `ym=2022-05/` for every partitioned vault table, deletes the matching rows from Postgres sink tables (filtered by `ym` once the column is added to `fact_trajectory_enriched`), and sets `pipeline_watermarks.last_ym` to `2022-02` for every `(vault, *)` row. A subsequent `run_e2e --only vault` picks the three months back up via the ordinary incremental path. `--layer marts|sink|matrix` do the equivalent for their respective layer. `--layer all` chains all four in the correct order and refuses to run without `--yes` to protect against accidental full rewinds.

**6. Migration path (one-shot zero-reprocess split)**. For each partitioned table, a one-off script reads the current monolithic `part.parquet`, derives `ym` from `day` (vault flightlist outputs) or `floor(time / month_seconds)` (sat_state_observation), and writes `ym=YYYY-MM/part.parquet` via `pyarrow.parquet.write_to_dataset(..., partition_cols=["ym"])`. Zero re-processing of upstream data: the migration is a parquet rechunking pass. For the three non-partitioned small-dim tables there is no change. The alternative "lazy ym=all" bucket was rejected because it keeps a monolithic parquet in the tree indefinitely: the next full-refresh (schema change, hash change, reset) would still have to re-materialise it, and every reader would have to union it with newer partitions forever.

**Consequences**: One new month triggers roughly one month of real work at every layer. The vault step drops from 20 to 30 min to about 1 to 2 min for a single-month delta on current hardware. The 20 GB shard footprint during finalize vanishes because each monthly shard is written once and never re-concatenated with the others. The Postgres footprint grows additively instead of being wiped and rewritten each run; WAL pressure drops proportionally. The `eval_mart` view removes one storage-bearing stage entirely. The matrix stops accumulating duplicate rows. The migration itself is a single pyarrow pass per table, measured in minutes against today's data. Required follow-ups: (a) extend `vault/marts.py` to accept a `--only-ym` filter, (b) add `ym` to `fact_trajectory_enriched` and backfill it, (c) add the unique constraint migration on `pipeline_runs`, (d) extend `pipeline_watermarks` writes into every layer that writes partitions. Readers continue to work during the migration window because they fall back to the single `part.parquet` when no `ym=*` children exist.

**Alternatives considered**: Per-day partitions (rejected - 30x partition count with no analytic payoff and no match to the ingest cadence). Partitioning every table including the small dims (rejected - duplicates a globally-unique entity across months, forcing every reader to union-and-dedup; the compaction cost of three around-10 MB non-partitioned files is lower than the join-time cost everywhere else). Skip the watermark table and infer state from the filesystem (rejected - a half-written partition directory plus a crash leaves the pipeline unable to distinguish done from partial; Postgres provides a transactional commit point). Track incremental state in sqlite or a json file (rejected - Postgres is already up and already depended on by the matrix and the dashboard, a second state store doubles operational surface for no gain). Keep the monolithic `part.parquet` and add a bronze-level change-data-capture layer (rejected - adds a Debezium-equivalent service outside the scope of this project and does not solve the within-run dedup memory bomb).

**Risks**:

1. *Cross-month flights*. A flight that departs on the last UTC day of month M and arrives on the first UTC day of month M+1 is a single real flight but is split across two Zenodo files (`firstseen` in M, `lastseen` in M+1). The link_flight_route and sat_flight_summary partitions carry two rows for one flight unless dedup is global. Mitigation: natural-key hashes are stable, so the two rows share an identical `hk_flight`; the mart build is instructed to pick the row with the later `lastseen` when reducing the stream. The rule lives on the reader side, so writers stay partition-local.

2. *Global uniqueness for small hubs*. hub_aircraft asserts one row per airframe in the whole dataset. The compaction pass trusts that natural-key hashes are stable. If a future ingest source normalises `icao24` differently (lowercase vs uppercase, leading-zero stripping), the same airframe could land twice. Mitigation: the string-cleaning contract in `clean.py::project_raw` is already the single point of enforcement (CLAUDE.md section 2.6); the compaction pass dedupes on `hk_aircraft`, so any drift surfaces as a visible row-count anomaly via the watermark's `row_count` column.

3. *ym extraction when the source lacks a `day` column*. The S3 state-vector bronze is partitioned by `date=YYYY-MM-DD/hour=HH/` in its path, not by an in-file column (`kafka_bronze_consumer.py` and `daily_batch.py` strip the column deliberately). ym has to come from the path, not the file. Mitigation: the vault extractor uses `pyarrow.dataset` with a partitioning spec that turns `date=` back into a virtual column, then takes `.str.slice(0, 7)` for ym. For flightlist bronze the in-file `day` column is authoritative. One helper keeps the two sources symmetric.

4. *Watermark vs filesystem drift*. If an operator deletes a partition directory by hand without using `rewind`, the watermark will claim the month is loaded when it is not. Mitigation: a cheap QA check (one `COUNT(*)` of partition dirs per table vs `last_ym`) runs at the top of `run_e2e` and refuses to start if they disagree.

### 2026-11-14 - NoiseTruth return contract for inject_noise and __truth_id correspondence tag

**Context**: Half of `fact_experiment_result`'s metric columns (`seg_precision`, `seg_recall`, `seg_f1`, `position_rmse_m`, `boundary_err_s`) are NULL because `bench/noise.py::inject_noise` currently returns only the noisy DataFrame and throws away everything else it knows. The injection step is the only place in the pipeline that has ground truth for the synthetic path, and the only place that can carry deviation-detection ground truth for the real-data path (by scoring detectors against the ids it jumped). `plan.md` section 15 and the course PDF section 3 both list these metrics as success criteria; shipping them as NULL hides the accuracy half of the accuracy-vs-cost question the project is organised around.

**Decision**: `inject_noise(df, profile, seed=42) -> pd.DataFrame` becomes `inject_noise(df, profile, seed=42) -> tuple[pd.DataFrame, NoiseTruth]`. `NoiseTruth` is a frozen dataclass in the same module carrying `profile`, `seed`, `original_row_count`, `original_coords` (DataFrame of pre-jump `__truth_id, lat, lon, baroaltitude`), `dropped_ids`, `duplicated_ids`, `shuffled_ids`, `jumped_ids` (each a frozenset of ints), and `true_boundaries` (list of per-flight start points). All callers update: `run_experiment.prepare_silver` captures the truth, `reconstruct_from_silver` forwards it into `metrics.*`.

Pair with a stable `__truth_id` column emitted by `ingest/synthetic.py` on every row it generates (integer, monotonic across the generated set). Preserved through `clean.project_raw`, `clean.clean` (dedup tiebreaker = lower `__truth_id` wins), `event_time_order`, every `reconstruct.*` method, and the final gold write.

**Consequences**: Full feature-metric matrix populates for the synthetic path. `seg_*`, `position_rmse_m`, `boundary_err_s`, `coverage_ratio`, `deviation_prf` all go from NULL to real numbers. The Pareto-frontier view (`v_pareto_frontier` in the same PR) finally has data to render. Real-data (`s3`, `rest`) paths see no behaviour change - no `__truth_id` is emitted there, and the metric functions skip when the column is absent.

Downstream impact is confined: `metrics.py` functions already accept the shapes `NoiseTruth` and `__truth_id` enable; `sink/postgres.py::_PERSISTED_METRICS` already lists the five formerly-NULL metrics; `pipeline_runs`/`eval_results` schemas are unchanged.

**Alternatives considered**: Compute ground truth by rerunning the clean synthetic through the methods and diffing (rejected - noise is random; the diff is noise, not signal); mutate `df` in place and return masks separately via a global or thread-local (rejected - breaks determinism and testability); score only against external reference tracks (rejected - no cheap source of trusted reference paths at scale); extend `metrics.py` to join on `(icao24, time)` with tolerance (rejected - `(icao24, time)` drifts under jump noise, so position RMSE would underreport; `__truth_id` makes the join exact).

**Risks**: `clean.dedup` must gain an explicit `__truth_id` tiebreaker or duplicate-dropping becomes non-deterministic (documented in task T-002). `psutil` must be pinned if `bench/scale_study.py` ships in the same PR (documented separately if that change lands).

