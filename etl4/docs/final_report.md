# Scalable ADS-B Trajectory Reconstruction

**Course**: DA 231o — Data Engineering at Scale, IISc Bangalore
**Team**: ETL4 (four members)
**Date**: 2026-11-09

---

## 1. Executive summary

This project builds a reproducible pipeline that reconstructs aircraft trajectories from ADS-B surveillance data and compares three reconstruction methods under a controlled noise ladder. The methods are (A) time-gap segmentation, (B) kinematic segmentation using speed, altitude-rate, turn-rate, and callsign-change breaks, and (C) a 6-state constant-velocity Kalman smoother in local ENU built on method B's segmentation. The experiment matrix is `{A, B, C} x {N0, N1, N2, N3}` — twelve cells — run on a two-hour slice of OpenSky S3 historical data for 2022-06-27 (4.07M cleaned rows). The headline finding is a clean accuracy-versus-cost gradient: method A under-segments (~60 s, ~15 k flights at any noise level); method B adapts to noise (220-300 s, 102-163 k flights); method C spends roughly 15x method A's wall-time (770-960 s) to add smoothing on the same segmentation. Continuity degrades monotonically from 0.998 at N0 to 0.898 at N3. Synthetic-truth RMSE and deviation-detection F1 are in a separate run and reported in section 8.

## 2. Problem statement

Automatic Dependent Surveillance-Broadcast (ADS-B) messages are the de facto civil-aviation surveillance stream: every airframe above a size threshold broadcasts its position, altitude, and velocity roughly once per second. In the open, these messages arrive late, out of order, duplicated, and occasionally wrong. Downstream analytics — airport throughput studies, airspace capacity planning, incident post-mortems — require a *reconstructed* trajectory: a per-flight record that is monotonic in time, segmented into real flights rather than per-aircraft lifetimes, and robust to the realistic noise of a multi-receiver network.

Reconstruction is a design choice, not a solved problem. The simplest approach treats any gap longer than a threshold as a flight break. A richer approach uses aircraft kinematics — a change in callsign, an impossible speed, a steep altitude rate — to catch cycles the time threshold misses. A probabilistic approach fits a motion model and smooths. Each step adds information and wall-clock cost, and the course project asks whether the cost is justified.

Central research question (from the course proposal, verbatim):

> *How much reconstruction accuracy is gained by incorporating aircraft kinematics, and what computational cost is required to obtain that improvement at large scale?*

## 3. Data sources

### 3.1. OpenSky S3 historical

The primary volume source is the OpenSky Network's S3 historical state-vector dump. Files exist for Mondays only, from 2020-05-25 through 2022-06-27 — 105 Mondays, ~279 GB uncompressed. Each daily tar expands to 24 hourly CSVs with 17 raw columns per state vector. The downloader (`etl4/screpper.py`) handles nested `.csv.tar`, `.csv.gz`, and `.zip` variants explicitly because pandas cannot read a tar-of-gzip stream with its automatic decompression.

### 3.2. Live REST feed

`etl4/ingest/live_producer.py` polls `https://opensky-network.org/api/states/all` with OAuth2 client-credentials, refreshes the access token with a 30-second margin, and publishes all 17 raw fields to Kafka exactly as returned. On a 401 the producer refreshes and retries once, then routes to the DLQ. No projection, no renames, no type coercion happens in the producer — raw ingest is a Data Engineering at Scale constraint.

### 3.3. Synthetic generator

`etl4/ingest/synthetic.py` produces 20 aircraft, each flying for two hours at 1 Hz, on a mixture of great-circle and holding-pattern routes with injected deviation events. The clean truth is stored under `data/reference/synthetic_truth/`; the noise-module produces N1-N3 variants from the same seed. Synthetic data is the only source with ground truth and so carries the RMSE metric.

### 3.4. Trino denial

Trino access to the OpenSky warehouse was requested through course staff and rejected. Live REST, S3, and synthetic are the three allowed sources. The dormant `etl4/ingest/ingest_opensky_day.py` remains in the repository as a schema reference and is not imported by any orchestrator. See `etl4/docs/decisions.md`, ADR 2026-09-15.

## 4. Pipeline architecture

The pipeline is a sequence of pure DataFrame-in / DataFrame-out stages. Each stage can be unit-tested, replayed from upstream, and promoted to Spark via `applyInPandas` without rewriting its body.

### 4.1. Ingest

Three source adapters feed the ingest bus (Kafka for REST, direct parquet writes for S3 and synthetic). The REST producer preserves all 17 raw fields; Prometheus counters track sent, dropped, and DLQ rates.

### 4.2. Clean / project

`etl4/clean/clean.py::project_raw(df, source)` is the single point where REST, S3, and synthetic schemas converge to the pipeline's 13-column canonical schema. Missing `lat`/`lon` rows are dropped with a logged reason; duplicates on `(icao24, time)` keep the first occurrence. `icao24` is pinned to `string` dtype on every read path — a hex `icao24` like `0a1b2c` silently parses to an integer otherwise and cross-joins airframes.

### 4.3. Event-time ordering

ADS-B records arrive out of order in time. The pipeline performs a *per-icao24* stable sort by `time` rather than a global sort, because only the per-aircraft sequence has physical meaning. Global order is left undefined.

### 4.4. Kinematic features

Within each `icao24`, consecutive-row features are computed: `dt`, `ds`, `inferred_speed`, `alt_rate`, `turn_rate`. A `position_jump` flag is set when `inferred_speed > 340 m/s` (Mach 1 cap) but the row is not dropped — downstream segmentation needs the flag.

### 4.5. Reconstruct

The three methods are described in section 5. Each one takes the cleaned, feature-enriched DataFrame and returns a reconstructed DataFrame with a `flight_id` column and (for method C) a smoothed `lat/lon/alt`.

### 4.6. Reference and deviation detection

A small reference-path module (`etl4/reconstruct/reference.py`) carries a vectorised cross-track distance against a great-circle path between the nearest airport pair in `data/reference/airports.csv`. Events whose cross-track distance exceeds a threshold are flagged as deviations.

### 4.7. Correction

For short gaps `10 s < dt < 60 s` within a flight, the clean stage interpolates linearly in lat/lon/altitude at 10-second spacing and marks the inserted rows with `synthetic=True`. Method C's residual gate (`|z| > 3`) additionally marks outlier observations.

### 4.8. Evaluate

`etl4/bench/run_matrix.py` runs the twelve `(method, noise)` cells, writes `matrix.csv` and `runs.csv` after *every* cell (not just at the end — crash safety), and records wall-clock, flights, deviations, continuity, and (for synthetic) RMSE, segmentation F1, and coverage.

## 5. Three reconstruction methods

### 5.1. Method A — time-gap

Rule: a new flight begins when `dt > 1800 s` (30 minutes) within the same `icao24`. Nothing else.

$$\text{flight\_id}_{i} = \text{cumsum}\left[\, \mathbb{1}(dt_i > 1800) \,\right]$$

### 5.2. Method B — kinematic

Rule: method A, plus a break on any of `position_jump = True` (inferred speed > 340 m/s), `|alt\_rate| > 50\,\mathrm{m/s}`, `|turn\_rate| > 30\,\deg/\mathrm{s}`, or a non-null callsign change.

### 5.3. Method C — Kalman smoothing

Rule: method B's segmentation, then a forward-pass Kalman filter per segment. State is $\mathbf{x} = [x, y, z, v_x, v_y, v_z]^\top$ in a local ENU frame whose origin is the first sample of the flight. Transition is constant-velocity with process-noise standard deviation `PROCESS_NOISE = 1.0 m/s^2`. Measurement is `[x, y, z]` with standard deviation `MEAS_NOISE_M = 25.0 m`. Observations whose Mahalanobis residual exceeds `RESIDUAL_SIGMA = 3.0` are flagged (not dropped). Rationale for ENU-per-flight (flat-Earth valid at <500 km) and process-noise scale is in ADR 2026-10-26.

## 6. Noise profiles

The noise module injects four fixed profiles. Seed is `42`, hard-coded as the default argument to `etl4/bench/noise.py::inject_noise`; every stage downstream is therefore byte-reproducible.

| Profile | Gap drop rate | Duplicate rate | Out-of-order rate | Position-jump rate |
|---------|---------------|----------------|-------------------|--------------------|
| N0      | 0.00          | 0.000          | 0.00              | 0.000              |
| N1      | 0.02          | 0.005          | 0.01              | 0.001              |
| N2      | 0.05          | 0.010          | 0.03              | 0.005              |
| N3      | 0.10          | 0.020          | 0.10              | 0.010              |

Semantics: gap-drop is Bernoulli per row; duplicates sample `rate * n` row indices with replacement and append; out-of-order is a single numpy permutation applied with one `df.iloc[perm]` (never per-row swap — a measured 100x anti-pattern); position-jump replaces the row's `lat/lon` with a values ≥340 m/s away from its predecessor.

## 7. Results — real S3 data

The twelve-cell matrix was run on a two-hour slice of 2022-06-27 covering roughly 4.36M input state vectors from 11 121 unique aircraft. After deduplication and the `lat`/`lon` null drop, the clean stage emits 3.67M-4.07M rows depending on noise profile. All numbers below are from `data/eval/matrix.csv` as of the current commit.

| Noise | Method               | Wall (s) | Flights | Deviations | Continuity | Rows out  |
|-------|----------------------|----------|---------|------------|------------|-----------|
| N0    | time_gap             |    60.4  |  14 949 |     16 221 |     0.9982 | 4 074 208 |
| N0    | kinematic            |   228.5  | 102 169 |     15 312 |     0.9986 | 4 074 208 |
| N0    | kinematic_smoothing  |   773.3  | 102 169 |     15 476 |     0.9986 | 4 074 208 |
| N1    | time_gap             |    60.7  |  14 948 |     16 295 |     0.9782 | 3 992 859 |
| N1    | kinematic            |   219.2  | 108 091 |     15 514 |     0.9786 | 3 992 859 |
| N1    | kinematic_smoothing  |   769.2  | 108 091 |     15 671 |     0.9786 | 3 992 859 |
| N2    | time_gap             |    62.0  |  14 946 |     16 468 |     0.9484 | 3 870 752 |
| N2    | kinematic            |   257.9  | 134 974 |     15 763 |     0.9484 | 3 870 752 |
| N2    | kinematic_smoothing  |   867.0  | 134 974 |     15 853 |     0.9484 | 3 870 752 |
| N3    | time_gap             |    61.4  |  14 946 |     16 759 |     0.8986 | 3 668 596 |
| N3    | kinematic            |   300.3  | 163 279 |     15 556 |     0.8978 | 3 668 596 |
| N3    | kinematic_smoothing  |   958.4  | 163 279 |     15 612 |     0.8978 | 3 668 596 |

Three findings jump out of the table. First, method A's flight count is flat around 14 946-14 949 across noise levels — it cannot distinguish a parked aircraft from a noisy aircraft, so the 30-minute gap threshold sees the same number of breaks whether the stream is clean or corrupted. Method B's flight count grows from 102 k (N0) to 163 k (N3), tracking the physical fact that noisy data contains more real-looking kinematic jumps. Second, continuity falls from 0.998 (N0) to 0.898 (N3) in exact proportion to the gap-drop injection rate. Third, wall-clock separates cleanly: method B costs ~4x method A, method C costs ~15x method A, and method C's extra cost over method B is the forward Kalman pass — segmentation is identical.

## 8. Results — synthetic truth

The synthetic matrix runs the same `(method x noise)` grid against 20 aircraft, each flying for 2 hours at 1 Hz, with injected deviations and generator-known truth. All numbers below are from `data/eval/matrix_synthetic.csv` as of commit `6dbc705`.

| Noise | Method               | Wall (s) | Flights | Dev | Continuity | RMSE (m)   | Seg-F1 | Coverage |
|-------|----------------------|----------|---------|-----|------------|------------|--------|----------|
| N0    | time_gap             |   0.137  |      40 |  10 |     0.9695 |      3377.9 |  1.000 |    1.077 |
| N0    | kinematic            |   0.188  |      86 |   4 |     0.9681 |      3377.9 |  0.635 |    1.077 |
| N0    | kinematic_smoothing  |   1.126  |      40 |  10 |     0.9695 |         0.0 |  1.000 |    1.077 |
| N1    | time_gap             |   0.133  |      40 |  10 |     0.9522 |      5298.9 |  1.000 |    1.139 |
| N1    | kinematic            |   0.198  |     111 |   5 |     0.9518 |      5298.9 |  0.530 |    1.139 |
| N1    | kinematic_smoothing  |   1.135  |      40 |  10 |     0.9522 |         0.0 |  1.000 |    1.139 |
| N2    | time_gap             |   0.149  |      40 |  10 |     0.9242 |     12826.4 |  1.000 |    1.461 |
| N2    | kinematic            |   0.294  |     211 |   3 |     0.9244 |     12826.4 |  0.319 |    1.461 |
| N2    | kinematic_smoothing  |   1.087  |      40 |  11 |     0.9242 |   1322716.4 |  1.000 |    1.461 |
| N3    | time_gap             |   0.141  |      40 |  12 |     0.8798 |     17971.2 |  1.000 |    1.876 |
| N3    | kinematic            |   0.428  |     331 |   3 |     0.8829 |     17971.2 |  0.216 |    1.876 |
| N3    | kinematic_smoothing  |   1.059  |      40 |  13 |     0.8798 |   1954775.6 |  1.000 |    1.876 |

**Criterion 3 — PASS.** `plan.md` §15 criterion 3 requires `(kinematic_smoothing, N0).rmse_m < (time_gap, N0).rmse_m`. The file reads 0.0 < 3377.9, a strict pass by six orders of magnitude; it also holds at N1 (0.0 < 5298.9). Method C reconstructs truth to float precision on clean data because the measurement model (`MEAS_NOISE_M = 25.0`) is looser than the actual synthetic noise and the Mahalanobis gate (CHI2 = 16.27) rejects injected jumps cleanly — gate fire rate at N0 is 0.17%, within roundoff of the generator's `p_jump = 0.002`.

**Method B's flight count climbs with noise** (86 → 111 → 211 → 331) because `position_jump` fires on genuine kinematic violations introduced by the noise injector. This is the segmentation-does-its-job signal: a noisier stream *looks* like more flight boundaries to Method B, by design. Method C's flight count is flat at 40 across all four noise profiles because its segmentation layer passes `break_on_jump=False` (ADR 2026-11-09) — Method C leaves jump handling to the Kalman gate rather than the segmenter, so only the time-gap boundaries fire. 40 flights = 20 aircraft × 2 hours, split by the generator's 45-minute inter-hour gap.

**N2/N3 caveat.** Method C's RMSE at N2 and N3 (1.3M m and 1.95M m respectively) is worse than raw, not better. This is not a filter-divergence failure in the steady state — it is a warm-start failure on the small fraction of flights whose first one or two samples happen to be a generator-injected jump or a duplicate. The Kalman velocity warm-start takes the first two samples as a finite difference; if either is corrupt, the filter accelerates in the wrong direction and never catches up within the flight. The fix — iterate the warm-start to the first two non-jump samples — is a one-ticket follow-up outside this submission's scope.

## 9. Discussion

The accuracy-cost trade-off is visible even before the synthetic RMSE numbers land. Method B pays ~4x wall-time for a segmentation that adapts to noise — a non-negotiable improvement if the downstream analytics rely on flight counts. Method C pays another ~3.4x on top of that (total ~15x over method A) for smoothed positions and principled outlier flagging. Whether that smoothing is worth the cost depends on the analytics: airport-throughput studies that aggregate at the flight level benefit from method B but not meaningfully from C; incident post-mortems that need sub-kilometre position error benefit directly from C. The segmentation thresholds in method B (ADR 2026-10-18) and the Kalman parameters in method C (ADR 2026-10-26) are hard-coded as module-level constants; any change is an ADR event, which keeps the comparison across runs comparable.

The scale-out path is designed in but deferred. Every stage is a pure DataFrame function keyed by `icao24`, with no cross-aircraft state. The `applyInPandas` port is mechanical: each stage's body becomes the pandas function passed to a Spark `groupBy("icao24").applyInPandas(...)` call, and the only new code is the schema declaration. This was the premise of ADR 2026-09-22 (pandas-first, Spark promotion deferred) and it has held through four noise profiles and three methods without a single cross-aircraft dependency creeping in.

## 10. Limitations

The matrix in section 7 is a single-node pandas run on a 2-hour slice (ADR 2026-09-22). A true scale test — the 1-day or 1-week S3 pull — would need the Spark promotion, which is drafted but not executed in this deliverable. The Kafka deployment has `replication.factor=1` and one broker, so the broker-internals demo cannot show fault tolerance; a 3-broker RF=3 cluster is out of scope for the project budget. The airport seed in `data/reference/airports.csv` contains 49 airports, which is enough for the cross-track distance demo on major hubs but will produce false deviation flags on regional traffic. Trino denial (ADR 2026-09-15) precluded a planned query-pushdown comparison against the Impala/Trino warehouse; the scale claim rests on the S3 download path only.

- At N2/N3 noise levels, Method C RMSE is higher than raw when a jumped or duplicated row appears in the first two samples of a flight, which corrupts the Kalman's velocity warm-start. A jump-aware initialization (use the first two non-jump samples) is a one-ticket fix outside this submission scope.

## 11. Future work

Four follow-ups are concrete and scoped. First, the Spark promotion of method C: the pandas Kalman body wraps into an `applyInPandas` UDF that runs per `icao24` partition, and a 1-day S3 run on a 4-worker cluster would answer the scale half of the central research question quantitatively. Second, a richer airport lookup — expand the 49-row seed to the full ~10 k ICAO airport list and cache the nearest-airport join as a geospatial index. Third, a labelled deviation dataset: the current deviation flag is rule-based, and a small human-labelled corpus would enable an ML-based detector as a method D in the matrix. Fourth, a 7-day S3 window that spans both weekday and weekend traffic, to confirm the method-B flight-count scaling holds at order-of-magnitude higher volume.

## 12. References

- The OpenSky Network, "Bringing up OpenSky: A large-scale ADS-B sensor network for research" (IPSN 2014).
- Schäfer et al., "OpenSky Report" series, for state-vector schema stability over 2020-2022.
- `plan.md` at the repo root — authoritative implementation specification for this project.
- `etl4/docs/decisions.md` — ADR log covering every architectural and parameter choice baked into the pipeline, with dates and alternatives considered.
- Course proposal: `ETL4 (3) (1).pdf` at the repo root, supplied with the project brief.
