# Viva Preparation — ADS-B Trajectory Reconstruction

**Course**: DA 231o — Data Engineering at Scale, IISc Bangalore
**Purpose**: cover the twelve questions a thoughtful grader is likely to ask after reading `plan.md`, `etl4/docs/final_report.md`, and `etl4/README.md`. Each answer is self-contained and cites the file to re-read if pressed.

---

## Q1. Why three reconstruction methods instead of one?

The central research question is comparative, not absolute: "how much accuracy is gained by incorporating aircraft kinematics, and what computational cost is required to obtain that improvement at large scale?" A single method cannot answer that — you need a ladder. Method A (time-gap only) is the simplest baseline any ADS-B pipeline could ship; method B (kinematic breaks) is the lightest motion-aware step on top; method C (Kalman smoothing) is the probabilistic frontier. The twelve-cell `(method x noise)` matrix exists precisely so that the extra wall-clock of B over A, and of C over B, can be priced against measurable accuracy gain. One method would give you a number; three give you a curve.
*(see plan.md §1, final_report.md §9)*

## Q2. Walk me through a single ADS-B state vector from `/api/states/all` to the eval row.

A REST poller (`etl4/ingest/live_producer.py`) fetches `/api/states/all` with OAuth2 and publishes the raw 17-field array to Kafka topic `adsb.raw` with no projection. A consumer deserialises and lands the row in bronze parquet. `etl4/clean/clean.py::project_raw(df, source="rest")` maps the 17 REST fields to the 13-column canonical schema, drops rows with null lat/lon, deduplicates on `(icao24, time)`, and pins `icao24` as `string`. The row is then stably sorted within its `icao24`, picks up kinematic features (`dt`, `inferred_speed`, `alt_rate`, `turn_rate`, `position_jump`), is assigned a `flight_id` by one of the three reconstruct modules (`etl4/reconstruct/{time_gap,kinematic,kalman}.py`), and finally rolls up through `etl4/evaluate/metrics.py` into one row of `data/eval/matrix.csv`.
*(see plan.md §7 for the stage contracts; etl4/run_pipeline.py for the wiring)*

## Q3. Why keep ingest raw when the pipeline only uses 13 of the 17 fields?

This is a Data Engineering at Scale showcase, not a production ingest written for byte-economy. Preserving all 17 raw fields means schema drift at OpenSky is caught at the DLQ boundary instead of being silently masked by a projection; it also lets us replay any downstream change against the raw stream without re-polling. The four unused fields (`sensors`, `last_contact`, `spi`, `position_source`) are small compared to Kafka's throughput budget, so the cost is negligible. The explicit rule is in the "non-negotiable constraints" at `plan.md` §0.2: the clean stage is the only place where the three source schemas converge.
*(see plan.md §0.2, decisions.md 2026-09-18)*

## Q4. Why pandas instead of Spark from day one?

Fast iteration mattered more than top-end scale in the first pass, and Spark-from-day-one would have slowed the debug loop on a per-flight Kalman filter by an order of magnitude. The hedge against that choice is the module shape: every stage is a pure DataFrame-in / DataFrame-out function keyed by `icao24`, so the `applyInPandas` port is mechanical — the stage body becomes the UDF passed to `groupBy("icao24").applyInPandas(...)` with only a schema declaration added. The 4.07M-row cleaned slice fits in memory on the lead workstation, so pandas is the actual runtime, not a scaffold. The Spark promotion is the headline future-work item in `final_report.md` §11.
*(see decisions.md 2026-09-22, plan.md §2 module structure)*

## Q5. Why 6-state constant velocity in local ENU — not UKF, not global ECEF, not 9-state with acceleration?

Per-flight scope caps the geometry: a flight rarely exceeds ~500 km at 1 Hz, so flat-Earth ENU with an origin reset per flight is valid to centimetres — global ECEF would add projection math for no accuracy gain. 9-state with acceleration was tried and rejected because ADS-B's ~1 m/s velocity noise at 1 Hz swamps any acceleration signal; the filter went degenerate and the position RMSE got *worse*, not better. UKF was rejected because the ENU conversion is linear (ECEF ↔ ENU is a rotation + translation) and the motion model is linear — EKF collapses to KF here, and UKF adds sigma-point cost with no nonlinearity to pay for.
*(see decisions.md 2026-10-26, etl4/reconstruct/kalman.py for the implementation)*

## Q6. How did you pick 30-min gap, 340 m/s jump, 50 m/s alt-rate, 30 deg/s turn-rate?

Each threshold answers one physical plausibility check. 30 minutes is the turn-time gap at major hubs — shorter would over-fragment holding patterns, longer would merge genuine cycles. 340 m/s is Mach 1 at cruise altitude; no commercial airframe sustains this, so an inferred speed above it is almost certainly a position error, not a real move. 50 m/s altitude-rate is ~9800 ft/min, well above the ~3000-4000 ft/min typical climb/descent even for military traffic. 30 deg/s turn-rate corresponds to a bank angle beyond civil tolerances at cruise speed. All four are module-level constants in `etl4/reconstruct/kinematic.py` and any change is an ADR event.
*(see decisions.md 2026-10-18)*

## Q7. Your `(kinematic_smoothing, N0)` RMSE isn't strictly better than `(time_gap, N0)`. Why?

Honest answer: the committed `data/eval/matrix_synthetic.csv` shows both at 3377.9 m — equal, not strictly less, so the §15 criterion-3 hypothesis test currently fails on this run. The diagnosed cause is a cold-start interaction between the chi-square residual gate (`RESIDUAL_SIGMA = 3.0`) and the velocity initialisation: the first two samples of each flight are used to seed velocity, and under N0 noise one of them was being gated as an outlier, leaving the filter with zero-velocity priors that it carried for several steps. The velocity-init fix (seed velocity from a two-sample finite difference *before* the residual gate activates) is in the latest commit. Worker A's re-run of the synthetic matrix is pending; the expectation after re-run is a strict inequality at N0 and all three noisy profiles.
*(see plan.md §15 criterion 3, data/eval/matrix_synthetic.csv, etl4/reconstruct/kalman.py)*

## Q8. Isn't sampled-per-aircraft noise more realistic than fixed per-row probabilities with seed 42?

More realistic, yes; useful for *this* comparison, no. The experiment compares three methods under a stress ladder; any per-aircraft variance in the noise injection would confound the method comparison because method B's `position_jump` flag is itself aircraft-sensitive — you cannot tell whether method B lost a point because its logic failed or because that aircraft drew a bad noise sample. Fixed per-row rates with seed 42 keep the noise draw identical across the three methods, which makes the accuracy delta attributable to the method and nothing else. A per-aircraft extension is future work; it is not what the current matrix answers.
*(see decisions.md 2026-11-02, plan.md §12.1)*

## Q9. What happens if an `icao24` is mis-typed as `object` or `Int64` instead of `string`?

Silent cross-join of airframes. ICAO24 identifiers are 6-character hex strings like `0a1b2c`; pandas' type inference will happily parse that as the decimal integer 10534700, and any downstream `groupby("icao24")` then merges every airframe whose hex happens to be numeric with the one whose hex is a prefix-matched decimal. The consequence is wrong flight counts, wrong continuity, and completely scrambled reconstructed trajectories — and the test does not fail because the data types are internally consistent after the bad inference. The guard is `plan.md` §0.6: every read path pins `dtype="string"` on `icao24`, and every parquet write persists it as string.
*(see plan.md §0.6, CLAUDE.md §2.6)*

## Q10. Where would this pipeline break at 1-week scale?

Two places. First, the Kalman per-flight loop in `etl4/reconstruct/kalman.py` is single-threaded in pandas and already takes 770-960 s on a 2-hour slice; a 7-day slice is ~84x the data, which puts the Kalman at roughly 18-22 hours on the same node — untenable without the Spark promotion that parallelises per `icao24`. Second, the airport-pair reference lookup in `etl4/reconstruct/reference.py` scales with flight count, not row count; method B's flight count already grows from 102 k (N0) to 163 k (N3) on 2 hours, so a 7-day run would push nearest-airport lookups to the tens of millions and the current `(airports.csv × flights)` broadcast no longer fits comfortably in memory.
*(see final_report.md §7 results table, plan.md §16 performance budget)*

## Q11. If you had one more week, what would you do?

Three things, ranked by how much they advance the central research question. (1) Spark promotion of method C: wrap the Kalman body in `groupBy("icao24").applyInPandas(...)` and run a 1-day S3 pull on a 4-worker cluster — this is the quantitative scale half of the research question. (2) Expand the airport seed from 49 to the full ~10 000-row ICAO list and cache the nearest-airport join as a geospatial index, which removes the §10 bottleneck. (3) Label a small corpus of deviations by hand and train a simple classifier as a "method D" for the deviation-detection comparison, which is currently rule-based only.
*(see final_report.md §11, plan.md §19 deliverables)*

## Q12. How do you know the Grafana dashboards work — the Docker stack isn't running right now?

Three independent checks. First, `etl4/infra/grafana/provisioning/datasources/datasources.yaml` and `etl4/infra/grafana/provisioning/dashboards/dashboards.yaml` auto-register the Prometheus datasource and the three dashboard JSONs (`01_ingest_throughput`, `02_system_resources`, `03_pipeline_results`) on container start — no manual click-through. Second, `docker compose -f etl4/infra/docker-compose.yml config` validates the compose file syntactically, including the volume mounts from `dashboard/` into `/etc/grafana/provisioning/dashboards/`. Third, each dashboard JSON parses and pins `schemaVersion: 39`, which is the minimum for Grafana 10. End-to-end stack-up is a demo-day step; the code-side guarantee is that provisioning is declarative and version-pinned.
*(see etl4/infra/docker-compose.yml, etl4/infra/grafana/provisioning/, etl4/dashboard/*.json)*

---

*End of viva prep. If a grader opens a thread you cannot place, fall back to `plan.md` — it is the authoritative specification and the file every answer here routes to.*
