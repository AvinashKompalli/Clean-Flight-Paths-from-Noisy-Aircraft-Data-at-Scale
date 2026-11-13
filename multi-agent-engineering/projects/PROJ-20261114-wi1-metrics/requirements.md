# Requirements - PROJ-20261114-wi1-metrics

**Authored by**: product-owner
**Date**: 2026-11-14T02:10:00Z
**Source request**: User confirmed in parent conversation on 2026-11-14 to execute Work Item 1 (feature-metric matrix completion + Pareto-frontier view + scale study) on branch `feature/pipeline-3-avinash`. Scope is **not** anomaly-detection work (deferred to a later project).

## Business context

The project is the DA 231o Data Engineering at Scale course capstone (Team ETL4, IISc Bangalore). Its **authoritative headline question** is quoted verbatim from `ETL4 (3) (1).pdf` §1 Problem Motivation:

> *"How much reconstruction accuracy can be gained by incorporating aircraft kinematics, and what computational cost is required to obtain that improvement at large scale?"*

Current reality: `fact_experiment_result` has 12 populated cells (3 methods × 4 noise profiles), but only the `continuity` and `n_flights` columns carry values. Every other metric the PDF §3 **Feature Metrics for Success** list requires is `NULL`. A reviewer cannot answer the headline question from `NULL` cells.

This project fills those cells using the ground truth that `etl4/bench/noise.py::inject_noise` already produces (knows exactly which rows it dropped, duplicated, shuffled, jumped) but currently discards. It also materialises the one chart that converts the completed matrix into the headline answer (a Pareto frontier) and runs the scale study the PDF §3 **Performance Metrics for Success** list demands.

The deliverable is the single piece of evidence a DE-at-scale reviewer can grade against the PDF's own criteria.

## Scope

### In

1. **Ground-truth plumbing**: `noise.py` returns a `NoiseTruth` sidecar with `dropped_ids`, `duplicated_ids`, `shuffled_ids`, `jumped_ids`, `original_coords`, and `true_boundaries`. A stable `__truth_id` column is added by `ingest/synthetic.py` and survives dedup/sort.
2. **Metric engine**: `etl4/evaluate/metrics.py` gains six scoring functions — `segmentation_prf1`, `boundary_time_error`, `position_rmse_m`, `coverage_ratio`, `cross_track_from_reference` (aggregator over existing vectorised function), and `deviation_prf1`.
3. **Matrix integration**: `etl4/bench/run_experiment.py` wires the new scores into the `rec` dict the Postgres sink already expects. No change to `sink/postgres.py::_PERSISTED_METRICS` — the plumbing is already named.
4. **Pareto view + panel**: Postgres view `v_pareto_frontier` over `fact_experiment_result`; one Grafana XY scatter panel in a new `etl4/dashboard/08_accuracy_cost.json` dashboard.
5. **Scale study**: new `scale_runs` Postgres table, new `etl4/bench/scale_study.py` CLI, runs the matrix at `[100k, 1M, 10M, 50M]` rows × 3 methods, logs throughput + peak RSS, surfaces as line panels on dashboard 08.
6. **Synthetic matrix volume**: 20 flights × 1,000 samples × 4 noise regimes = 80k rows per cell (user-confirmed).
7. **Unit tests**: `etl4/tests/test_pipeline.py` (or a new `test_metrics.py`) covers each new metric with handcrafted positive, negative, boundary, null, and transformation cases.

### Out

- WI-2 Phase A (batch deviation detector gates + `deviation_events_v2` + dashboard 09)
- WI-2 Phase B (streaming promotion via Spark Structured Streaming)
- ADR 2026-11-13 implementation (ym=YYYY-MM partition contract, watermark table, incremental refresh across layers)
- Any new ingest paths or bronze source
- Any refactor of existing reconstruction method internals (`time_gap.py`, `kinematic.py`, `kalman.py`)
- Dashboard visual refreshes beyond 08_accuracy_cost.json
- Postgres schema migration tooling (Alembic / dbt)

## Functional requirements

| ID | Description | Priority | Trace |
|---|---|---|---|
| FR-01 | `inject_noise` returns `(noisy_df, NoiseTruth)` where `NoiseTruth` has stable `dropped_ids`, `duplicated_ids`, `shuffled_ids`, `jumped_ids` sets, an `original_coords` DataFrame, and a `true_boundaries` Series. | must | PDF §3 feature metric "Robustness to missing, duplicated, and out-of-order observations" |
| FR-02 | Synthetic generator emits a `__truth_id` column, monotonically increasing on the clean frame, that survives downstream dedup and sort. | must | derived from FR-01 |
| FR-03 | `segmentation_prf1(pred_boundaries, true_boundaries, tolerance_s)` returns a dict with `precision`, `recall`, `f1`. | must | PDF §3 "Flight segmentation precision, recall, and F1-score" |
| FR-04 | `boundary_time_error(pred, true)` returns the mean absolute time delta in seconds between matched predicted and true boundaries. | must | PDF §3 "Flight boundary time error" |
| FR-05 | `position_rmse_m(reconstructed, truth)` joins on `__truth_id` and returns the root mean squared haversine distance in metres. | must | PDF §3 "Position/path reconstruction error" |
| FR-06 | `coverage_ratio(reconstructed, truth)` returns the fraction of unique `__truth_id` values that survived reconstruction. | must | PDF §3 "Trajectory coverage and continuity" |
| FR-07 | `cross_track_from_reference` aggregator wraps the existing vectorised `reconstruct/reference.py::cross_track_distance_m` and returns the max and the mean per flight. | must | PDF §3 "Cross-track or spatial deviation from reference path" |
| FR-08 | `deviation_prf1(flagged_mask, jumped_ids, row_ids)` returns a precision/recall/F1 dict for deviation-detection output scored against noise-injected jumps. | must | PDF §3 "Precision and recall of significant deviation detection" |
| FR-09 | `run_experiment.py` populates all six metric columns in the `rec` dict for every `(method, noise_profile)` synthetic-path run; existing continuity and n_flights remain unchanged. | must | PDF §3 all feature metrics + headline question |
| FR-10 | `fact_experiment_result` after a full synthetic matrix run has zero NULL values in columns `rmse_m`, `seg_f1`, `seg_precision`, `seg_recall`, `continuity`, `boundary_err_s` for every (method, noise) cell. | must | FR-09 verification |
| FR-11 | Postgres view `v_pareto_frontier` exposes `(method, noise, accuracy_y, cost_x, efficiency)` with one row per cell. | must | PDF §1 headline question |
| FR-12 | New Grafana dashboard `08_accuracy_cost.json` exists with uid `etl4-accuracy-cost`, provisioned into the ETL4 folder, including at minimum: an XY scatter panel on `v_pareto_frontier`, a per-metric heatmap of `fact_experiment_result`, and the scale-curve panels specified in FR-15. | must | PDF §3 evaluation approach |
| FR-13 | New Postgres table `scale_runs(scale_run_id, data_volume_rows, method, wall_secs, rows_per_sec, peak_rss_mb, shuffle_mb, recorded_at)` persists one row per (volume, method). | must | PDF §3 "Throughput", "CPU and memory utilization", "Scaling efficiency" |
| FR-14 | New CLI `etl4/bench/scale_study.py` runs each method at the volumes `[100_000, 1_000_000, 10_000_000, 50_000_000]`, measures wall seconds and peak RSS via `psutil`, writes `shuffle_mb` when available (Spark path), inserts into `scale_runs`, is idempotent on re-run of identical volumes. | must | FR-13 driver |
| FR-15 | Dashboard 08 includes line panels of `rows_per_sec` and `wall_secs` vs `data_volume_rows`, grouped by `method`, driven by `scale_runs`. | must | PDF §3 scaling efficiency reporting |
| FR-16 | Unit tests cover every new metric with at least one positive, one negative, one boundary (empty inputs), one null-safety, and one determinism case, and every pre-existing test in `etl4/tests/test_pipeline.py` continues to pass. | must | CLAUDE.md §3.8 |

## Non-functional requirements

| ID | Category | Requirement | Measurement |
|---|---|---|---|
| NFR-01 | determinism | All stages reproducible from inputs + seed=42. | identical output hashes across two runs with the same inputs |
| NFR-02 | dtype | `icao24` is pandas string dtype at every boundary, never `object` or `Int64`. | unit test asserts `.dtype.name == 'string'` after every transform |
| NFR-03 | platform | Windows is a first-class target. No `→`, em-dash, or non-cp1252 chars emitted to stdout. | smoke run under cp1252 locale emits no UnicodeEncodeError |
| NFR-04 | performance | Synthetic matrix at 80k rows/cell × 12 cells completes in under 5 minutes on the current host. | `time python etl4/bench/run_experiment.py --source synthetic --profiles N0 N1 N2 N3` |
| NFR-05 | idempotency | Re-running `run_experiment.py` with identical args overwrites the prior cell (no row duplication on `pipeline_runs`). | query `SELECT COUNT(*) FROM pipeline_runs WHERE method=? AND noise=?` is 1 per cell after two consecutive runs |
| NFR-06 | reliability | New metrics gracefully return NaN (persisted as NULL) when ground truth is unavailable (real-OpenSky path); do not raise. | unit test with `truth=None` returns a value, does not raise |
| NFR-07 | observability | Every metric computation logs one line with its timing to stdout, cp1252-safe. | grep stdout for `[metric] ... t=... s` lines |
| NFR-08 | security | No secrets (OAuth tokens, DSNs) appear in any new file. Postgres DSN sourced from `POSTGRES_DSN` env with placeholder default. | pre-commit scan of staged diff |
| NFR-09 | footprint | No large data commits. `data/raw`, `data/bronze`, `data/silver`, `data/gold`, `data/eval`, `logs/`, `**/_shards/`, `**/_duckdb_tmp/`, `**/_DONE` remain gitignored. | `git status --short` after work shows only source + schema + doc + dashboard changes |
| NFR-10 | memory | `position_rmse_m` and `coverage_ratio` avoid `df.apply(axis=1)` on hot paths; use vectorised joins and numpy. | code review |
| NFR-11 | attribution | No "Co-Authored-By: Claude" trailer and no "Generated with Claude Code" PR footer. | pre-commit scan |
| NFR-12 | cost | Token spend minimised; agents dispatched per phase only when needed. | orchestrator's status line reports specialists dispatched |

## Acceptance criteria

| ID | For FR | Condition | How verified |
|---|---|---|---|
| AC-01 | FR-01 | `from etl4.bench.noise import inject_noise, NoiseTruth; _, truth = inject_noise(df, 'N2'); assert set(truth.jumped_ids).issubset(set(df['__truth_id']))` passes. | unit test |
| AC-02 | FR-02 | Synthetic `generate_fleet_day` output has `df['__truth_id'].is_monotonic_increasing` true and unique. | unit test |
| AC-03 | FR-03 | `segmentation_prf1([100, 500], [101, 502], tolerance_s=5)` returns `precision==1.0, recall==1.0, f1==1.0`; with `[100, 999]` recall drops to 0.5. | unit test |
| AC-04 | FR-04 | Mean of `abs(pred - true)` over matched pairs equals the returned value within 1e-9. | unit test |
| AC-05 | FR-05 | Perfect reconstruction yields RMSE 0.0; a single 1-degree-latitude offset on one row of a 1-row frame yields RMSE ≈ 111 km. | unit test |
| AC-06 | FR-06 | `coverage_ratio(full_reconstruction, truth)` returns 1.0; `coverage_ratio(empty_df, truth)` returns 0.0. | unit test |
| AC-07 | FR-07 | Aggregator returns `(max_m, mean_m)` matching hand-computed values on a 3-row fixture. | unit test |
| AC-08 | FR-08 | `deviation_prf1(flagged={10, 20}, jumped={10, 30}, row_ids=range(50))` returns precision=0.5, recall=0.5, f1=0.5. | unit test |
| AC-09 | FR-09 | After `python etl4/bench/run_experiment.py --source synthetic --profile N1 --method kinematic`, the resulting `rec` dict contains all six new keys with finite numeric values. | integration test |
| AC-10 | FR-10 | `SELECT COUNT(*) FROM fact_experiment_result WHERE source='synthetic' AND (rmse_m IS NULL OR seg_f1 IS NULL OR seg_precision IS NULL OR seg_recall IS NULL OR continuity IS NULL OR boundary_err_s IS NULL)` returns 0 after full matrix. | functional test |
| AC-11 | FR-11 | `SELECT COUNT(*) FROM v_pareto_frontier` equals the number of synthetic matrix cells (12 at default scope). | functional test |
| AC-12 | FR-12 | `curl -s -u admin:admin http://localhost:3000/api/search?query=Accuracy%20Cost` returns a dashboard with uid `etl4-accuracy-cost`; panel count ≥ 5. | functional test |
| AC-13 | FR-13 | `\d scale_runs` lists all eight specified columns with the specified types. | psql inspection |
| AC-14 | FR-14 | `python etl4/bench/scale_study.py --methods time_gap kinematic kinematic_smoothing --volumes 100000 1000000 10000000 50000000` inserts 12 rows into `scale_runs`; re-running the same command does NOT duplicate rows. | functional test |
| AC-15 | FR-15 | Panels render non-empty data after FR-14 completes; refresh interval matches dashboard default. | UI test by functional-tester |
| AC-16 | FR-16 | `pytest etl4/tests/ -v` reports zero failures and all new metric tests appear in the pass list. | test engineer |

## Requirement Traceability Matrix

| FR | ACs | Architecture section | Code module | Test cases |
|---|---|---|---|---|
| FR-01 | AC-01 | §Ground-truth plumbing | `etl4/bench/noise.py` | `test_noise_truth_*` |
| FR-02 | AC-02 | §Ground-truth plumbing | `etl4/ingest/synthetic.py` | `test_synthetic_truth_id_*` |
| FR-03 | AC-03 | §Metric engine | `etl4/evaluate/metrics.py::segmentation_prf1` | `test_segmentation_prf1_*` |
| FR-04 | AC-04 | §Metric engine | `etl4/evaluate/metrics.py::boundary_time_error` | `test_boundary_time_error_*` |
| FR-05 | AC-05 | §Metric engine | `etl4/evaluate/metrics.py::position_rmse_m` | `test_position_rmse_*` |
| FR-06 | AC-06 | §Metric engine | `etl4/evaluate/metrics.py::coverage_ratio` | `test_coverage_ratio_*` |
| FR-07 | AC-07 | §Metric engine | `etl4/evaluate/metrics.py::cross_track_from_reference` | `test_cross_track_aggregator_*` |
| FR-08 | AC-08 | §Metric engine | `etl4/evaluate/metrics.py::deviation_prf1` | `test_deviation_prf1_*` |
| FR-09 | AC-09 | §Matrix integration | `etl4/bench/run_experiment.py` | `test_run_experiment_populates_metrics` |
| FR-10 | AC-10 | §Matrix integration + §Sink | `etl4/sink/postgres.py` (unchanged plumbing), `etl4/infra/create_tables.sql` | SQL check by functional-tester |
| FR-11 | AC-11 | §Pareto view + panel | `etl4/infra/create_tables.sql` (view DDL) | SQL check |
| FR-12 | AC-12, AC-15 | §Dashboard | `etl4/dashboard/08_accuracy_cost.json` | Grafana API check |
| FR-13 | AC-13 | §Scale study | `etl4/infra/create_tables.sql` (table DDL) | psql check |
| FR-14 | AC-14 | §Scale study | `etl4/bench/scale_study.py` | integration test |
| FR-15 | AC-15 | §Dashboard | `etl4/dashboard/08_accuracy_cost.json` | UI test |
| FR-16 | AC-16 | §Test coverage | `etl4/tests/test_pipeline.py` + `etl4/tests/test_metrics.py` (new) | pytest run |

Traceability coverage: 16 / 16 FRs trace to at least one AC, one architecture section, one code module, and one test case = **100%**.

## Open questions

- Does `NoiseTruth` need to persist to disk for later analysis, or is in-memory per-run sufficient? **Proposed default: in-memory only. Data Engineer to confirm in architecture.**
- Scale-study volume 50M may exceed single-node RSS for `kinematic_smoothing` on this host; cap at 10M for that method if peak_rss_mb > 12_000? **Proposed default: skip-and-log rather than abort. Data Engineer to confirm.**
- Should `deviation_prf1` ingest the smoothed-Kalman residual mask that already exists in `reconstruct/deviations.py`, or compute its own flagged mask inside `run_experiment.py`? **Proposed default: wrap the existing residual mask. Data Engineer to confirm.**

Open questions are not blockers for approval; they are decisions the Data Engineer owns in the next phase.

## Status

`APPROVED`
