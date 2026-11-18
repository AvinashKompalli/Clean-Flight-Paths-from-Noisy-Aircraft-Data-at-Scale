# Test Report - PROJ-20261114-wi1-metrics

**Authored by**: functional-tester
**Date**: 2026-11-18T04:20:00Z

## Scope

Independent end-to-end validation of Work Item 1 (feature-metric matrix completion + Pareto-frontier view + scale study) against the 16 acceptance criteria in `requirements.md`. Covers:

- Matrix e2e (3 methods x 4 noise profiles on synthetic bronze)
- `stage_eval_mart` refresh and population of six PDF-mandated metrics in `fact_experiment_result`
- `v_pareto_frontier` row count
- `scale_study` at three volumes (10k, 100k, 1M) per orchestrator directive (10M/50M excluded by instruction)
- Grafana dashboard `etl4-accuracy-cost` panel-by-panel SQL smoke-tests plus 200/panel-count smoke on the other six ETL4 dashboards
- Full pytest regression suite under `etl4/tests/`
- `etl4.qa.reconcile` row-count sanity

Explicitly excluded: scale_study at 10M / 50M (per instructions); writing production code or fixing defects (prohibited).

## Environment

- Python / conda env: Python 3.12, active env `opensky` (CLAUDE.md names `etl4`; `opensky` resolves the same deps per user memory)
- Postgres state (post-matrix + post-eval_mart + post-scale_study):
  - `pipeline_runs`: 55 rows (12 new synthetic runs id=43..54 appended)
  - `fact_experiment_result`: 25 rows (12 synthetic + 12 s3 + 1 zenodo)
  - `eval_results`: 19 populated cells per new metric out of 55 pipeline_runs (unchanged - 12 new runs wrote only `continuity / n_flights / n_deviations`)
  - `scale_runs`: 15 rows (6 pre-existing + 9 fresh: 3 methods x 3 volumes)
  - `v_pareto_frontier`: 0 rows **[regression from 12 - see DEF-001]**
- Grafana / infra state: full docker-compose stack up (etl4-postgres, etl4-grafana, etl4-kafka, etl4-prometheus, etl4-airflow, etl4-spark-master, etl4-spark-worker-1/2, etl4-node-exporter). Grafana `/api/dashboards/uid/<uid>` returns 200 for all 7 dashboards.
- Data volumes exercised:
  - Synthetic bronze: 14,025 rows (generated 2026-11-09 17:02 - PRE-T-002, no `__truth_id` column)
  - `data/bronze/raw_states` (S3 slice): 500k rows cap per volume as spec'd by scale_study.py

## Results summary

| Category | Total | Passed | Failed | Skipped | Coverage |
|---|---|---|---|---|---|
| Functional (A matrix e2e)   | 1  | 1  | 0 | 0 | wall 10.8s, 12 pipeline_runs appended |
| Functional (B eval_mart)    | 1  | 0  | 1 | 0 | fact_experiment_result 12 rows, 6 metrics all NULL |
| Functional (C pareto view)  | 1  | 0  | 1 | 0 | 0 rows, expected 12 |
| Functional (D scale study)  | 1  | 1  | 0 | 0 | 9 fresh + 6 prior = 15 rows, no 1M skip-and-log |
| Dashboard   (E UI/UX)       | 7  | 6  | 1 | 0 | Pareto panel empty, remaining panels populated or appropriately zero |
| Regression  (F pytest)      | 34 | 33 | 0 | 1 | T-002 round-trip test still skipped |
| Functional (G reconcile)    | 1  | 0  | 1 | 0 | pre-existing flightlists drop 20.7% > 2% tol |
| **Total**                   | 46 | 41 | 4 | 1 | |

## Test A - Matrix e2e PASS

Command (orchestrator's command augmented with the required `--bronze` arg that was missing from the brief):
```
python etl4/bench/run_matrix.py --bronze data/bronze/synthetic --source synthetic --profiles N0 N1 N2 N3 --methods time_gap kinematic kinematic_smoothing --workers 1
```
Wall time: `real 0m10.805s` (NFR-04 target: < 5 min - PASS).

Evidence (tail):
```
[pass 1] done in 0.3s
[pass 2] 12 reconstruct tasks, workers=1
  >>> noise=N0  method=time_gap           [postgres run_id=43]  wall=0.169s  flights=40   rows_out=13955
  >>> noise=N0  method=kinematic          [postgres run_id=44]  wall=0.221s  flights=86   rows_out=13955
  >>> noise=N0  method=kinematic_smoothing[postgres run_id=45]  wall=1.319s  flights=40   rows_out=13955
  ...
  >>> noise=N3  method=kinematic_smoothing[postgres run_id=54]  wall=1.185s  flights=40   rows_out=12626
[pass 2] done in 8.8s
wrote data\eval\matrix.csv
```

Twelve new `pipeline_runs` rows written as expected. Silver tree written per `(source, noise)`; gold tree written per `(source, noise, method)`.

Deviation from brief: the orchestrator's command omitted `--bronze`; the CLI requires it and errored on first invocation. The correct form is `--bronze data/bronze/synthetic`. Flagged as a brief error, not a product defect.

## Test B - eval_mart six-metric population FAIL [CRITICAL]

Command:
```
python -c "from etl4.run_e2e import stage_eval_mart; stage_eval_mart()"
```
Output: `eval_mart: ok  rows=25  elapsed=0.5s` (exit 0).

Query (verbatim from brief):
```sql
SELECT method, noise,
       ROUND(rmse_m::numeric,1) AS rmse, ROUND(seg_f1::numeric,3) AS f1,
       ROUND(seg_precision::numeric,3) AS prec, ROUND(seg_recall::numeric,3) AS rec,
       ROUND(coverage_ratio::numeric,3) AS cov, ROUND(boundary_err_s::numeric,2) AS b_err
  FROM fact_experiment_result WHERE source='synthetic' ORDER BY noise, method;
```

Result (12 rows, **every one of the six metric columns is NULL**):
```
       method        | noise | rmse | f1 | prec | rec | cov | b_err
---------------------+-------+------+----+------+-----+-----+-------
 kinematic           | N0    |      |    |      |     |     |
 kinematic_smoothing | N0    |      |    |      |     |     |
 time_gap            | N0    |      |    |      |     |     |
 kinematic           | N1    |      |    |      |     |     |
 kinematic_smoothing | N1    |      |    |      |     |     |
 time_gap            | N1    |      |    |      |     |     |
 kinematic           | N2    |      |    |      |     |     |
 kinematic_smoothing | N2    |      |    |      |     |     |
 time_gap            | N2    |      |    |      |     |     |
 kinematic           | N3    |      |    |      |     |     |
 kinematic_smoothing | N3    |      |    |      |     |     |
 time_gap            | N3    |      |    |      |     |     |
```

Verification of root cause (`eval_results` keyed by run_id):
```
 run_id | method              | noise | metrics present
     35 | time_gap            | N0    | boundary_err_s,continuity,coverage_ratio,n_deviations,n_flights,position_rmse_m,seg_f1,seg_precision,seg_recall  (prior run, full 9)
     43 | time_gap            | N0    | continuity,n_deviations,n_flights                                                                                   (new run, 3 only)
     ...
     54 | kinematic_smoothing | N3    | continuity,n_deviations,n_flights                                                                                   (new run, 3 only)
```
`stage_eval_mart` takes `DISTINCT ON (source, noise, method) ... ORDER BY started_at DESC`, so my fresh run 43..54 wins over the prior fully-populated runs 35..40, and the mart drops back to NULL for the six headline metrics. This also collapses `v_pareto_frontier` (Test C).

Diagnosis: `run_matrix.py` wiring gap. `reconstruct_from_silver(... truth=None ...)` guards the six metrics behind `if source == "synthetic" and truth is not None and not truth.empty:` (etl4/bench/run_experiment.py:177). Because `run_matrix.py` does not pass `--truth`, and because the current `data/bronze/synthetic` lacks `__truth_id` (parquet mtime 2026-11-09, i.e. pre-T-002), the only working truth-derivation path (`run_experiment` wrapper line 223) is bypassed by the matrix runner. The `NoiseTruth` sidecar is correctly built and threaded through the pool, but the guard never lets it reach the scorers.

AC-10 FAIL. Severity: CRITICAL.

## Test C - v_pareto_frontier FAIL [CRITICAL]

```sql
SELECT method, noise, ROUND(accuracy_y::numeric,1) AS rmse, ROUND(cost_x::numeric,3) AS wall_s FROM v_pareto_frontier ORDER BY noise, method;
```
Result: **0 rows** (expected 12 per AC-11).

The view predicate `WHERE fer.rmse_m IS NOT NULL AND fer.wall_secs IS NOT NULL` filters every row because `rmse_m` is NULL in `fact_experiment_result` (see Test B). Same root cause.

AC-11 FAIL. Severity: CRITICAL (follows from Test B).

## Test D - Scale study PASS

Command (orchestrator's command corrected: `--methods` must be comma-separated per CLI parser, the space-separated form fails with `unrecognized arguments`):
```
python -m etl4.bench.scale_study --volumes 10000,100000,1000000 --methods time_gap,kinematic,kinematic_smoothing --bronze data/bronze/raw_states
```
Wall time: `real 1m55.809s`.

Final table (15 rows = 6 pre-existing + 9 new; zero notes column populated at any volume including 1M):
```
       method        | data_volume_rows | wall_s  |    rps    | peak_rss_mb | notes
---------------------+------------------+---------+-----------+-------------+-------
 kinematic           |            10000 |  0.021  |   447787  |     933     |
 kinematic           |           100000 |  0.067  |  1388256  |     972     |
 kinematic           |          1000000 |  0.620  |  1507541  |    1289     |
 kinematic_smoothing |            10000 |  5.421  |     1718  |     940     |
 kinematic_smoothing |           100000 | 19.459  |     4804  |     937     |
 kinematic_smoothing |          1000000 | 78.239  |    11937  |    1461     |
 time_gap            |            10000 |  0.017  |   555751  |    1399     |
 time_gap            |           100000 |  0.059  |  1590508  |    1061     |
 time_gap            |           1000000|  0.598  |  1562477  |    1263     |
 (pre-existing 6 rows retained)
```

AC-13 (table shape) PASS: all 8 spec columns (`scale_run_id, data_volume_rows, method, wall_secs, rows_per_sec, peak_rss_mb, shuffle_mb, recorded_at`) present; 13 cols total (extended per architecture).
AC-14 (12-row idempotent target) PARTIAL: 9 rows for the 3 completed volumes. 10M + 50M excluded per orchestrator instruction; the skip-and-log gate was not exercised in this run (not a defect for this test battery, but the full AC remains unverified end-to-end).

Deviation from brief: the orchestrator's CLI form `--methods time_gap kinematic kinematic_smoothing` is rejected by the parser (space-separated args); the working form is `--methods time_gap,kinematic,kinematic_smoothing`. Flagged.

## Test E - Dashboard UX for `etl4-accuracy-cost`

Discovery:
```
$ curl -s -u admin:admin "http://localhost:3000/api/dashboards/uid/etl4-accuracy-cost"
title: ETL4 - Accuracy vs Cost (WI-1)   uid: etl4-accuracy-cost   panel_count: 9
```
9 panels in the ETL4 folder. AC-12 PASS (panel count >= 5).

Panel-by-panel SQL smoke (queries fetched verbatim from dashboard JSON, run via `docker exec etl4-postgres psql`):

| # | Type    | Title                                                 | Rows | Verdict |
|---|---------|-------------------------------------------------------|------|---------|
| 1 | text    | Accuracy-cost trade-off - PDF headline question       | n/a  | OK      |
| 2 | xychart | Pareto scatter - rmse_m vs wall_secs                  | 0    | FAIL (expected >= 12; blocked by Test B/C) |
| 3 | table   | Metric heatmap - seg_f1 by method x noise (continuity fallback) | 3 rows x 4 cols = 12 cells | PASS (>= 12 cells) |
| 4 | xychart | Throughput scale curve rows_per_sec vs volume         | 15   | PASS (> 0) |
| 5 | xychart | Latency scale curve wall_secs vs volume               | 15   | PASS (> 0) |
| 6 | xychart | Memory scale curve peak_rss_mb vs volume              | 15   | PASS (> 0) |
| 7 | table   | Skip-and-log evidence (notes IS NOT NULL)             | 0    | PASS (expected 0 - no gate fired) |
| 8 | bargauge| Metric completeness per metric                        | 6    | PASS (all six metrics present; 19/55 populated) |
| 9 | text    | How to read these panels                              | n/a  | OK      |

Panel 3 heatmap verbatim (continuity fallback; MEDIUM note: seg_f1 column itself is NULL because of the Test B defect):
```
       method        |       clean        |       light        |      moderate      |       heavy
---------------------+--------------------+--------------------+--------------------+--------------------
 kinematic           | 0.9985629807581883 | 0.9786255330302658 | 0.9483745460022263 | 0.8976403243809427
 kinematic_smoothing | 0.9983253963903160 | 0.9782746048339567 | 0.9484393415169966 | 0.8986794952609908
 time_gap            | 0.9982263526080405 | 0.9782489746446629 | 0.9483603671885589 | 0.8985542905057499
```

Other dashboards (one representative SQL / Prometheus query per dashboard; API 200 for all seven):
- `etl4-analyst-view` (13 panels): `SELECT count(*) FROM dim_aircraft;` -> 149,747 rows PASS
- `etl4-pipeline-results` (8 panels): latest wall_secs query returns 12 rows across method/noise PASS
- `etl4-de-pipeline` (13 panels): `SELECT SUM(rows) FROM pipeline_volumes WHERE layer IN (...);` -> 333,366,517 PASS
- `etl4-flight-paths` (7 panels): `SELECT count(*) FROM fact_trajectory_enriched;` -> 4,374,016 rows PASS
- `etl4-ingest-comparison` (6 panels): `ingest_runs GROUP BY source, pattern` -> 4 rows PASS
- `etl4-ingest-throughput` (4 panels): `sum by (job) (rate(records_sent_total[1m]))` -> empty vector (live_producer not running - documented, not a fail)
- `etl4-accuracy-cost` (9 panels): see detail above

AC-12 PASS, AC-15 PARTIAL (scale curves populate; Pareto scatter empty due to Test B/C).

Minor UX note (NON-cp1252 character): `etl4-flight-paths` panel title contains a Unicode character (ndash-like codepoint rendered as replacement in cp1252). This is in the dashboard title text only, not in any script output, so NFR-03 is not violated end-to-end; flagged as LOW cosmetic.

## Test F - pytest regression PASS

Command: `pytest etl4/tests/ -q`
Result: `33 passed, 1 skipped in 5.64s` (exit 0).

The one skipped test is `test_synthetic_truth_id_is_monotonic_unique_int64` in `etl4/tests/test_noise_truth.py`. Its skipif probe `_synthetic_emits_truth_id()` calls `make_flight(...)` and checks `TRUTH_ID in df.columns` - but `__truth_id` is added by `generate_synthetic()` / `synthetic.main()`, NOT by `make_flight()`. Verified at the Python REPL:
```
make_flight has __truth_id: False
generate_synthetic has __truth_id: True
monotonic_increasing: True
is_unique: True
```
The underlying AC-02 behaviour is satisfied, but the test still skips because its probe targets the wrong function. The orchestrator's brief asserted "The T-002 round-trip test now auto-unskips"; empirically it does not. Logged as DEF-002 (medium).

AC-16 PASS on the letter ("zero failures"), MEDIUM note on the spirit ("all new metric tests appear in the pass list" - one still skips).

## Test G - Reconcile FAIL (pre-existing, not WI-1 regression)

Command: `python -m etl4.qa.reconcile --tolerance 0.02`
Result (exit 1):
```
[reconcile] [FAIL] bronze_flightlists -> vault.hub_flight   upstream=117,258,215  downstream=92,929,010  drop=20.748%  tol=2.0%
[reconcile] [OK  ] bronze_raw_states -> vault.sat_state_observation  upstream=4,374,016  downstream=4,374,016  drop=0.0%  tol=2.0%
[reconcile] [OK  ] vault.sat_state_observation -> mart.fact_trajectory_enriched  upstream=4,374,016  downstream=4,374,016  drop=0.0%  tol=2.0%
[reconcile] summary: 2 ok, 1 fail, 0 skip
```

The 20.7 % flightlists drop is pre-existing (bronze layer is already out of balance with the hub_flight vault table; no WI-1 change touches those ingest paths). Logged as DEF-003 (low, pre-existing, out of scope for WI-1 sign-off).

## Pass list

- Test A - matrix e2e wall=10.8s, 12 pipeline_runs appended
- Test D - scale_study 9 fresh rows written, no gate firing at 1M
- Test E panels 1, 3-9 (8 of 9) render correctly against live data
- Test E dashboards etl4-analyst-view, etl4-pipeline-results, etl4-de-pipeline, etl4-flight-paths, etl4-ingest-comparison (all 200, sample query > 0 rows)
- Test E etl4-ingest-throughput 200, 4 panels, Prometheus vector empty documented acceptable
- Test F pytest 33 passed (AC-01, 03, 04, 05, 06, 07, 08, 16 verified)
- AC-12 dashboard discovery + panel count 9 PASS
- AC-13 scale_runs schema PASS (all 8 spec cols)

## Failure details

### DEF-001: `fact_experiment_result` 6 PDF-mandated metrics NULL after full matrix [CRITICAL]

- Expected (AC-10): `SELECT COUNT(*) FROM fact_experiment_result WHERE source='synthetic' AND (rmse_m IS NULL OR seg_f1 IS NULL OR seg_precision IS NULL OR seg_recall IS NULL OR continuity IS NULL OR boundary_err_s IS NULL)` returns 0.
- Got: 12 (every synthetic cell has all six headline-metric columns NULL).
- Also fails AC-11 (`v_pareto_frontier` 0 rows, expected 12).
- Also degrades AC-15 (Pareto scatter panel renders empty; scale curves fine).
- Error class: WIRING. `run_matrix.py` does not pass a truth frame to `reconstruct_from_silver`; the current synthetic bronze (`data/bronze/synthetic`, mtime 2026-11-09) pre-dates T-002 and therefore has no `__truth_id` column, so the `run_experiment()` wrapper's truth-from-bronze fallback (run_experiment.py:223) is also unavailable; the `NoiseTruth` sidecar IS built and passed through the pool, but the metric block is gated on `truth is not None` (run_experiment.py:177) and so is never entered.
- Likely cause: two separable regressions acting together. (1) The synthetic bronze on disk was generated before T-002 added `__truth_id` emission; no one regenerated it after T-002 landed. (2) The guard at run_experiment.py:177 requires a truth frame even when a complete `NoiseTruth` sidecar is available - the sidecar already carries `original_coords` and `original_ids` that are sufficient for position_rmse_m / coverage_ratio / segmentation_prf1 / boundary_time_error_s. A `_synthesise_noise_truth` path exists but is only reached inside the truth != None branch.
- Severity: CRITICAL. Blocks the WI-1 headline deliverable (Pareto frontier answers the PDF question).
- Suggested fix owner: data-engineer. Two options: (a) regenerate `data/bronze/synthetic` with `python etl4/ingest/synthetic.py --hours 1 --date 2026-01-01 --out data/bronze/synthetic` so the wrapper fallback kicks in; (b) relax run_experiment.py:177 so the sidecar alone is sufficient and `_synthesise_noise_truth` can populate from `NoiseTruth.original_coords`. (a) is the smaller patch for this gate; (b) is the correct long-term fix.

### DEF-002: T-002 round-trip test still auto-skips [MEDIUM]

- Expected (orchestrator brief + CLAUDE.md §3.8 spirit): `test_synthetic_truth_id_is_monotonic_unique_int64` unskips post-T-002 and passes.
- Got: still skipped. 33 passed, **1 skipped**, 0 failed.
- Error class: TEST BUG. `_synthetic_emits_truth_id()` in `etl4/tests/test_noise_truth.py:206` probes `make_flight(...)` which never emitted `__truth_id` (that column is added by the outer `generate_synthetic()` / `synthetic.main()` on top of `make_flight` rows). The skipif predicate is wrong.
- The underlying invariant AC-02 is actually satisfied at runtime (REPL confirms `generate_synthetic(...)[TRUTH_ID]` is monotonic and unique), but no test asserts it in CI.
- Severity: MEDIUM (invariant is met but coverage gap persists; AC-16 spirit unmet).
- Suggested fix owner: unit-tester. Point the probe and the test body at `generate_synthetic` (or add a fresh test that does), remove the skipif.

### DEF-003: Reconcile `bronze_flightlists -> vault.hub_flight` drop 20.7 % [LOW, pre-existing]

- Expected: drop <= 2 % per `--tolerance 0.02`.
- Got: 20.748 % drop.
- Pre-existing: WI-1 does not touch flightlists ingest or the hub_flight vault layer.
- Severity: LOW (unrelated to WI-1 scope, but visible on every `run_e2e` and should be tracked separately).
- Suggested fix owner: data-engineer, separate ticket.

## Regression status

Zero pre-existing tests broken. Pytest went from the baseline 32 pass / 1 skip / 1 pre-existing fail to **33 pass / 1 skip / 0 fail** — the previously-failing `test_matrix_csv_written_after_every_cell` now passes (fix noted in commit 01eedd5). The only remaining skip is DEF-002, which is a stale test probe, not a regression.

## Evidence paths

- Commands run (all absolute):
  - `python etl4/bench/run_matrix.py --bronze data/bronze/synthetic --source synthetic --profiles N0 N1 N2 N3 --methods time_gap kinematic kinematic_smoothing --workers 1` - log: `C:/Users/avina/AppData/Local/Temp/claude/B--IISC-DE-Proj/8e461c31-3453-4d7e-abd5-ad362a41f2ee/scratchpad/matrix_run.log`
  - `python -c "from etl4.run_e2e import stage_eval_mart; stage_eval_mart()"` - log: `C:/Users/avina/AppData/Local/Temp/claude/B--IISC-DE-Proj/8e461c31-3453-4d7e-abd5-ad362a41f2ee/scratchpad/eval_mart.log`
  - `python -m etl4.bench.scale_study --volumes 10000,100000,1000000 --methods time_gap,kinematic,kinematic_smoothing --bronze data/bronze/raw_states` - log: `C:/Users/avina/AppData/Local/Temp/claude/B--IISC-DE-Proj/8e461c31-3453-4d7e-abd5-ad362a41f2ee/scratchpad/scale_study.log`
  - `pytest etl4/tests/ -q` - log: `C:/Users/avina/AppData/Local/Temp/claude/B--IISC-DE-Proj/8e461c31-3453-4d7e-abd5-ad362a41f2ee/scratchpad/pytest.log`
  - `python -m etl4.qa.reconcile --tolerance 0.02` - log: `C:/Users/avina/AppData/Local/Temp/claude/B--IISC-DE-Proj/8e461c31-3453-4d7e-abd5-ad362a41f2ee/scratchpad/reconcile.log`
  - `curl -s -u admin:admin "http://localhost:3000/api/dashboards/uid/etl4-accuracy-cost"` - response: `C:/Users/avina/AppData/Local/Temp/claude/B--IISC-DE-Proj/8e461c31-3453-4d7e-abd5-ad362a41f2ee/scratchpad/dashboard-accuracy-cost.json`
- Panel-by-panel SQL results and dashboard-by-dashboard discovery results captured inline above.

## Recommendation

`FAILED`

## Next action

- Defect triage: DEF-001 (critical) back to data-engineer. Preferred fix is to relax the `truth is not None` guard in `etl4/bench/run_experiment.py:177` so a `NoiseTruth` sidecar alone is sufficient (which the current code *almost* supports via `_synthesise_noise_truth`), followed by a full `etl4/bench/run_matrix.py --bronze data/bronze/synthetic --source synthetic ...` rerun and `stage_eval_mart` refresh. The cheaper interim fix is to regenerate `data/bronze/synthetic` with the post-T-002 `etl4/ingest/synthetic.py` so `__truth_id` is on disk and the existing wrapper fallback activates; this restores AC-10/AC-11 immediately but leaves the matrix path brittle to the next bronze regeneration.
- Defect triage: DEF-002 (medium) to unit-tester - fix the skipif probe and ensure AC-02 is actually asserted in CI.
- Defect triage: DEF-003 (low, pre-existing) tracked out-of-scope.
- Retest after DEF-001 fix: full battery A-E re-run; expect `fact_experiment_result` zero NULLs across six metrics, `v_pareto_frontier` 12 rows, Pareto scatter panel populated.
- Not yet ready for product-owner acceptance.
