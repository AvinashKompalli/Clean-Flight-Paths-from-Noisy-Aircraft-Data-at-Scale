# Architecture - PROJ-20261114-wi1-metrics

**Status**: `APPROVED` - PO's `requirements.md` landed during authoring with 3 conditions on their REQUIREMENTS approval; all three confirmed by the data-engineer below.
**Authored by**: data-engineer
**Date**: 2026-11-14T02:11Z
**Based on**: `requirements.md` (PO-APPROVED 2026-11-14T02:10Z with conditions) + user-confirmed scope captured in `state.json.source_request`

## PO conditions addressed

1. **"NoiseTruth in-memory per run (no disk persistence)"** - CONFIRMED. The `NoiseTruth` dataclass is constructed inside `inject_noise`, forwarded in-process to `reconstruct_from_silver`, consumed by `metrics.*`, then goes out of scope with the Python interpreter at end of run. No parquet, no pickle, no sidecar file is written. It is the ground-truth sidecar for one matrix cell, not a persisted artifact.
2. **"50M scale-study volume skip-and-log for kinematic_smoothing if peak_rss_mb > 12000"** - CONFIRMED and folded into task T-005 as a hard gate: `scale_study.py` inspects `psutil.virtual_memory()` before launching a cell, estimates peak RSS from the prior volume cell for that method (linear extrapolation), and if the estimate for `kinematic_smoothing @ 50M` exceeds 12 GB, the cell is **skipped** with a NULL row inserted into `scale_runs` tagged `notes='skipped: projected_peak_rss_mb>12000'`. Dashboard 08 filter clauses handle NULLs so the scale curve has a visible gap rather than a crash.
3. **"deviation_prf1 wraps existing reconstruct/deviations.py residual mask rather than recomputing"** - CONFIRMED. `metrics.deviation_prf1(flagged_mask, jumped_ids, row_ids) -> dict` accepts the **mask that `reconstruct/deviations.py::detect_deviations` already produces** (specifically the residual-gate mask when Kalman is used; the time/position gate mask otherwise). No new detector logic lives in `metrics.py`; it is a scorer against `NoiseTruth.jumped_ids`.

## Decision summary

Three thin, additive changes to the existing experiment pipeline turn a half-populated metric matrix into a full one. **No engine change, no rewrite of the matrix runner.** `etl4/evaluate/metrics.py` already contains the scoring functions; the gap is upstream (ground truth is thrown away) and in two small SQL artifacts.

Approach: (1) teach `inject_noise` to return a `NoiseTruth` describing exactly which rows it touched, (2) add a stable `__truth_id` to synthetic generation so correspondence survives `clean + event_time_order + reconstruct`, (3) wire the existing metric functions into `run_experiment.py` so every (method × noise) cell on the synthetic path computes the full set, (4) add `v_pareto_frontier` view + `scale_runs` table + `scale_study.py` CLI.

## Component diagram

```
 synthetic.py                          noise.py                                            run_experiment.py
 ------------                          --------                                            -----------------
 make_flight() -> clean_df  ---->  inject_noise(clean_df, profile, seed)   -->  noisy_df   reconstruct_from_silver(silver, method, ..., truth=clean_df)
 (adds __truth_id int col)             (returns noisy_df, NoiseTruth)                      (uses truth + NoiseTruth to call metrics.py)
                                                                                                         |
                                                                                                         v
                                                                              metrics.py: position_rmse_m, segmentation_prf, boundary_time_error_s,
                                                                                         coverage_ratio, deviation_prf  [ALL EXIST]
                                                                                                         |
                                                                                                         v
                                                                              sink/postgres.py::write_run -> pipeline_runs + eval_results
                                                                                                         |
                                                                                                         v
                                                                              v_pareto_frontier VIEW -> dashboard 08_accuracy_cost.json
                                                                              scale_runs TABLE       <- scale_study.py CLI
```

## Data contracts

### `NoiseTruth` dataclass (in `etl4/bench/noise.py`)

```python
from dataclasses import dataclass, field
import pandas as pd

@dataclass(frozen=True)
class NoiseTruth:
    profile: str                              # "N0".."N3"
    seed: int                                 # 42 by default
    original_row_count: int
    original_coords: pd.DataFrame             # columns: __truth_id, lat, lon, baroaltitude
    dropped_ids: frozenset[int]               # __truth_ids removed by gap step
    duplicated_ids: frozenset[int]            # __truth_ids that have a second copy present
    shuffled_ids: frozenset[int]              # __truth_ids whose positional index moved
    jumped_ids: frozenset[int]                # __truth_ids whose lat/lon were perturbed
    true_boundaries: list[tuple]              # [(icao24, time), ...] per-flight start times from the clean df

    def surviving_ids(self) -> frozenset[int]:
        return frozenset(range(self.original_row_count)) - self.dropped_ids
```

Rationale: frozensets make equality/subset checks O(1) and are hashable so downstream caching works. `original_coords` is a DataFrame so `position_rmse_m` can join on `__truth_id` instead of `(icao24, time)` which can drift under noise.

### `__truth_id` column (in `etl4/ingest/synthetic.py`)

- Integer column added inside `make_flight` before concat.
- Monotonic across the generated set; `__truth_id = 0 .. N-1` where N is total rows across all flights before noise.
- **Must survive**: `clean.project_raw`, `clean.clean`, `event_time_order`, `kinematic_features`, every `reconstruct.*.segment_*` method, and the final gold write.
- Added to `data/reference/synthetic_truth/<date>.parquet`.
- NOT added to real (`rest`, `s3`) bronze — those sources do not have GT and the column must be absent (metrics skip when missing).

### New Postgres view `v_pareto_frontier`

```sql
CREATE OR REPLACE VIEW v_pareto_frontier AS
SELECT fer.method,
       fer.noise,
       fer.rmse_m           AS accuracy_y,
       fer.wall_secs        AS cost_x,
       fer.n_flights,
       fer.continuity,
       CASE WHEN fer.wall_secs > 0 THEN fer.rmse_m / fer.wall_secs ELSE NULL END AS efficiency
  FROM fact_experiment_result fer
 WHERE fer.rmse_m IS NOT NULL;
```

Dominated set computed in the Grafana scatter panel via color/opacity; not materialized.

### New Postgres table `scale_runs`

```sql
CREATE TABLE IF NOT EXISTS scale_runs (
  scale_run_id       BIGSERIAL PRIMARY KEY,
  recorded_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  data_volume_rows   BIGINT NOT NULL,
  source             TEXT NOT NULL,         -- 'synthetic' | 's3' | 'rest'
  method             TEXT NOT NULL,         -- 'time_gap' | 'kinematic' | 'kinematic_smoothing'
  noise              TEXT NOT NULL,         -- 'N0'..'N3'
  wall_secs          DOUBLE PRECISION,
  rows_per_sec       DOUBLE PRECISION,
  peak_rss_mb        BIGINT,
  shuffle_mb         BIGINT,                -- Spark-engine runs only; NULL for pandas
  engine             TEXT NOT NULL,         -- 'pandas' | 'spark' | 'duckdb'
  seed               INT NOT NULL DEFAULT 42,
  notes              TEXT
);
CREATE INDEX IF NOT EXISTS idx_scale_runs_volume ON scale_runs(data_volume_rows);
CREATE INDEX IF NOT EXISTS idx_scale_runs_method ON scale_runs(method);
```

## Pipeline spec (per transformation)

| # | Stage | Input | Output | Engine | Key transformation | Scale posture |
|---|---|---|---|---|---|---|
| 1 | `synthetic.main` | config (hours, n_aircraft) | bronze parquet + `synthetic_truth/<date>.parquet` | pandas | emits `__truth_id` as monotonic int | per-flight generation, O(N) |
| 2 | `noise.inject_noise` | clean df + profile + seed | (noisy_df, NoiseTruth) | pandas vectorised | gaps/dups/shuffle/jumps + truth tracking | O(N); mask-based operations |
| 3 | `clean.project_raw` + `clean.clean` + `event_time_order` | noisy_df | silver | pandas | **MUST preserve `__truth_id`** | existing |
| 4 | `kinematic_features` + `METHODS[method]` | silver | gold reconstructed | pandas | boundaries + optional Kalman | existing |
| 5 | `metrics.*` | gold + synthetic_truth + NoiseTruth | metrics dict | pandas | full 8-metric computation on synthetic path | existing functions |
| 6 | `sink.postgres.write_run` | metrics dict | `pipeline_runs`, `eval_results` rows | psycopg | `_PERSISTED_METRICS` already lists targets | existing |
| 7 | `bench.scale_study.main` | source bronze + volumes list | `scale_runs` rows + stdout log | pandas + psutil | sample at `[100k,1M,10M,50M]` per method | memory-bounded via `psutil.Process().memory_info().rss` |

## Files touched / files NOT to touch

### Touch
- `etl4/bench/noise.py` - add `NoiseTruth` dataclass + new signature returning tuple
- `etl4/ingest/synthetic.py` - emit `__truth_id`
- `etl4/bench/run_experiment.py` - thread `NoiseTruth` through, wire metrics for synthetic path
- `etl4/bench/run_matrix.py` - accept `NoiseTruth` return from `prepare_silver` (memoized)
- `etl4/bench/scale_study.py` - NEW
- `etl4/infra/create_tables.sql` - append `scale_runs` + `v_pareto_frontier`
- `etl4/dashboard/08_accuracy_cost.json` - NEW
- `etl4/tests/test_pipeline.py` - 5 new unit tests
- `etl4/docs/decisions.md` - ADR appended

### Do NOT touch
- `etl4/evaluate/metrics.py` - functions already correct; do not reshape them
- `etl4/sink/postgres.py` - `_PERSISTED_METRICS` already lists our targets
- `etl4/reconstruct/*` - method implementations stay as-is; only must preserve `__truth_id` pass-through
- Any marts / vault / Grafana dashboards other than the new one

## Development task breakdown

| Task ID | Agent | Description | Prerequisites | Acceptance |
|---|---|---|---|---|
| T-001 | developer-backend | Add `NoiseTruth` dataclass + new return contract in `noise.py`; update all callers | PO.APPROVED, this doc.APPROVED | `inject_noise` returns tuple; `run_experiment.py` + `run_matrix.py` compile and unit tests pass |
| T-002 | developer-backend | Add `__truth_id` emission in `synthetic.py`; ensure it round-trips through clean + event_time_order + reconstruct | T-001 (interface) | round-trip test shows IDs match end-to-end on synthetic path |
| T-003 | developer-backend | Wire `NoiseTruth` + truth parquet into `run_experiment.py::reconstruct_from_silver` so all metrics compute on synthetic path | T-001, T-002 | `fact_experiment_result` rows for synthetic source have non-NULL rmse/f1/precision/recall/boundary_err |
| T-004 | developer-backend | Add `v_pareto_frontier` view + `scale_runs` table to `create_tables.sql`; verify `docker exec etl4-postgres psql ... -f create_tables.sql` is idempotent | - | DDL applies cleanly; view returns rows for synthetic matrix |
| T-005 | developer-backend | New `etl4/bench/scale_study.py` CLI with `[100k,1M,10M,50M]` volumes; writes to `scale_runs`; uses `psutil` for RSS. **50M cells for `kinematic_smoothing` honour PO condition**: before launching, linearly extrapolate `peak_rss_mb` from the prior volume cell for that method; if projected > 12000, skip the cell and insert a NULL row with `notes='skipped: projected_peak_rss_mb>12000'`. | T-001 | runs end-to-end on one method for a 100k sample; inserts one row; dry-run of 50M smoothing cell against capped host correctly inserts the skip row |
| T-006 | developer-frontend | New dashboard `etl4/dashboard/08_accuracy_cost.json`: Pareto scatter from `v_pareto_frontier`, metric heatmap from `fact_experiment_result`, scale curve from `scale_runs` | T-004 | dashboard provisioned at `/d/etl4-accuracy-cost`, all three panels render with non-empty data |
| T-007 | unit-tester | Unit tests for: NoiseTruth invariants (surviving_ids correctness); `__truth_id` round-trip; metrics return non-NaN on injected synthetic | T-001, T-002 (interface stubs) | 5 new tests pass; existing suite still green |
| T-008 | functional-tester | E2E: run matrix on synthetic volume (20 flights × 1k samples × 4 noise × 3 methods); verify `fact_experiment_result` has 12 rows with 8 metrics populated; verify dashboard 08 panels render | T-003, T-004, T-005, T-006 | test_report.md with pass/fail per cell + evidence |

## Parallelizable workstreams

- **Workstream A** (backend contract): T-001 first (interface-defining), then T-002 and T-005 in parallel
- **Workstream B** (backend wiring): T-003 after T-001 + T-002; T-004 in parallel with T-003
- **Workstream C** (unit tests): T-007 starts as soon as T-001 interface is merged (can test against stubs)
- **Workstream D** (frontend): T-006 starts after T-004 (needs the view + table to exist)
- **Workstream E** (functional): T-008 serializes behind T-003 through T-006

Max concurrency: 3 after T-001 (T-002, T-005, T-007); 2 after T-001+T-002 for T-003 + T-004.

## Dashboard (preliminary)

Dashboard 08 panels:
1. Pareto scatter: X=`wall_secs`, Y=`rmse_m`, color=method, shape=noise regime (label as Clean/Light/Moderate/Heavy per the existing convention in commit 4c93973)
2. Metric heatmap: method rows × noise cols × F1 color
3. Scale curves: line chart `rows_per_sec` vs `data_volume_rows` log-log by method, from `scale_runs`
4. Summary table: top 3 methods ranked by Pareto non-dominated score

Full spec deferred to `dashboard_spec.md` once Pareto numbers exist and the DE can define specific thresholds.

## Risks

1. **`__truth_id` leakage through dedup** - `clean.dedup` works on `(icao24, time)` keys; duplicates injected by noise share `(icao24, time)` with the original. Dedup will drop one of the two; which one it drops is unstable without an explicit tiebreaker. **Mitigation**: pass `__truth_id` as a secondary sort key so the lower `__truth_id` always wins; document in dev handoff.
2. **Synthetic-only RMSE/F1** - these metrics only compute when GT exists. Real-data matrix rows (s3, rest) must remain NULL. **Mitigation**: dashboards label cells by source; `v_pareto_frontier` filters `WHERE rmse_m IS NOT NULL` so real-data runs don't appear.
3. **Tolerance parameters** - `BOUNDARY_TOLERANCE_S=60.0` already set in `metrics.py`; deviation matching window reuses it. No new knobs; may need sensitivity study if P/R look suspicious. **Mitigation**: unit test with known matched/unmatched cases; documented default.
4. **`shuffle_mb` not available for pandas engine** - `scale_runs.shuffle_mb` must be NULL for pandas runs. **Mitigation**: column is nullable; log INFO when writing NULL.
5. **psutil may not be installed** - check `etl4/requirements.txt`; if absent, add pin in the implementation PR (and record in decisions.md).
6. **This doc is DRAFT without PO** - if `requirements.md` eventually contains scope beyond this (e.g., stage-time breakouts, different metric list), the arch needs an amendment ADR.

## Non-negotiables inherited

From `CLAUDE.md`:
- §2.1 No Claude co-author trailer on commits
- §2.3 No large-data commits (scale_study samples must not persist)
- §2.6 `icao24` is pandas string dtype (preserved through `__truth_id` add)
- §2.7 Windows cp1252-safe output
- §3.2 No `df.apply()` on hot paths (NoiseTruth bookkeeping must stay vectorised)
- §3.5 Determinism; seed 42; do not change

From `plan.md`:
- §12.1 seed 42 default
- §15 test invariants in `test_pipeline.py` must stay green

## Status

`APPROVED` by data-engineer at 2026-11-14T02:13Z against `requirements.md` (PO-APPROVED with 3 conditions, all addressed above). Ready for IMPLEMENTATION phase dispatch.
