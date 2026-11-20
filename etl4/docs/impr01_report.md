# Partition-Contract Implementation for Incremental Reconstruction

**Course**: DA 231o — Data Engineering at Scale, Indian Institute of Science, Bangalore
**Team ETL4**: Nadhiya R S, Kasi Annapurna Uppalapati, Kompalli Avinash Bhargav, Sathyanarayanan C
**Project**: Scalable Kinematic Flight Trajectory Reconstruction from Noisy ADS-B Data — Accuracy–Cost Trade-offs at Scale
**Deliverable**: Engineering milestone IMPR-01 — partition contract + watermark-driven incremental refresh
**Date**: 2026-11-20

---

## 1. Abstract

The course proposal lists "scaling efficiency as data volume and cluster resources increase" among the six **Performance Metrics for Success** (section 3 of the project PDF). As of 2026-11-13 the pipeline could not credibly report that metric: every layer below the raw-bronze landing zone rebuilt in full on every run, so every added month of data paid the cost of re-processing every previous month. This report documents the engineering change that converts the pipeline from full-refresh to per-month incremental across five layers — vault, marts, Postgres sink, experiment matrix, and the evaluation view — while preserving all WI-1 scientific invariants and the bit-identical reproducibility guarantee from the project proposal. On the test bed the pipeline now rewrites only the shard corresponding to a newly added month; prior-month shards are not re-read, not re-written, and verifiably bit-identical across runs. Every layer carries a Postgres-resident watermark that advances atomically with its partition commit, so crash-recovery and selective rewind are first-class operations rather than manual artefacts.

## 2. Motivation

### 2.1. The headline research question

The course proposal frames the project around a single research question, which is reproduced here verbatim because it dictates what must be measured:

> *How much reconstruction accuracy is gained by incorporating aircraft kinematics, and what computational cost is required to obtain that improvement at large scale?*

"At large scale" is the operative phrase. The project's three reconstruction methods — time-gap, kinematic, Kalman-smoothed — must be compared not only on static accuracy numbers but on the scaling behaviour of the pipeline that produces them. The course proposal (section 3) names six Performance Metrics for Success: throughput (records/sec), end-to-end wall time, streaming latency, CPU and memory utilisation, shuffle and partitioning overhead, and scaling efficiency as data volume and cluster resources increase.

### 2.2. Why the sixth metric was unreportable

Before this milestone the pipeline topology was:

- **Vault build**: `_chunked_flightlist_vault` re-read every monthly flightlist archive on every invocation (20 – 30 min wall time, ~20 GB of transient parquet shards during the final dedup pass). During WI-1's scale-out we observed a 10 GB ENOSPC failure attributable to this transient footprint.
- **Mart build**: the two fact tables (`fact_flight`, `fact_trajectory_enriched`) rebuilt from the entire vault on every run.
- **Postgres sink**: a `TRUNCATE` + `COPY` of roughly 4M fact rows per run, with no notion of "this month changed and the others did not".
- **Experiment matrix**: `pipeline_runs` admitted duplicates on `(source, method, noise)`, so a rerun with identical inputs grew the table rather than idempotently refreshing.
- **Evaluation**: `eval_mart` was `DROP TABLE` + `CREATE TABLE`, which evicts downstream views and defeats cache.

On this topology, adding one month of bronze triggered full recomputation of every previous month. Every data-volume increase paid the same full-rebuild cost. A scaling-efficiency curve measured on this pipeline would be dominated by constant-factor rework and would not reflect the marginal cost of new data — which is precisely what the course metric asks.

### 2.3. The design the ADR committed

Architectural Decision Record `2026-11-13` (`etl4/docs/decisions.md`) committed the full redesign: partition everything time-sensitive on `ym=YYYY-MM`, carry a per-layer watermark in Postgres, and use the watermark to filter inputs so each layer rewrites only the months downstream of its last recorded position. This milestone is the execution of that ADR.

## 3. Design

### 3.1. The partition contract

Every partitioned table on disk follows the same filesystem convention:

```
data/<layer>/<table>/ym=YYYY-MM/part.parquet
```

where `<layer>` is one of `silver/vault` or `gold/marts`. One shard per month; the shard is rewritten atomically on rerun of that month via a temporary path and `os.replace`. Readers on top of the tree use `pyarrow.dataset` with a predicate `ym > last_ym` so that unmodified months are not read.

Three tables (`hub_aircraft`, `hub_airport`, `sat_aircraft_identity`) are *globally* unique rather than per-month, so they stay as a single `part.parquet` at the table root. A per-month compaction pass reads the new month's partitioned facts, extracts new primary-key rows, unions with the existing single file, deduplicates on the hash key, and rewrites the single file atomically.

### 3.2. The watermark registry

A new Postgres table holds the authoritative "last successfully materialised month" per (layer, table):

```sql
CREATE TABLE pipeline_watermarks (
    layer       TEXT        NOT NULL,
    table_name  TEXT        NOT NULL,
    last_ym     TEXT        NOT NULL,   -- regex-constrained to YYYY-MM
    row_count   BIGINT      NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (layer, table_name)
);
```

A CHECK constraint on `last_ym` forbids any string that does not match the `YYYY-MM` calendar regex, which rejects sentinel values at the storage boundary. The write-side API `watermark.set(layer, table, last_ym, row_count, conn)` takes an open connection so the caller can commit the watermark advance in the same transaction as the sibling Postgres write it is advertising; vault and mart layers commit the watermark immediately after their filesystem write succeeds.

### 3.3. Per-layer incremental mechanism

| Layer | Pre-milestone | Post-milestone |
|---|---|---|
| **Vault** | Full re-read of all bronze flightlists | Reads `last_ym` from watermark; filters bronze files by file-month; writes one shard per new month; `_DONE` sentinel resumes crashed runs mid-month |
| **Marts** | Full rebuild from vault | `pyarrow.dataset` with `ym > last_ym` filter; writes new-month shards only; dims rebuilt in full (cheap, single-file) |
| **Sink** | `TRUNCATE` + `COPY` | `INSERT ... ON CONFLICT (hk_*) DO UPDATE` per table, per month; same-transaction watermark advance; `COALESCE` preserves non-NULL columns |
| **Matrix** | Duplicate rows per rerun | `UNIQUE (ym, source, method, noise)` constraint; `ON CONFLICT DO NOTHING` on insert |
| **Eval** | `DROP TABLE` + `CREATE TABLE` | Plain SQL view over `pipeline_runs LEFT JOIN eval_results`; `v_pareto_frontier` recreated against the view |

### 3.4. Rewind and crash-safety

Selective rewind is implemented as a CLI with explicit chaining order:

```
python -m etl4.orchestration.rewind --layer {vault|marts|sink|matrix|all} \
       --from YYYY-MM --to YYYY-MM [--yes]
```

`--layer all` chains the four layers in the **downstream-first** order (eval -> matrix -> sink -> marts -> vault) so every downstream reader is already drained when its source is rewound. The `--yes` guard is required for destructive runs; `--dry-run` bypasses the guard and prints the plan.

Crash-safety is enforced by three independent mechanisms. First, every partition write is atomic: parquet is written to a temporary filename, fsync'd, and then renamed into place with `os.replace`. Second, a `_DONE` sentinel file is written after every per-month commit; the next run skips months whose sentinel exists. Third, the watermark advance is a same-transaction Postgres write for layers that terminate in Postgres (sink, matrix), and a post-commit write for layers that terminate on disk (vault, marts). A kill between write and advance leaves the month either fully written (sentinel present) or fully absent (sentinel missing) — never half-written.

### 3.5. Reader fallback

The partition contract is adopted layer-by-layer in the field, so every reader falls back to the monolithic `part.parquet` at the table root when no `ym=*` children exist. This keeps pre-migration trees working while the operator runs the migration script offline.

## 4. Implementation

The work was planned as 27 engineering tasks organised into five code-review bundles plus a migration bundle and a QA bundle. All 24 landed task tickets closed on branch `feature/pipeline-3-avinash`.

### 4.1. Bundle 1 — schema and watermarks

- `etl4/infra/create_tables.sql` added `pipeline_watermarks` with the CHECK constraint, idempotent `CREATE TABLE IF NOT EXISTS` plus `ALTER TABLE ... ADD COLUMN IF NOT EXISTS ym` on `fact_trajectory_enriched` and `pipeline_runs`, backfill from existing timestamps, and the `UNIQUE (ym, source, method, noise)` constraint after a one-shot collapse of pre-existing duplicate matrix rows.
- `etl4/vault/watermark.py` added the write-side API `set(layer, table, last_ym, row_count, conn)` accepting an open connection, and `get(layer, table)` returning `None` for an unknown key.

### 4.2. Bundle 2 — vault I/O helpers

- `etl4/vault/vault_io.py` centralises nine routines: `read_table` (with monolithic fallback), `write_partition` (atomic tmp+rename), `row_count` (pyarrow footer-only), `list_ym`, `ym_from_filename`, `ym_from_date`, `ym_from_epoch`, `extract_ym_from_frame`, and `extract_ym_from_path`. The three `ym_from_*` helpers unify the two derivation paths (in-file `day` column for flightlist bronze; virtual `date=YYYY-MM-DD` for state bronze) under one API.
- Reader fallback is implemented in `read_table`: a probe for `ym=*` children routes to the dataset reader; absence of children routes to a direct `pd.read_parquet` on the monolithic file.

### 4.3. Bundle 3 — raw vault

- `etl4/vault/raw_vault.py` was restructured around a per-month loop. The flightlist builder walks bronze files, groups them by their extracted file-month, and writes each group's output into its own `ym=YYYY-MM/` shard. The `_DONE` sentinel is written only after the shard commit succeeds.
- `etl4/vault/raw_vault_spark.py` (the `sat_state_observation` builder) adds the ym column via `ym_from_epoch`, repartitions by ym, and writes partitioned. `icao24` stays pandas `string` dtype on the Python read path (course constraint, CLAUDE.md §2.6).
- `etl4/vault/compact.py` is the new small-hub compaction module. Its `DEDUP_KEYS` mirror the original `_FINAL_DEDUP` keys from `raw_vault.py`. The raw-vault caller is a one-line wrapper that delegates.

### 4.4. Bundle 4 — marts

- `etl4/vault/marts.py` reads with `pyarrow.dataset` under a `ym > last_ym` filter and writes `fact_flight/ym=*/part.parquet` + `fact_trajectory_enriched/ym=*/part.parquet`. A new `--only-ym YYYY-MM` CLI flag materialises exactly one month for targeted reruns driven by rewind.
- A new `_latest_sfs(sfs)` helper enforces the cross-month flight rule: within the current per-month read window, rows that share an `hk_flight` are reduced to the single row with the latest `lastseen`, breaking ties on `load_dt`, under a stable sort. ADR `2026-11-20` documents the operational mitigation for cross-month flights that span two per-ym reads (sink UPSERT plus neighbour rewind M±1).

### 4.5. Bundle 5 — sink, rewind, matrix, eval

- `etl4/sink/postgres.py` and `etl4/sink/postgres_sink.py` replace `TRUNCATE` + `COPY` with staged `COPY` into a `TEMP` table followed by `INSERT ... ON CONFLICT (hk_*) DO UPDATE ... SET col = COALESCE(EXCLUDED.col, target.col)`. `COPY` cannot do `ON CONFLICT` directly, so the staged pattern is required. The watermark advance is written on the same connection as the UPSERT and commits atomically.
- `etl4/bench/run_matrix.py` accepts a `--ym YYYY-MM` flag; inserts use `ON CONFLICT (ym, source, method, noise) DO NOTHING`; re-measurement of a cell is only possible after `rewind --layer matrix`.
- `etl4/run_e2e.py::stage_eval_mart` is now idempotent in a single psycopg transaction. The CTE named `latest` carries `pipeline_runs.ym` through to the outer SELECT; the DDL grows to 19 columns (the original 18 plus `last_ym TEXT`); the DROP block covers both `v_eval_mart_ym` and `v_pareto_frontier` so recreation against the new view does not leave stale dependents.
- `etl4/orchestration/rewind.py` is the new CLI, with downstream-first chaining in `--layer all`, idempotent re-rewind (second run returns zero rows affected), and an in-script cross-month overlap probe that reads next-month `sat_flight_summary` via `vault_io.read_table` with `columns=['hk_flight']` and prints a WARNING line citing ADR 2026-11-20 when `.isin()` overlap is non-zero.

### 4.6. Migration

- `etl4/scripts/migrate_partition_ym.py` is the one-shot zero-reprocess migration. It reads each pre-existing monolithic `part.parquet` with `ParquetFile.iter_batches(batch_size=1_000_000)`, groups by derived `ym` via `pd.factorize`, writes atomically to tmp + `os.replace`, and asserts row-count equality before and after. Exit policy: dry-run with schema mismatch warns and exits 0; real-run fails fast and skips the watermark advance.
- Three flightlist-derived tables (`link_flight_aircraft`, `link_flight_route`, `sat_flight_summary`) are rebuilt from bronze under the T-08 code path rather than stream-migrated, because their monolithic `part.parquet` lacks the file-month attribution that the partition contract now requires. This correction was made after a primary-source read of `raw_vault.py:543` confirmed that the authoritative key is the bronze **file-month** (`ym_from_filename(f.name)`), not any in-file timestamp.

### 4.7. Drift QA

- `etl4/orchestration/pipeline_volumes.py` now routes silver/gold row counts through `vault_io.row_count` (footer-only, no body read). At the top of `run_e2e` a drift check compares partition-directory count per table against the watermark's `last_ym`; mismatch exits non-zero with a message pointing at the appropriate rewind invocation.

## 5. Evidence

### 5.1. Test suite

`pytest etl4/tests/ -v` reports **92 passed in 8.25 s** on `feature/pipeline-3-avinash @ 4c67013`. The pre-milestone baseline was 34 passed; the delta of +58 is new coverage targeted at the partition contract:

- `test_vault_io.py` — 43 tests covering the nine public helpers, reader fallback, column pushdown, Spark-shard reader extension, write-partition `ym` validation, and SHA-256 determinism across two fresh writes.
- `test_watermark.py` — 15 tests covering `get` / `set` semantics, CheckViolation on malformed `last_ym`, transaction sharing, idempotent repeat, and rollback atomicity.

No pre-existing test was removed, renamed, or skipped. CLAUDE.md §3.8 (one-invariant-per-test convention) is preserved.

### 5.2. Row-count preservation

Thirteen tables measured through the drift-check instrumentation report **333,366,530 rows total identical** before and after the route switch to `vault_io.row_count`. No table deviates on either side of the migration script's dry-run comparison.

### 5.3. Idempotency and bit-equality

Two back-to-back `run_e2e` invocations on the same bronze produce:

- unchanged partition `mtime` on every vault / mart / sink shard,
- unchanged `pipeline_watermarks.last_ym` / `row_count` / `updated_at`,
- SHA-256-identical shard contents across the two runs (determinism contract, seed 42, CLAUDE.md §3.5).

### 5.4. Rewind end-to-end

A 7-scenario functional test was executed against an isolated Postgres schema `t27_sandbox` (not production). Scenarios covered: dry-run on empty state; single-layer vault rewind; full-stack `--layer all` chain with call-order verification; cross-month flight rewind M then M+1; `--help` text content; idempotent repeat rewind; and global dim invariance (`dim_aircraft` must never roll back from a partition-layer rewind). All 7 scenarios passed. Production state assertions verified at test exit: `public.dim_aircraft` row count 149,747 pre and post; two production `pipeline_watermarks` rows unchanged; sandbox schema dropped `CASCADE` in `finally`.

### 5.5. WI-1 scientific invariants held

The two WI-1 acceptance criteria that are structurally sensitive to the schema change were re-verified:

- **AC-10** (`fact_experiment_result` zero NULLs across the six PDF metric columns on the 12 synthetic matrix cells): held. The new 19-column view projects the same 18 base columns plus `last_ym`; the metric columns are unchanged.
- **AC-11** (`SELECT COUNT(*) FROM v_pareto_frontier` equals the synthetic matrix cell count, baseline 12): held. The DROP block in `stage_eval_mart` covers `v_pareto_frontier` so the view is recreated against the new base view with identical projection and row count.

Neither invariant was regressed by any change in this milestone.

## 6. Correctness and reproducibility

### 6.1. Architectural decisions recorded

Three ADRs were appended to `etl4/docs/decisions.md` during this milestone:

1. **2026-11-20 Cross-month flight reader rule in `marts._latest_sfs`** — defines the stable-sort reduction `(hk_flight ASC, lastseen DESC, load_dt DESC)` and documents the ADR's R1 closure path (operational mitigation via sink UPSERT and neighbour rewind, since per-ym read means two adjacent months never share a frame in the mart reader).
2. **2026-11-20 `fact_experiment_result` schema expansion 18 -> 19 columns** — records the addition of `last_ym TEXT` carried through the CTE `latest` from `pipeline_runs.ym`, preserving the downstream `rmse_m` mapping from ADR 2026-11-18.
3. **2026-11-20 IMPR-01 T-13 file-month as the single `ym` key for flightlist-derived vault tables** — binds the migration path to `raw_vault.py:543`'s `frame["ym"] = ym_from_filename(f.name)`, rejecting the alternative of deriving `ym` from `firstseen` which would have created a permanent split-brain on 12,685 rows at the Jan/Feb boundary.

### 6.2. Codebase invariants preserved

Every CLAUDE.md non-negotiable was audited on the final commit:

- §2.1 — no Claude attribution on any commit or PR body.
- §2.2 — no secrets in the staged diff; `.env` remains gitignored.
- §2.3 — no tracked data; `data/raw/`, `data/bronze/`, `data/silver/`, `data/gold/`, `data/eval/`, and `data/reference/synthetic_truth/` remain gitignored.
- §2.6 — `icao24` stays pandas `string` on every read path (vault_io tests assert `.dtype.name == 'string'`).
- §2.7 — Windows cp1252-safe stdout; no `->`, em-dash, en-dash, or arrow glyph in any new file (byte scans run in each task's smoke).

### 6.3. Reproducibility

The determinism contract (CLAUDE.md §3.5) is held end-to-end: a second run on the same bronze with seed 42 produces byte-identical shards. The migration script is idempotent on rerun (row-count equality enforced by `AssertionError` before any watermark advance). The rewind CLI is idempotent on repeat invocation (second call affects zero rows, exits 0).

## 7. Limitations and follow-ups

Four items are carried forward as explicit backlog entries. None of them block the acceptance of this milestone; all are documented in `multi-agent-engineering/improvements_backlog.md`.

1. **IMPR-02 — Spark promotion for the flightlist vault.** The non-partitioned Spark port of `hub_flight` / `link_*` / `sat_flight_summary` is deliberately out of scope for this milestone and queued as the next engineering project.
2. **IMPR-15 — Drift check should walk `sat_state_observation`.** The 333M-row table is already partitioned pre-migration (produced by the Spark builder), so it was excluded from the migration script's table list. The drift check at the top of `run_e2e` should be extended to cover it; the fix is a ~30-line patch.
3. **Operator migration execution.** The migration script is tested, idempotent, and dry-runnable, but the real-run against the ~279 GB silver tree is a manual operator step, not an automated one. The revised runbook is in the T-13 migration review document.
4. **Cross-month flight scope.** `marts._latest_sfs` reduces within the per-month read window; a flight that is physically split across two adjacent months is closed via the sink UPSERT when its second half is written, and/or a subsequent `rewind --from M --to M+1`. This operational mitigation is explicitly documented in ADR 2026-11-20 and surfaced in the rewind CLI's `--help` epilog.

## 8. Discussion

The partition contract is not a performance tuning pass. It is an architectural change that converts the pipeline from monolithic to incremental, and the course-relevant consequence is a shift in **what "scaling" means** for the project's measurements.

Pre-milestone, adding one month of bronze paid the full-rebuild cost. The scaling-efficiency curve would have shown a linear-or-worse growth of wall time in cumulative data volume, dominated by the constant work of re-reading every previous month. That curve answers no interesting question about kinematic reconstruction cost.

Post-milestone, adding one month of bronze pays the cost of the new month only. The scaling-efficiency curve is now a function of **marginal** wall time versus marginal data volume, which is the number the PDF metric asks for. The three reconstruction methods — time-gap, kinematic, and Kalman-smoothed — can now be compared on their actual incremental cost per month, independent of pipeline infrastructure churn.

The milestone does not itself report the scaling-efficiency numbers for the three methods. That is the remit of the subsequent scale study. What this milestone provides is the pipeline topology on which those numbers become reportable.

## 9. Conclusion

The milestone delivers the architectural change required by the course's sixth Performance Metric. Five pipeline layers moved from full-refresh to per-month incremental; every partitioned write is atomic and crash-safe; every layer carries a Postgres-resident watermark that advances on commit; selective rewind is a first-class CLI operation; the migration from pre-partition state to partitioned state is a one-shot script that preserves row counts and is idempotent on rerun. The WI-1 scientific invariants (AC-10 and AC-11) are preserved. The test suite grew from 34 to 92 passes with zero regressions. The branch `feature/pipeline-3-avinash` is ready for integration into `main`.

## 10. References

- Course proposal: `ETL4 (3) (1).pdf`, section 1 (research question) and section 3 (Performance Metrics for Success).
- Project specification: `plan.md` at the repository root (authoritative).
- Architectural decisions: `etl4/docs/decisions.md`, entries dated `2026-11-13`, `2026-11-18`, `2026-11-20` (three on this date).
- Project tracker: `multi-agent-engineering/projects/PROJ-20261118-impr01-partition-contract/`, including the per-task acceptance criteria (`tasks/T-*.json`) and the technical reviews (`t11_vault_review.md`, `t13_migration_review.md`, `t16_marts_review.md`, `t19_sql_review.md`, `t24_pr5_review.md`, `t27_rewind_e2e_report.md`).
- Backlog: `multi-agent-engineering/improvements_backlog.md`.

---

*End of report.*
