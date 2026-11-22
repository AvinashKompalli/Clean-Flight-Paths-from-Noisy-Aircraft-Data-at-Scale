# ETL4 improvements backlog

Ranked, scoped follow-ups for after PROJ-20261114-wi1-metrics closes. Each
item has an explicit PDF/plan.md trace, a scope boundary, an effort estimate,
and a prerequisite list.

Rank order reflects impact against the PDF's §3 Performance Metrics for
Success and §2 High-Level Design, weighted by effort.

---

## Evaluation verdict (orchestrator, 2026-11-18)

User directive: "look into and implement the 3 suggestions if needed."

| Item | Verdict | Rationale | Queued dispatch |
|---|---|---|---|
| **IMPR-01** partition contract | **IMPLEMENT** | PDF §3 "scaling efficiency" cannot be credibly reported without this. Removes the proximate cause of the 10 GB ENOSPC and the 20-30 min full-vault rebuild on any new month. Design is already committed as ADR 2026-11-13 - pure execution ahead. | `/workflow IMPR-01` fires the moment PROJ-20261114-wi1-metrics reaches `phase: DONE`. |
| **IMPR-02** Spark flightlist vault | **IMPLEMENT** | PDF §2 literally names Spark at the top of the architecture diagram; only `sat_state_observation` currently uses it. Without this, the scaling curve on dashboard 08 is pandas-sized and the "we scale by adding executors" story is unsupported. | `/workflow IMPR-02` fires after IMPR-01 (shares vault files; sequencing avoids merge churn). |
| **IMPR-03** Iceberg/Delta migration | **DEFER (not needed)** | Not in the PDF scope. Buys lakehouse maturity (ACID, time travel, hidden partitioning, schema evolution) but delivers no PDF-grading evidence. Course scope-creep risk outweighs the engineering lift. Keep on this list as an aspirational extension if the team later wants to argue "beyond the brief"; do NOT auto-dispatch. | Do not auto-dispatch. Only on explicit user signoff. |

Both IMPR-01 and IMPR-02 have a hard prerequisite on WI-1 acceptance (the
backlog items say so, and dispatching them earlier would clash with the
in-flight T-008 e2e + data-engineer review). The orchestrator will wire the
auto-chain from WI-1 final acceptance -> IMPR-01 kickoff on the final-accept
hand-back; and IMPR-01 acceptance -> IMPR-02 kickoff on IMPR-01's.

---

---

## IMPR-01 - Implement the ym=YYYY-MM partition contract (watermark-driven incremental refresh)

**Rank: 1 (highest leverage)**

**Design state**: committed as ADR `2026-11-13 - Partition contract (ym=YYYY-MM) and watermark-driven incremental refresh` in `etl4/docs/decisions.md`. Zero implementation.

**PDF/plan.md trace**:
- PDF §3 Performance Metrics for Success: "throughput", "end-to-end processing time", "scaling efficiency as data volume ... increase" - cannot be credibly reported when every new month re-runs the prior 47
- plan.md §17 (schema change risk), §15 (determinism under rerun)

**Current pain this removes**:
- Vault: 20-30 min full rebuild on any new month input - already observed as the 10 GB ENOSPC crash
- Marts: full rebuild from the entire vault every run
- Sink: `TRUNCATE` + `COPY` of 4.37M `fact_trajectory_enriched` rows every run - WAL pressure proportional
- `pipeline_runs`: duplicate rows per `(method, noise)` on every rerun, biasing dashboard aggregates
- One added month triggers a full 10 GB recomputation; the headline "at scale" claim is weak until this is fixed

**Scope**:
1. Watermark DDL (from ADR): `pipeline_watermarks(layer, table_name, last_ym, row_count, updated_at)`
2. Partition vault: `_chunked_flightlist_vault` filters bronze by `ym > last_ym`, writes `hub_flight/ym=YYYY-MM/part.parquet`, etc. Non-partitioned small dims (hub_aircraft, hub_airport, sat_aircraft_identity) get a per-month compaction pass
3. Partition marts: `vault/marts.py` reads with `pyarrow.dataset` + partition filter, writes per-month
4. Sink UPSERT: `postgres_sink.py` replaces `TRUNCATE` + `COPY` with `INSERT ... ON CONFLICT (hk_*) DO UPDATE` per target, per month
5. Matrix dedup: add `ym TEXT` to `pipeline_runs`, add unique constraint `(ym, source, method, noise)`, `ON CONFLICT DO NOTHING`
6. `eval_mart`: convert to materialized view over the deduped `pipeline_runs`
7. Rewind CLI: `python -m etl4.orchestration.rewind --layer vault --from 2022-03 --to 2022-05`
8. Zero-reprocess migration: one-shot pyarrow pass per existing monolithic `part.parquet` split by extracted `ym`

**Prerequisites**:
- WI-1 (PROJ-20261114-wi1-metrics) complete so the matrix + dashboard baseline is frozen before the schema moves
- Branch off current pipeline-3 or a new pipeline-4 branch

**Effort**: 3-4 days (split into 5 PRs - watermark DDL, vault partition, marts partition, sink UPSERT, rewind CLI)

**Risks** (from ADR):
1. Cross-month flights (same `hk_flight` in two ym partitions) - reader-side merge rule, already specified
2. Small-hub uniqueness drift - compaction pass enforces
3. `ym` extraction asymmetry: flightlist bronze has in-file `day`; state bronze has it in the partition path - one helper symmetrises
4. Watermark vs filesystem drift - startup QA check in `run_e2e` refuses to start if they disagree

**Why rank 1**: Biggest single engineering uplift the PDF's claims rely on. Design is already done; this is pure execution. Removes the proximate cause of ~90% of the current operational pain.

---

## IMPR-02 - Finish Spark promotion for the flightlist vault

**Rank: 2 (high leverage, PDF-aligned)**

**PDF trace**:
- PDF §2 High-Level Design puts `Kafka -> Spark Structured Streaming` at the top of the architecture diagram
- PDF §2 Big Data Platforms: "Apache Spark Structured Streaming: distributed processing of streaming and historical trajectory data" + "Spark DataFrames: cleaning, transformation, deduplication, and feature engineering"
- Currently only `sat_state_observation` uses Spark (via `raw_vault_spark.build_sat_state_observation_spark`). The other 7 vault tables (hub_flight 93M, link_flight_aircraft 95M, link_flight_route, sat_flight_summary, sat_aircraft_identity, hub_aircraft, hub_airport) are pandas + DuckDB single-process

**Current pain this removes**:
- Single-process memory ceiling - the entire reason the WI-1 scale study caps at bronze tree size instead of showing a scaling curve
- Hand-rolled shard dirs + DuckDB streaming are a band-aid for the lack of shuffle-managed spill
- No story for "we scale by adding executors" - the docker-compose already runs 3 workers, but they sit idle

**Scope**:
1. New `etl4/vault/raw_vault_spark.py::build_flightlist_vault_spark(flightlist_dir, out_dir, load_dt)` mirroring the current pandas pipeline shape
2. Read the flightlist tree as a Spark DataFrame (`spark.read.parquet(flightlist_dir)`)
3. Each builder (hub_aircraft, hub_airport, hub_flight, link_flight_aircraft, link_flight_route, sat_aircraft_identity, sat_flight_summary) ports to either native Spark DF transforms (preferred, pure SQL / Column API) OR `applyInPandas` grouped on `icao24` (fallback for row-level logic)
4. Final dedup becomes `dropDuplicates(subset=keys)` - shuffle-spill is Spark-managed
5. `coalesce(1).write.parquet(...)` for the single-file output contract or keep the partitioned write
6. Add `engine="spark"` branch to `build_raw_vault` so it routes all vault builders through Spark when selected, matching the existing `sat_state_observation` pattern
7. Scale study (`etl4/bench/scale_study.py` from T-005) gains a `--engine spark` option so the curves show pandas vs Spark side-by-side

**Prerequisites**:
- WI-1 complete (scale study reports a flat pandas curve to contrast against)
- Working Spark cluster (already running in docker-compose)
- IMPR-01 landing first is helpful but not required - Spark version can write monolithic parquet for now and partition later

**Effort**: 2-3 days

**Risks**:
- Shuffle size on 93M-row joins may exceed worker memory - needs `spark.sql.shuffle.partitions` tuning
- `applyInPandas` carries icao24 group as a string - verify the dtype contract holds end-to-end
- Driver-side `.toPandas()` would re-create the current memory bomb; reads must stay in Spark

**Why rank 2**: The PDF literally names Spark as the processing engine. Without this, the "scale" story is pandas-sized. Directly unblocks the scale-curve narrative in the final dashboard.

---

## IMPR-03 - Migrate silver/gold parquet lake to Iceberg or Delta Lake

**Rank: 3 (aspirational; large infra lift)**

**PDF trace**:
- NOT named in the PDF. The brief talks about bronze/silver/gold in prose but does not demand lakehouse table formats
- This is a "DE at scale maturity" extension, not a PDF-mandated deliverable

**What it buys**:
- Table-level ACID (atomic multi-file writes, atomic schema change)
- Hidden partitioning - writers do not manage `ym=YYYY-MM/` dirs; metadata does. Supersedes IMPR-01's filesystem-level partition convention
- Time travel - reproducible rerun against any historical snapshot (powerful for "replay the matrix against the Q3 vault" scenarios)
- Schema evolution - `ADD COLUMN` without rewriting data files
- Native integration with Spark 3.5 (both Iceberg and Delta have first-class Spark catalogs)

**Current pain this removes**:
- Shard-dir cleanup discipline becomes unnecessary - no half-written partition dirs
- The ADR's "watermark vs filesystem drift" risk (IMPR-01 risk #4) disappears - the metadata is the watermark
- Dashboard "which snapshot does this chart come from" becomes queryable, not an ask of the reader

**Scope**:
1. Choose: Iceberg vs Delta (Iceberg has better pyarrow/Python story outside Spark; Delta is simpler on Spark-only)
2. Add catalog config to `etl4/infra/docker-compose.yml` (Hive-metastore or local filesystem catalog)
3. Rewrite vault writers to emit tables via the chosen format (per-table `CREATE TABLE ... USING iceberg` first time; subsequent `INSERT INTO` / `MERGE INTO`)
4. Marts: same migration
5. Readers: swap `pd.read_parquet(...)` for `spark.read.table(...)` where possible; keep a pandas fallback via `pyiceberg` (Iceberg) or deltalake (Delta)
6. Write an ADR documenting the choice

**Prerequisites**:
- IMPR-02 landing first - Iceberg/Delta pay off most on Spark; running them from pandas alone is expensive and fragile
- Clear agreement from the user that this is in scope - the PDF does not ask for it
- Course reviewer context: if the reviewer grades on "stretch beyond the brief", this helps; if they grade strictly to brief, it is noise

**Effort**: 4-6 days (new infra + two full rewrite passes + docs + tests)

**Risks**:
- Operational overhead of a catalog service on the course laptop
- Debuggability drops - table format is less "cat the file" friendly than parquet
- Course scope creep - if the brief evaluator does not expect lakehouse tables, the effort does not show up on the grade

**Why rank 3**: Large infra lift with low PDF trace. Worth doing only after IMPR-01 and IMPR-02 land and if the team wants to argue "beyond the brief". Mark as aspirational.

---

## How to dispatch any of these

```
/workflow IMPR-01: implement partition contract ADR for ym=YYYY-MM incremental refresh
/workflow IMPR-02: finish Spark promotion for the flightlist vault
/workflow IMPR-03: evaluate Iceberg vs Delta Lake migration for silver/gold
```

The orchestrator creates a fresh project under `multi-agent-engineering/projects/`, routes through product-owner (requirements) + data-engineer (architecture) + developers + testers in the usual DAG.

---

## Follow-ups raised by data-engineer CORE_SOLUTION_REVIEW (2026-11-18)

Six medium/low-severity items surfaced during the independent technical
review of PROJ-20261114-wi1-metrics. None blocked acceptance (DE recommended
APPROVE). Tracked here so WI-2 and the IMPR-01/02 waves can fold them in
cleanly.

### IMPR-04 - persist deviation_prf1 to Postgres
**Severity: medium.** `etl4/evaluate/metrics.py` emits `dev_precision`,
`dev_recall`, `dev_f1` into the `rec` dict, but
`etl4/sink/postgres.py::_PERSISTED_METRICS` does not list them, so they are
dropped before Postgres. Three-line fix. Natural landing spot: WI-2 Phase A
(deviation detection pipeline) where the metrics are the headline. Effort: 15 min.

### IMPR-05 - jump-aware init for Kalman smoothing at N2/N3
**Severity: medium.** `kinematic_smoothing` under N2/N3 noise produces
rmse_m in the 1.3M-1.95M range because CHI2_GATE rejects 2-degree position
jumps as 4-sigma outliers on the clean-fixture line. Pareto narrative holds
("Kalman degrades under heavy ADS-B noise without jump-aware init") but the
numeric result is embarrassing on the dashboard. Fix: skip gate during
first K samples after a flagged jump, or seed Kalman with a robust init.
Existing related ADR: 2026-11-09. Effort: 1-2 days.

### IMPR-06 - clamp coverage_ratio at 1.0 (or investigate correct_trajectory duplication)
**Severity: medium.** `coverage_ratio > 1.0` on `kinematic_smoothing` under
N2/N3 (1.451, 1.867) because Kalman reconstruction appears to produce
duplicate rows mid-flight. Either tighten the clamp in `metrics.py` or
dedupe inside `reconstruct/kalman.py`. The dashboard panel renders correctly
but the number reads as a bug. Effort: half a day.

### IMPR-07 - vectorise `_mask_from_events`
**Severity: low.** `etl4/evaluate/metrics.py::_mask_from_events` is an
O(events*rows) Python loop. Fine at the WI-1 synthetic fixture scale; will
hurt under N3 at real bronze volumes once IMPR-01 lets the matrix run on
per-month partitions. Vectorise with pandas or numpy when IMPR-01 lands.
Effort: 2-3 hours.

### IMPR-08 - remove double-noising from synthetic main()
**Severity: medium (pre-existing).** `etl4/ingest/synthetic.py::main()`
calls `add_noise()` before bronze write, which then becomes input to
`inject_noise()` in the matrix path. The matrix therefore applies noise
twice. Pareto ordering survives this (relative method ranking preserved)
so WI-1 acceptance is not blocked, but downstream absolute numbers are
biased. Fix: drop `add_noise()` from `main()`; `inject_noise()` is the
single source of controlled noise. Effort: 30 min.

### IMPR-09 - stable runs.csv header
**Severity: low.** `data/eval/runs.csv` widened from 14 to 18 columns when
T-003 added the four new metric columns. Pre-existing runs.csv files cause
a pandas `ParserError` on the next matrix run ("Expected 14 fields in
line 34, saw 18"). File is gitignored, dev-local, and only affects a
developer's second local run. Fix: version the header, or wipe-and-regen
on schema mismatch. Effort: 15 min.

### IMPR-13 - clean up stale `synthetic.add_noise` comments
**Severity: low (janitorial).** After IMPR-08 removed `synthetic.add_noise`,
two stale comment references remain in files that were under IMPR-08's
"Do NOT touch" scope: `etl4/bench/noise.py:48` and
`etl4/bench/run_experiment.py:232`. Both are comments explaining historical
behaviour that no longer exists. Replace the "synthetic.add_noise" references
with "pre-noise synthetic bronze (IMPR-08)" or similar. 5-minute cleanup;
bundle with another developer-backend commit. Owner: developer-backend.

### IMPR-14 - per-cell smoothing divergence: N2 kinematic_smoothing rmse=1.45e6
**Severity: medium.** After the IMPR-08 bronze cleanup, `kinematic_smoothing`
at N2 shows `rmse_m = 1,445,595.8` while its N1 and N3 twins return `0.0`.
This is a per-cell smoothing instability that existed before but was masked
by the pre-IMPR-08 noise baseline. Likely related to IMPR-05 (Kalman CHI2
gating at mid-noise levels), but the per-cell pattern (N2 broken, N3 clean)
suggests a seed-sensitive numerical edge in the matrix routing rather than
a pure gating issue. Diagnosis + fix: 2-4 hours. Owner: data-engineer.
File: `etl4/reconstruct/kalman.py`. Could roll into IMPR-05 or stand alone.

### IMPR-12 - file flightlist->hub_flight 20.75% reconcile drift as standalone data-engineer ticket
**Severity: medium (pre-existing, flagged as DEF-003 in T-008).** Reconcile
check reports 20.748% drop between `bronze_flightlists` row-sum and
`vault.hub_flight` row count, above the 2% tolerance. Pre-existing (predates
WI-1); surfaced by the reconcile gate. Must be filed and triaged before
IMPR-01 kickoff so the partition contract does not inherit an undiagnosed
drop as noise in its own reconcile numbers. Owner: data-engineer. Likely
root cause: `hub_flight` dedups on `hk_flight` which compresses multi-row
bronze flights into one hub row; the 20% "drop" is actually correct dedupe
behaviour, not a data loss. If confirmed, the reconcile check itself needs
a per-table expectation shape (flightlists vs hub_flight is NOT 1:1), which
is the real fix. Effort: 2 hours of diagnosis + 1 hour of reconcile.py
tolerance-profile update.

### IMPR-11 - `run_matrix.py` auto-derive `--truth` from bronze for synthetic source
**Severity: medium (dev-UX, surfaces as CRITICAL if forgotten).** DEF-001 required the developer to pass `--truth etl4/data/reference/synthetic_truth/2026-01-01.parquet` explicitly to the matrix CLI for N0 cells to score (inject_noise is skipped at N0 so no sidecar is produced). Without that flag, the metric block falls through and all 6 PDF-mandated metrics write NULL. `run_experiment.py:223` already has bronze-derived truth fallback logic - mirror the same into `run_matrix.py` so the CLI default is "just works". Effort: 1 hour. Owner: data-engineer or developer-backend. File: `etl4/bench/run_matrix.py`.

### IMPR-15 - run_e2e drift check should cover sat_state_observation
**Severity: low (pre-flight blind spot).** T-24 PR5 review F2: the T-22
`VAULT_PARTITIONED_TABLES` drift-check list omits `sat_state_observation`
per the original dispatch spec. The 333M-row table is the single largest
partitioned asset in the pipeline; any watermark-vs-filesystem drift on
it is invisible until the next vault stage runs. `qa/reconcile.py` still
catches it post-build, but surfacing it pre-flight matches the contract
for all other partitioned vault tables. Effort: ~5 min to extend the list
+ rerun smoke. Owner: data-engineer. File:
`etl4/run_e2e.py` (`VAULT_PARTITIONED_TABLES` constant near the drift
check).

### IMPR-10 - ADR for the eval_mart schema expansion
**Severity: low (process hygiene).** 2026-11-18 orchestrator fix to
`stage_eval_mart` grew `fact_experiment_result` from 14 columns to 18
(added `seg_precision`, `seg_recall`, `coverage_ratio`, `boundary_err_s`),
remapped the metric key from `rmse_m` to `position_rmse_m`, dropped the
`source='s3'` filter, and introduced `DROP VIEW ... BEFORE DROP TABLE`
because `v_pareto_frontier` depends on `fact_experiment_result`. These are
real, load-bearing decisions that should be documented in
`etl4/docs/decisions.md` for the audit trail. Effort: 20 min.

---

## Not on this list (and why)

- Anomaly-detection streaming promotion (WI-2 Phase B from the earlier discussion): already named, scoped and user-confirmed; lives separately as its own project kickoff after WI-1, not an improvement.
- Deviation-detection dashboard (WI-2 Phase A): same - that is the next in-flight project, not an improvement. IMPR-04 naturally merges into Phase A.
- Fixing the `test_matrix_csv_written_after_every_cell` pre-existing failure: ALREADY FIXED as part of the WI-1 wave-3 commit.
