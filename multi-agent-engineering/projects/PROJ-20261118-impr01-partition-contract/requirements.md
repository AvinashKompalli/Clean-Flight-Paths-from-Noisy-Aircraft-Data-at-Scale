# Requirements - PROJ-20261118-impr01-partition-contract

**Authored by**: product-owner
**Date**: 2026-11-18T06:10:00Z
**Source request**: User directive 2026-11-18 ("look into and implement the 3 suggestions if needed"). Orchestrator evaluation marked IMPR-01 IMPLEMENT (PDF section 3 "scaling efficiency" cannot be credibly reported without this; design is already committed as ADR `2026-11-13 - Partition contract (ym=YYYY-MM) and watermark-driven incremental refresh`). This project is pure execution of that ADR. Full rationale: `multi-agent-engineering/improvements_backlog.md` IMPR-01 entry.

## Business context

This is the DA 231o Data Engineering at Scale capstone (Team ETL4, IISc Bangalore). The authoritative headline question is quoted verbatim from `ETL4 (3) (1).pdf` section 1:

> *"How much reconstruction accuracy can be gained by incorporating aircraft kinematics, and what computational cost is required to obtain that improvement at large scale?"*

PDF section 3 names the **Performance Metrics for Success** the project is graded against:

> "Throughput (records processed per second). End-to-end processing time and streaming latency. CPU and memory utilization. Shuffle and partitioning overhead. **Scaling efficiency as data volume and cluster resources increase.**"

Current reality: every layer downstream of bronze is full-refresh. The vault re-reads all monthly flightlists on every build (20-30 min wall time, roughly 20 GB of transient shards during the final dedup, and the proximate cause of the 10 GB ENOSPC failure observed during WI-1's scale-out). Marts rebuild from the entire vault. Postgres sink is `TRUNCATE` + `COPY` of ~4M fact rows per run. `eval_mart` is `DROP TABLE` + `CREATE TABLE`. One added month of bronze triggers full recomputation of the other 47. On this topology, "scaling efficiency" is unreportable because every data-volume increase pays a full rebuild cost regardless of what actually changed.

ADR `2026-11-13` committed the full design: a partition contract keyed on `ym=YYYY-MM`, a watermark registry in Postgres, and per-layer incremental mechanisms (vault, marts, sink, matrix, eval_mart). This project is the implementation wave. There is no design authority remaining with the product-owner - the ADR is binding and this requirements document formalizes business framing plus the acceptance criteria the data-engineer, developers, and testers must deliver against.

The deliverable is the single engineering change that turns the headline PDF section 3 metrics from "unreportable" into "reportable under incremental load".

## Scope

### In

1. **Watermark registry**: new `pipeline_watermarks(layer, table_name, last_ym, row_count, updated_at)` table in `etl4/infra/create_tables.sql`; writes happen inside the same transaction / after the same commit that materializes the target partition.
2. **Vault partitioning**: five partitioned-per-month tables (`hub_flight`, `link_flight_aircraft`, `link_flight_route`, `sat_flight_summary`, `sat_state_observation`) write to `data/silver/vault/<table>/ym=YYYY-MM/part.parquet`; three globally-unique small dims (`hub_aircraft`, `hub_airport`, `sat_aircraft_identity`) stay single-file with a tiny per-month compaction pass.
3. **Vault incremental build**: `_chunked_flightlist_vault` reads `pipeline_watermarks(vault, hub_flight)` for `last_ym`, keeps only bronze files whose extracted `ym > last_ym`, streams each into its own partition shard, then runs the small-dim compaction.
4. **Spark builder consistency**: the existing Spark `sat_state_observation` builder (`etl4/vault/raw_vault_spark.py::build_sat_state_observation_spark`) emits `ym=YYYY-MM/` partition children on write. Porting the other vault builders to Spark is IMPR-02 scope and stays out of this wave.
5. **Mart partitioning**: `fact_flight` and `fact_trajectory_enriched` are partitioned-per-month; `dim_aircraft` and `dim_airport` remain single-file. `vault/marts.py` reads with `pyarrow.dataset` + `ym > last_ym` partition filter, joins against non-partitioned dims in full, writes per-month, advances the mart watermark.
6. **Mart `--only-ym` flag**: `vault/marts.py` accepts `--only-ym YYYY-MM` to materialize exactly one month, for targeted reruns driven by `rewind`.
7. **Sink UPSERT**: `postgres_sink.py` replaces per-table `TRUNCATE` + `COPY` with `INSERT ... ON CONFLICT (hk_*) DO UPDATE` per target, per month. Conflict keys as specified in the ADR for `dim_aircraft` (`hk_aircraft`) and `fact_trajectory_enriched` (`hk_aircraft, time`). Conflict keys for `fact_flight` and `dim_airport` are an open question to the data-engineer (see Open Questions).
8. **`ym` column on `fact_trajectory_enriched`**: add `ym TEXT` to the Postgres schema, backfill existing rows from `time`, so the sink can load one month at a time and `rewind --layer sink` can delete by `ym`.
9. **Matrix uniqueness**: add `ym TEXT` to `pipeline_runs`, backfill existing rows, collapse pre-existing duplicate `(source, method, noise)` runs to a single surviving row per cell before adding `UNIQUE (ym, source, method, noise)`, then enable `ON CONFLICT DO NOTHING` on insert.
10. **`eval_mart` as plain SQL view**: converted from a `DROP TABLE` + `CREATE TABLE` materialized stage into a plain SQL view over `pipeline_runs LEFT JOIN eval_results`, preserving the 2026-11-18 ADR contract (18 columns, `position_rmse_m` -> `rmse_m` mapping, no `source='s3'` filter). `v_pareto_frontier` and its WI-1 AC-11 row-count invariant (12 rows for the synthetic matrix) must continue to hold.
11. **Rewind CLI**: `python -m etl4.orchestration.rewind --layer {vault|marts|sink|matrix|all} --from YYYY-MM --to YYYY-MM` deletes the corresponding partition directories / Postgres rows / matrix rows and rewinds the watermark. `--layer all` chains all four in the correct order and refuses to run without `--yes`.
12. **Zero-reprocess migration**: one-shot per-table script reads today's monolithic `part.parquet`, derives `ym` from `day` (flightlist outputs) or from the `date=` partition path (sat_state_observation), and writes `ym=YYYY-MM/part.parquet` via `pyarrow.parquet.write_to_dataset(..., partition_cols=["ym"])`. Zero re-processing of upstream data.
13. **Reader fallback**: during the migration window, readers fall back to the monolithic `part.parquet` when no `ym=*` children exist, so pre-migration trees keep working.
14. **Cross-month flight merge rule**: the mart reader deduplicates `link_flight_route` and `sat_flight_summary` rows that share the same `hk_flight` across two adjacent `ym` partitions, keeping the row with the later `lastseen`.
15. **`ym` helper**: one helper in the vault module symmetrises the two `ym` extraction paths (in-file `day` column for flightlist bronze; virtual `date=YYYY-MM-DD` from `pyarrow.dataset` partitioning spec for state bronze).
16. **Drift QA check**: at the top of `run_e2e`, a cheap check compares partition-directory count per table against `last_ym` from the watermark; mismatch exits non-zero.

### Out

- IMPR-02 (Spark promotion for the flightlist vault). The non-partitioned Spark port of hub_flight / link_* / sat_flight_summary / sat_aircraft_identity / hub_aircraft / hub_airport is a separate project that runs after this one.
- IMPR-03 (Iceberg / Delta migration). Not in PDF scope; stays aspirational.
- WI-1 metric changes. Ground-truth plumbing, metric definitions, scale-study CLI are frozen as of PROJ-20261114-wi1-metrics CONDITIONAL_ACCEPTANCE. This wave must not regress WI-1 AC-10 (zero NULLs in the six populated metric columns) or AC-11 (`v_pareto_frontier` row count equals the synthetic matrix cell count).
- Any new bronze ingest source. Zenodo monthly flightlist archive and S3 daily state tarballs remain the two sources.
- Any refactor of reconstruction methods (`time_gap.py`, `kinematic.py`, `kalman.py`) or noise profiles.
- Dashboard visual refreshes beyond the panels that already query the affected tables.
- Postgres schema migration tooling (Alembic / dbt). DDL changes land in `etl4/infra/create_tables.sql` with inline `ALTER TABLE ... IF NOT EXISTS` / `CREATE TABLE IF NOT EXISTS` idempotency.
- IMPR-04 through IMPR-12 backlog items that do not directly block the partition contract. IMPR-07 (vectorise `_mask_from_events`) is explicitly deferred to WI-2; its performance only matters once per-month partitions run at real volume (post-landing).

## Functional requirements

| ID | Description | Priority | Trace |
|---|---|---|---|
| FR-01 | `pipeline_watermarks(layer TEXT, table_name TEXT, last_ym TEXT, row_count BIGINT, updated_at TIMESTAMPTZ, PRIMARY KEY (layer, table_name))` plus `idx_watermarks_layer` exists in `etl4/infra/create_tables.sql` and is created idempotently by `create_tables.sh`. | must | ADR 2026-11-13 section 3 |
| FR-02 | Vault writers emit `data/silver/vault/<table>/ym=YYYY-MM/part.parquet` for `hub_flight`, `link_flight_aircraft`, `link_flight_route`, `sat_flight_summary`, `sat_state_observation`. One shard per month; shard is rewritten atomically on rerun of that month. | must | ADR 2026-11-13 section 2; PDF section 3 scaling efficiency |
| FR-03 | `hub_aircraft`, `hub_airport`, `sat_aircraft_identity` stay as single-file `part.parquet`. A per-month compaction pass reads the new month's partitioned sats/links, extracts new icao24 / airport / aircraft-identity rows, unions with the existing file, dedupes on the hash key, rewrites the single file. | must | ADR 2026-11-13 section 4 vault paragraph |
| FR-04 | `_chunked_flightlist_vault` reads `pipeline_watermarks` for `(vault, hub_flight).last_ym`, lists bronze flightlist files, filters to `extracted_ym > last_ym`, and processes each surviving file into its own monthly shard; after all shards for the month commit, the watermark advances. | must | ADR 2026-11-13 section 4 vault paragraph |
| FR-05 | The existing Spark builder `etl4/vault/raw_vault_spark.py::build_sat_state_observation_spark` writes `sat_state_observation` with `ym=YYYY-MM/` partition children (partitioned write), consistent with the pandas / DuckDB builders. `icao24` remains pandas `string` dtype on the Python read path. | must | ADR 2026-11-13 section 2 + CLAUDE.md 2.6 |
| FR-06 | `vault/marts.py` reads partitioned vault tables with `pyarrow.dataset` and a `ym > last_ym` partition filter, joins against non-partitioned dims in full, and writes `data/gold/marts/fact_flight/ym=YYYY-MM/part.parquet` and `data/gold/marts/fact_trajectory_enriched/ym=YYYY-MM/part.parquet`. Dim writers remain single-file. | must | ADR 2026-11-13 section 4 marts paragraph |
| FR-07 | `vault/marts.py` accepts `--only-ym YYYY-MM` and in that mode materializes exactly one month's partition, updates the mart watermark only if that month is the newest written. | must | ADR 2026-11-13 Consequences follow-up (a) |
| FR-08 | `postgres_sink.py` replaces `TRUNCATE` with per-partition `INSERT ... ON CONFLICT (hk_aircraft) DO UPDATE` for `dim_aircraft` and `INSERT ... ON CONFLICT (hk_aircraft, time) DO UPDATE` for `fact_trajectory_enriched`. Loads one month at a time, commits per month, advances the sink watermark. | must | ADR 2026-11-13 section 4 sink paragraph |
| FR-09 | `fact_trajectory_enriched` gets a `ym TEXT` column backfilled from `time`, so `rewind --layer sink --from X --to Y` deletes by `WHERE ym BETWEEN X AND Y`. | must | ADR 2026-11-13 Consequences follow-up (b) |
| FR-10 | `pipeline_runs` gets a `ym TEXT` column backfilled from `started_at`, pre-existing duplicate `(source, method, noise)` runs are collapsed to a single surviving row per cell via a one-shot migration, `UNIQUE (ym, source, method, noise)` is added. The matrix inserts use `ON CONFLICT (ym, source, method, noise) DO NOTHING`. Re-measurement of a cell is only possible via `rewind --layer matrix`. | must | ADR 2026-11-13 section 4 matrix paragraph + Consequences follow-up (c) |
| FR-11 | `eval_mart` is a plain SQL view over `pipeline_runs LEFT JOIN eval_results` with the 18-column shape, `position_rmse_m` -> `rmse_m` mapping, and no `source='s3'` filter established by ADR 2026-11-18. `v_pareto_frontier` is recreated against the view and returns the same row count it did under the materialized shape for the synthetic matrix (WI-1 AC-11: 12 rows). | must | ADR 2026-11-13 section 4 eval_mart paragraph + ADR 2026-11-18 |
| FR-12 | `etl4/orchestration/rewind.py` exposes a CLI `python -m etl4.orchestration.rewind --layer {vault\|marts\|sink\|matrix\|all} --from YYYY-MM --to YYYY-MM [--yes]`. Deletes partition directories (vault / marts) / Postgres rows (sink / matrix) for every `ym` in the inclusive range, then sets the corresponding `pipeline_watermarks.last_ym` to the month immediately before `--from`. `--layer all` chains vault -> marts -> sink -> matrix and refuses to run without `--yes`. | must | ADR 2026-11-13 section 5 |
| FR-13 | A zero-reprocess migration script per partitioned table reads the current monolithic `part.parquet`, derives `ym` from `day` (flightlist outputs) or from the `date=` partition path (sat_state_observation), and writes `ym=YYYY-MM/part.parquet` via `pyarrow.parquet.write_to_dataset(..., partition_cols=["ym"])`. | must | ADR 2026-11-13 section 6 |
| FR-14 | Every vault, mart, and sink reader falls back to the single `part.parquet` when no `ym=*` children exist under the table directory, so pre-migration trees keep working. | must | ADR 2026-11-13 Consequences last sentence |
| FR-15 | The mart reader deduplicates cross-month flight rows (`link_flight_route`, `sat_flight_summary`) that share the same `hk_flight` across adjacent `ym` partitions by keeping the row with the later `lastseen`. | must | ADR 2026-11-13 Risks 1 |
| FR-16 | One helper in the vault module symmetrises the two `ym` extraction paths (in-file `day` column for flightlist bronze; virtual `date=YYYY-MM-DD` via `pyarrow.dataset` partitioning spec for state bronze). | must | ADR 2026-11-13 Risks 3 |
| FR-17 | At the top of `run_e2e`, a cheap QA check counts partition directories per table and compares with `pipeline_watermarks.last_ym`; mismatch exits non-zero with a message pointing at `rewind`. | must | ADR 2026-11-13 Risks 4 |
| FR-18 | Every layer that writes partitions (vault, marts, sink, matrix) writes its own `pipeline_watermarks` row atomically with (or immediately after a successful) partition commit, including an advisory `row_count`. | must | ADR 2026-11-13 Consequences follow-up (d) |

## Non-functional requirements

| ID | Category | Requirement | Measurement |
|---|---|---|---|
| NFR-01 | performance | Single-month vault delta completes in 2 min or less on the current host, against the 20-30 min full-rebuild baseline. | wall-time measured by functional-tester on a fresh month insert |
| NFR-02 | performance | Finalize step for a single-month vault produces no transient shard footprint approaching 20 GB (the ENOSPC baseline). Peak intermediate parquet size under 2 GB. | `du -sh data/silver/vault/*/_shards/` during the run; functional-tester records peak |
| NFR-03 | correctness | Migration preserves row count and set of hash keys per partitioned table: for every table T in {hub_flight, link_flight_aircraft, link_flight_route, sat_flight_summary, sat_state_observation}, `row_count(pre_migration part.parquet) == sum(row_count(ym=* shards))` AND `set(hk_*(pre)) == set(hk_*(post))`. Measured directly against the parquet trees, NOT through `reconcile.py` (IMPR-12 drift masks the signal). | per-table reconciliation script run by functional-tester |
| NFR-04 | crash-safety | A crash after the partition write but before the watermark commit produces a clean re-write of that month with no row duplication on the next run. Per-partition writes are overwrites of the shard dir, never appends. | injected-fault test by functional-tester: `kill -9` between parquet write and watermark update, then rerun and assert row count is unchanged |
| NFR-05 | idempotency (no-op rerun) | A second `run_e2e` with no new bronze leaves vault, mart, sink partition mtimes unchanged, row counts unchanged, and all four watermark `last_ym` / `row_count` / `updated_at` timestamps unchanged. | functional-tester runs `run_e2e` twice back-to-back and diffs; second-run mtimes equal first-run mtimes |
| NFR-06 | idempotency (rewind) | `rewind --layer vault --from X --to Y` followed by a `run_e2e` produces partition shards that are byte-identical to those before the rewind (hash of each `ym=*/part.parquet` matches). Running `rewind` twice with identical arguments is a no-op after the first. | functional-tester runs rewind-then-run, diffs sha256 of shards; runs rewind twice and asserts second returns zero rows affected |
| NFR-07 | drift detection | Deleting a partition directory by hand (`rm -rf data/silver/vault/hub_flight/ym=2022-03`) causes the next `run_e2e` to exit non-zero at the drift check, with a message pointing at `rewind --layer vault --from 2022-03`. | functional-tester injects the deletion, runs `run_e2e`, asserts non-zero exit and expected message |
| NFR-08 | ym consistency | Every row written into `ym=X/part.parquet` has a calendar month equal to X (derived from `day` for flightlist tables; from `time` for state tables). Open question for the data-engineer: `floor(time / month_seconds)` suggested in ADR section 6 does NOT align to calendar months; the correct extraction is `ym = time.strftime('%Y-%m')` under UTC. See Open Questions. | per-table SQL check: `SELECT ym, MIN(time), MAX(time) FROM <table>` returns MIN and MAX bounded by the calendar month of ym |
| NFR-09 | dtype | `icao24` remains pandas `string` dtype on every `pyarrow.dataset.to_pandas()` read path (dataset reads default to `object`; explicit cast required). | unit test asserts `.dtype.name == 'string'` on the result of every reader |
| NFR-10 | WI-1 invariants held | After migration, WI-1 AC-10 (zero NULLs in rmse_m, seg_f1, seg_precision, seg_recall, continuity, boundary_err_s for every synthetic cell) and WI-1 AC-11 (`SELECT COUNT(*) FROM v_pareto_frontier` equals the synthetic matrix cell count) both still hold. | functional-tester reruns WI-1 ACs against the post-migration mart |
| NFR-11 | determinism | All stages reproducible from inputs + seed=42. Reseeding or re-running the same month produces identical shard content. | hash of `ym=*/part.parquet` identical across two runs |
| NFR-12 | platform | Windows cp1252-safe stdout. No `->`, em-dash, or non-cp1252 character emitted by rewind CLI, drift check, or any new log line. | smoke run under cp1252 locale emits no UnicodeEncodeError |
| NFR-13 | security | No secrets (OAuth tokens, DSNs) in any new file. Postgres DSN sourced from `POSTGRES_DSN` env var. | pre-commit scan of staged diff |
| NFR-14 | footprint | No tracked data commits. `data/raw`, `data/bronze`, `data/silver`, `data/gold`, `data/eval`, `logs/`, `**/_shards/`, `**/_duckdb_tmp/`, `**/_DONE` remain gitignored. | `git status --short` post-work shows only source + schema + doc changes |
| NFR-15 | attribution | No `Co-Authored-By: Claude` trailer; no `Generated with Claude Code` PR footer. | pre-commit scan |
| NFR-16 | tests | Every pre-existing test in `etl4/tests/test_pipeline.py` continues to pass. New tests cover watermark advance, rewind idempotency, drift detection, cross-month merge rule, and reader fallback. | `pytest etl4/tests/ -v` reports zero failures |
| NFR-17 | cost | Token spend minimised; agents dispatched per phase only when needed. | orchestrator status reflects per-phase dispatch |

## Acceptance criteria

| ID | For FR | Condition | How verified |
|---|---|---|---|
| AC-01 | FR-01 | `\d pipeline_watermarks` in psql lists columns `layer, table_name, last_ym, row_count, updated_at` with the specified types; `idx_watermarks_layer` appears in `\di`. | psql inspection |
| AC-02 | FR-02 | After a single-month run, `data/silver/vault/hub_flight/ym=YYYY-MM/part.parquet` exists and `data/silver/vault/hub_flight/ym=YYYY-MM-1/part.parquet` for a prior month is untouched (mtime unchanged). | functional-tester compares mtime |
| AC-03 | FR-03 | `hub_aircraft`, `hub_airport`, `sat_aircraft_identity` directories contain exactly one `part.parquet` each after the run, no `ym=*` children. Row counts equal the union-and-dedup of prior content + the new month's extracted rows. | parquet inspection |
| AC-04 | FR-04 | Insert a new bronze month, run vault, and observe: only files with extracted_ym > prior watermark are read (traced via log lines); after success, `pipeline_watermarks.last_ym` equals the new month. Prior months' shards show no mtime change. | functional-tester captures stdout + mtime diff |
| AC-05 | FR-05 | After a Spark vault run, `data/silver/vault/sat_state_observation/` contains `ym=YYYY-MM/part*.parquet` children (not `part.parquet` directly at the root). `pd.read_parquet(shard).dtypes['icao24'].name == 'string'`. | parquet inspection + dtype assert |
| AC-06 | FR-06 | After a mart run with watermark set to `2022-04`, inserting bronze for `2022-05` causes only `fact_flight/ym=2022-05/` and `fact_trajectory_enriched/ym=2022-05/` to be written; `ym=2022-01..04/` shards have unchanged mtime. | functional-tester mtime diff |
| AC-07 | FR-07 | `python -m etl4.vault.marts --only-ym 2022-03` writes exactly `fact_flight/ym=2022-03/part.parquet` and `fact_trajectory_enriched/ym=2022-03/part.parquet`; other months untouched. | file existence + mtime diff |
| AC-08 | FR-08 | A second sink run with the same bronze produces zero new rows in `fact_trajectory_enriched` and `dim_aircraft` (ON CONFLICT path); changing a `registration` value in the vault for one airframe and rerunning propagates the change to the one matching `dim_aircraft` row. | SQL count + targeted update check |
| AC-09 | FR-09 | `\d fact_trajectory_enriched` shows a `ym` column; `SELECT COUNT(*) FROM fact_trajectory_enriched WHERE ym IS NULL` returns 0 after the backfill. | psql inspection |
| AC-10 | FR-10 | `\d pipeline_runs` shows a `ym` column; `SELECT COUNT(*) FROM pipeline_runs WHERE ym IS NULL` returns 0; `SELECT ym, source, method, noise, COUNT(*) FROM pipeline_runs GROUP BY 1,2,3,4 HAVING COUNT(*) > 1` returns zero rows after the collapse migration. Re-running the matrix with identical args produces no new rows (count unchanged). | psql inspection + rerun |
| AC-11 | FR-11 | `SELECT relkind FROM pg_class WHERE relname='eval_mart'` returns `v` (view). `SELECT COUNT(*) FROM v_pareto_frontier` equals the synthetic matrix cell count (WI-1 baseline: 12). `eval_mart` exposes all 18 columns listed in ADR 2026-11-18. | psql inspection; WI-1 AC-11 re-execution |
| AC-12 | FR-12 | `python -m etl4.orchestration.rewind --layer vault --from 2022-03 --to 2022-05` deletes `ym=2022-03/`, `2022-04/`, `2022-05/` for every partitioned vault table and sets `pipeline_watermarks(vault, *).last_ym` to `2022-02` for every row. A subsequent `run_e2e --only vault` recreates the three months. `--layer all` without `--yes` exits non-zero with a prompt message. | functional-tester drives the CLI and inspects |
| AC-13 | FR-13 | Running the migration script converts the pre-existing monolithic `data/silver/vault/hub_flight/part.parquet` into `ym=*/part.parquet` children and `sum(rowcount_per_shard) == rowcount(original)` per table. Running the migration twice is a no-op (idempotent). | pre/post row-count diff |
| AC-14 | FR-14 | Delete all `ym=*` children, leave only the pre-migration `part.parquet`, run `vault/marts.py`; the reader falls back to the monolithic file and produces correct output (row count equal to monolithic file row count). | reader smoke test |
| AC-15 | FR-15 | Construct a fixture where one `hk_flight` appears in both `ym=2022-03` and `ym=2022-04` link_flight_route shards with different `lastseen`; the mart reader emits exactly one row for that `hk_flight` and its `lastseen` equals the later value. | unit test |
| AC-16 | FR-16 | A unit test calls the `ym` helper on a flightlist-bronze DataFrame (with `day` column) and on a state-bronze `pyarrow.dataset` (with `date=` partitioning spec) and asserts both return the same `ym` series shape for a known input. | unit test |
| AC-17 | FR-17 | Delete `data/silver/vault/hub_flight/ym=2022-03/` by hand; the next `run_e2e` exits non-zero with a message that includes `rewind --layer vault` and the affected month. | injected-fault test |
| AC-18 | FR-18 | After a full-matrix run, `SELECT layer, table_name, last_ym, row_count, updated_at FROM pipeline_watermarks` returns exactly one row per (layer, table_name) for every partitioned table across vault, marts, sink, matrix. `updated_at` is within the run window. | psql inspection |

## Requirement Traceability Matrix

| FR | ACs | Architecture section | Code module | Test cases |
|---|---|---|---|---|
| FR-01 | AC-01 | Watermark registry | `etl4/infra/create_tables.sql` | `test_watermark_table_schema` |
| FR-02 | AC-02 | Vault partition layout | `etl4/vault/raw_vault.py`, `etl4/vault/flightlist_vault.py` | `test_vault_writes_per_month_shard`, `test_prior_month_mtime_unchanged` |
| FR-03 | AC-03 | Vault partition layout (small-dim compaction) | `etl4/vault/raw_vault.py::_compact_small_dims` (new) | `test_small_dim_compaction_dedup` |
| FR-04 | AC-04 | Vault incremental build | `etl4/vault/flightlist_vault.py::_chunked_flightlist_vault` | `test_vault_watermark_filter`, `test_vault_watermark_advances` |
| FR-05 | AC-05 | Spark builder consistency | `etl4/vault/raw_vault_spark.py::build_sat_state_observation_spark` | `test_spark_builder_writes_ym_partitions`, `test_spark_read_icao24_string` |
| FR-06 | AC-06 | Mart partitioning | `etl4/vault/marts.py` | `test_mart_reads_partition_filter`, `test_mart_prior_month_mtime_unchanged` |
| FR-07 | AC-07 | Mart --only-ym flag | `etl4/vault/marts.py` (CLI) | `test_mart_only_ym_single_month` |
| FR-08 | AC-08 | Sink UPSERT | `etl4/sink/postgres_sink.py` | `test_sink_rerun_no_duplicates`, `test_sink_update_propagates` |
| FR-09 | AC-09 | fact_trajectory_enriched schema | `etl4/infra/create_tables.sql`, `etl4/sink/postgres_sink.py` | `test_fact_trajectory_ym_backfill` |
| FR-10 | AC-10 | Matrix uniqueness | `etl4/infra/create_tables.sql` (ALTER + UNIQUE), `etl4/bench/run_matrix.py` | `test_pipeline_runs_ym_backfill`, `test_matrix_rerun_no_duplicates` |
| FR-11 | AC-11 | eval_mart as view | `etl4/infra/create_tables.sql`, `etl4/run_e2e.py::stage_eval_mart` | `test_eval_mart_is_view`, `test_pareto_frontier_count_matches_wi1_baseline` |
| FR-12 | AC-12 | Rewind CLI | `etl4/orchestration/rewind.py` (new) | `test_rewind_vault_range`, `test_rewind_all_requires_yes`, `test_rewind_idempotent` |
| FR-13 | AC-13 | Zero-reprocess migration | `etl4/orchestration/migrate_partitions.py` (new) | `test_migration_row_count_preserved`, `test_migration_idempotent` |
| FR-14 | AC-14 | Reader fallback | `etl4/vault/marts.py`, `etl4/sink/postgres_sink.py` reader paths | `test_reader_falls_back_to_monolithic` |
| FR-15 | AC-15 | Cross-month flight merge | `etl4/vault/marts.py` reducer | `test_cross_month_flight_merge_latest_lastseen` |
| FR-16 | AC-16 | ym helper | `etl4/vault/_ym.py` (new) | `test_ym_helper_symmetric_flightlist_vs_state` |
| FR-17 | AC-17 | Drift QA check | `etl4/run_e2e.py::check_watermark_vs_filesystem` (new) | `test_drift_check_hand_deletion` |
| FR-18 | AC-18 | Watermark writes everywhere | `etl4/vault/raw_vault.py`, `etl4/vault/marts.py`, `etl4/sink/postgres_sink.py`, `etl4/bench/run_matrix.py` | `test_watermark_row_per_layer_per_table` |

Traceability coverage: 18 / 18 FRs trace to at least one AC, one architecture section, one code module, and one test case = **100%**.

## Open questions

1. **`v_pareto_frontier` cardinality after `ym` lands on `pipeline_runs`**: once `ym` is in `pipeline_runs`, does `v_pareto_frontier` go per-(ym, method, noise) (which would multiply the current 12-row count by the number of distinct `ym` values) or stay per-(method, noise) by averaging / most-recent across months? WI-1 AC-11 asserts count == 12. **Proposed default**: most-recent `ym` per (source, method, noise), so WI-1 AC-11 continues to hold. Data-engineer to confirm or propose an alternative.
2. **Matrix re-measurement semantics**: `ON CONFLICT (ym, source, method, noise) DO NOTHING` contradicts WI-1 NFR-05 ("re-running overwrites the prior cell"). The ADR resolves this by routing re-measurement through `rewind --layer matrix` (delete then re-insert). **Proposed default**: document the new contract; update WI-1 NFR-05 to "re-running is a no-op unless rewind precedes it". Data-engineer to confirm.
3. **Sink conflict keys for `fact_flight` and `dim_airport`**: ADR 2026-11-13 only specifies conflict keys for `dim_aircraft` (`hk_aircraft`) and `fact_trajectory_enriched` (`hk_aircraft, time`). **Proposed default**: `fact_flight ON CONFLICT (hk_flight)`; `dim_airport ON CONFLICT (hk_airport)`. Data-engineer to verify the DDL actually declares these as unique constraints (ON CONFLICT needs a target index).
4. **Orphaned `eval_results` rows under ON CONFLICT DO NOTHING**: if a matrix rerun computes metrics, finds the `pipeline_runs` row already present, and DO-NOTHINGs, do the computed `eval_results` rows get inserted for that run_id (creating orphans) or does the whole transaction roll back? **Proposed default**: compute-and-skip (do not insert into `eval_results` at all when `pipeline_runs` DO-NOTHINGs). Data-engineer to specify.
5. **`sat_state_observation` migration ym extraction**: ADR section 6 proposes `floor(time / month_seconds)` which does NOT align to calendar months (a month is 28-31 days, not a fixed-size bucket). **Proposed default**: `ym = pd.Timestamp(time, unit='s', tz='UTC').strftime('%Y-%m')`. Data-engineer to confirm.
6. **IMPR-12 status at kickoff**: the backlog states IMPR-12 (20.75% flightlist->hub_flight reconcile drift, likely dedup artefact) "must be filed and triaged before IMPR-01 kickoff so the partition contract does not inherit an undiagnosed drop as noise in its own reconcile numbers." If IMPR-12 is not resolved before architecture starts, NFR-03 cannot use `reconcile.py` as the measurement instrument (and this requirements doc explicitly rules that out). Orchestrator to confirm IMPR-12 is either closed or explicitly deferred with the data-engineer informed.

Open questions are not blockers for approval; they are decisions the data-engineer owns in the next phase.

## Risks

1. **IMPR-12 open at kickoff**: if the 20.75% reconcile drift is not diagnosed, downstream validation of migration no-row-loss has to bypass `reconcile.py` and measure directly against parquet, per NFR-03.
2. **PDF read confirmed**: PDF section 3 was successfully read in this requirements turn; citations are first-hand (not via backlog proxy).
3. **Spark promotion sequencing**: IMPR-02 is scheduled after IMPR-01. The partition contract in FR-05 (`sat_state_observation` writes ym partitions) touches the Spark code path. If IMPR-02 lands close behind, the port of the other vault builders to Spark needs to respect the ym partition contract this project establishes.
4. **Cross-ADR drift**: the state.json `upstream_design.scope_summary` says "eval_mart become a view or materialized view"; ADR 2026-11-13 says "plain SQL view" and ADR 2026-11-18 rejects materialized views. This document takes the ADR line (plain view) and flags the state.json wording as benign drift to be corrected by orchestrator on acceptance.

## Status

`APPROVED`
