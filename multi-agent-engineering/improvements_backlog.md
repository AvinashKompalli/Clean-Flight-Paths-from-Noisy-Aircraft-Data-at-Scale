# ETL4 improvements backlog

Ranked, scoped follow-ups for after PROJ-20261114-wi1-metrics closes. Each
item has an explicit PDF/plan.md trace, a scope boundary, an effort estimate,
and a prerequisite list. None of these are in flight; the orchestrator will
only dispatch a specialist against one when the user runs `/workflow` with
its slug.

Rank order reflects impact against the PDF's §3 Performance Metrics for
Success and §2 High-Level Design, weighted by effort.

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

## Not on this list (and why)

- Anomaly-detection streaming promotion (WI-2 Phase B from the earlier discussion): already named, scoped and user-confirmed; lives separately as its own project kickoff after WI-1, not an improvement.
- Deviation-detection dashboard (WI-2 Phase A): same - that is the next in-flight project, not an improvement.
- Fixing the `test_matrix_csv_written_after_every_cell` pre-existing failure: tiny, surgical, noise - do it as a one-shot `/workflow` without going through the improvements backlog.
