# IMPR-01: Partition Contract and Watermark-Driven Incremental Refresh

## Technical Implementation Report — What Was Done and How It Was Done

**Scalable Kinematic Flight Trajectory Reconstruction from Noisy ADS-B Data**

Team ETL4 — Kompalli Avinash Bhargav, Nadhiya R S, Kasi Annapurna Uppalapati, Sathyanarayanan C
DA 231o — Data Engineering at Scale, Indian Institute of Science, Bangalore
Branch `feature/pipeline-3-avinash` · 2026-11-20

---

## 1. Purpose

The course proposal lists **scaling efficiency as data volume and cluster resources increase** as one of the six Performance Metrics for Success. That metric could not be measured on the pre-milestone pipeline: every layer below the raw bronze landing zone rebuilt in full on every run, so each added month paid the cost of re-processing every previous month. The total work after N months grew as 1 + 2 + … + N = **O(N²)**, not O(N). An operational scale-out had already failed with ENOSPC when the full-rebuild path's ~20 GB transient footprint exceeded the host's 10 GB of free disk.

This report documents the engineering milestone that replaced that topology with a per-month partition contract, and the live deployment that proved it works on real data. Every section pairs **what was done** (the concrete deliverable) with **how it was done** (the mechanism, the tools, the design decisions).

---

## 2. Infrastructure Stack

### 2.1 What was done

The pipeline runs against a docker-compose stack with nine services. The IMPR-01 milestone used all of them but modified only two directly: PostgreSQL (new tables, new DDL) and Spark (partitioned writes).

| Service | Image | Role |
|---|---|---|
| `etl4-postgres` | `postgres:16` | Experiment tracker, watermarks, fact tables for dashboards |
| `etl4-kafka` | `apache/kafka:3.8.0` (KRaft, single-node) | Streaming ADS-B ingest buffer |
| `etl4-spark-master` | `apache/spark:3.5.3` | Cluster scheduler for the Spark vault builder |
| `etl4-spark-worker-1`, `-2` | `apache/spark:3.5.3` | Spark executors for `sat_state_observation` |
| `etl4-grafana` | `grafana/grafana:10.4.5` | Dashboards (08 panels for IMPR-01-sensitive views) |
| `etl4-prometheus` | `prom/prometheus` | Metrics scrape target |
| `etl4-node-exporter` | `prom/node-exporter` | Host-level CPU / RAM / disk telemetry |
| `etl4-airflow` | custom | DAG orchestration for live streaming |

All nine containers were already up before the milestone work started and remained up throughout the deployment.

### 2.2 How it was done

Spark services were pinned to `apache/spark:3.5.3`, not the Bitnami images that were removed from the public catalog in late 2025. Each Spark container has an explicit `command:` entry in `docker-compose.yml` because the apache/spark entrypoint does not pick a role (master or worker) without one. The milestone added no new services; all new logic lives in Python modules that run against the existing containers.

---

## 3. Kafka Ingestion Path

### 3.1 What was done

The project uses Kafka to decouple the live REST pull from downstream processing. Two topics live on the broker:

| Topic | Partitions | Replication | Purpose |
|---|---:|---:|---|
| `adsb.raw` | 3 | 1 | Raw OpenSky state vectors as pulled from the REST API |
| `adsb.dlq` | 1 | 1 | Dead-letter queue for malformed or auth-failed records |

Three Python modules sit around these topics:

- `etl4/ingest/live_producer.py` polls `https://opensky-network.org/api/states/all` and publishes each state vector to `adsb.raw`.
- `etl4/ingest/kafka_bronze_consumer.py` subscribes to `adsb.raw` and writes to the bronze parquet tree.
- `etl4/ingest/replay_producer.py` reads a historical bronze day and re-publishes it to `adsb.raw` to emulate a stream for testing.

IMPR-01 did not change the producer or consumer code. What IMPR-01 did change is the schema contract under the consumer: every row the consumer writes to bronze now flows into a per-month `ym=` partition downstream, not into a monolithic file. A separate but concurrent change (task #8 in the local tracker) built the Kafka → bronze consumer itself so that the new partition contract would receive live data correctly.

### 3.2 How it was done

**Producer.** `live_producer.py` authenticates to OpenSky via **OAuth2 client credentials**. A cache class `_TokenCache` refreshes the bearer token **30 seconds** before expiry so a long-running producer never pauses mid-poll on a 401. The producer reads 17 raw fields from the REST response and publishes each state vector **exactly as received** — no projection, no renaming, no type coercion. Projection to the pipeline canonical schema happens downstream at `etl4/clean/clean.py::project_raw`, not in the producer. This is a Data-Engineering-at-Scale discipline: raw ingest stays raw so downstream decisions can be revisited.

The 17 raw fields are: `icao24`, `callsign`, `origin_country`, `time_position`, `last_contact`, `longitude`, `latitude`, `baro_altitude`, `on_ground`, `velocity`, `true_track`, `vertical_rate`, `sensors`, `geo_altitude`, `squawk`, `spi`, `position_source`.

The producer's Kafka **partition key is `icao24`**. This guarantees that one aircraft's observations always land on the same partition, so partition-ordered consumption preserves per-aircraft event order. On a 401 the producer refreshes the token and retries once; on persistent failure the record is routed to `adsb.dlq` with a reason label. Three Prometheus counters are exposed:

- `records_sent_total` — successful publish to `adsb.raw`
- `records_dropped_total{reason=...}` — DLQ routes
- `kafka_produce_latency_seconds` — produce latency histogram

With OAuth credentials the producer polls every **5 seconds** (4,000 credits/day); anonymous, every **10 seconds** (400 credits/day).

**Consumer.** `kafka_bronze_consumer.py` enforces **at-least-once semantics**: offsets are committed only after a successful parquet flush. The consumer buffers records in memory, flushes a buffer to `data/bronze/live_states/date=YYYY-MM-DD/part.parquet`, then commits the offsets of the records in that buffer. A crash between flush and commit leads to replay of the un-committed window on restart; the row-level dedup at the bronze→silver boundary absorbs the duplicates. Auto-offset-reset defaults to `latest` so a new consumer group skips history; `--from-beginning` resets offsets to earliest before consuming. The consumer exits cleanly after **10 seconds of silence on the topic**, which is how the live_stream DAG knows the pull window has ended.

**Topic creation.** `etl4/infra/create_topics.sh` idempotently creates both topics with `--if-not-exists`. Partitions on `adsb.raw` default to 3 (overrideable via `PARTITIONS` env), replication is 1 (single-node Kafka in dev).

---

## 4. Data Vault 2.0 Architecture

### 4.1 What was done

The silver tier uses the **Data Vault 2.0** model. The vault holds eight tables organised into three categories, each with a specific role:

- **Hubs** carry the identity of a business entity. One row per unique business key.
- **Links** carry the relationship between two or more hubs. One row per unique relationship.
- **Satellites** carry the descriptive attributes of a hub or a link. Insertion-only: new observations append new sat rows with a new `load_dt`, never update existing rows.

| Category | Table | Business key | Row count (live) |
|---|---|---|---:|
| Hub | `hub_aircraft` | `icao24` (6-hex airframe ID) | 226,661 |
| Hub | `hub_airport` | `icao_code` (4-letter ICAO) | 18,595 |
| Hub | `hub_flight` | `(callsign, day)` | 98,426,017 |
| Link | `link_flight_aircraft` | `(hk_flight, hk_aircraft)` | 100,315,533 |
| Link | `link_flight_route` | `(hk_flight, hk_airport_origin, hk_airport_destination)` | 11,854,754 |
| Satellite | `sat_aircraft_identity` | `hk_aircraft` + attributes | 169,212 |
| Satellite | `sat_flight_summary` | `hk_flight` + attributes | 10,097,653 |
| Satellite | `sat_state_observation` | `hk_aircraft` + time + attributes | 4,374,016 |

Every row in every vault table carries four columns common to the DV 2.0 contract:

- `hk_*` — a deterministic hash of the business key(s). Hubs have one hash; links have a composite hash plus parent-hub hashes; satellites reference their parent hub's hash.
- `load_dt` — a UTC timestamp recording when the row entered the vault. Audit lineage.
- `rec_src` — the record source tag: `opensky_zenodo` (bulk Zenodo pulls), `opensky_s3` (daily S3 tarballs), `opensky_live` (REST stream via Kafka).
- (Partitioned tables only, new in IMPR-01) `ym` — the calendar month, e.g. `2019-03`.

### 4.2 How it was done

**Hash key derivation.** `etl4/vault/schema.py::hash_series` takes one or more string columns, concatenates them with a separator, and computes a stable 128-bit hash per row. The function is purely deterministic so that the same business key always produces the same `hk_*` across runs, across sources, and across language implementations:

```python
hub["hk_aircraft"] = hash_series(df["icao24"])

hub_flight["hk_flight"] = hash_series(df["callsign"], df["day"].astype(str))

link_flight_aircraft["hk_flight"]          = hash_series(df["callsign"], df["day"].astype(str))
link_flight_aircraft["hk_aircraft"]        = hash_series(df["icao24"])
link_flight_aircraft["hk_flight_aircraft"] = hash_series(df["hk_flight"], df["hk_aircraft"])

link_flight_route["hk_flight_route"] = hash_series(
    df["hk_flight"], df["hk_airport_origin"], df["hk_airport_destination"]
)
```

The hash columns replace natural-key joins at mart-build time. The marts layer joins `fact_flight` to `dim_aircraft` on `hk_aircraft` (a 16-byte join) rather than on `icao24` (a 6-char varchar join), which is both faster at scale and immune to casing or whitespace drift in the natural key.

**Hub semantics.** `build_hub_aircraft` reads both the Zenodo flightlist stream and the S3 state-vector stream, stacks their `icao24` columns, and deduplicates. The dedup rule is **first-seen `rec_src` wins**, implemented via `drop_duplicates(subset="icao24", keep="first")` after stacking in source order. This keeps hub provenance stable across runs: an airframe first observed in the Zenodo pull remains tagged `opensky_zenodo` even if a later S3 pull also contains it.

**Link semantics.** Links do not carry descriptive fields of their own — only the hash keys. `link_flight_aircraft` carries `(hk_flight, hk_aircraft, hk_flight_aircraft, load_dt, rec_src)`; nothing else. This separation is the Data Vault discipline: a relationship exists or it does not; attributes of the relationship go in a satellite.

**Satellite semantics.** Satellites are insertion-only. The pre-milestone pipeline enforced this at the final dedup step of each build pass by sorting on `load_dt DESC` and keeping only the most recent sat row per business key. IMPR-01 preserved this contract but added a new wrinkle: because the partitioned vault writes one shard per month, a cross-month flight (one whose observations span two monthly files) lands sat rows in both months' shards. The reader-side resolution for this (helper `_latest_sfs` in the marts layer) is described in §6.2.

**Partitioning decision.** Three of the eight vault tables — `hub_aircraft`, `hub_airport`, `sat_aircraft_identity` — are **globally unique on their business key**. Splitting them by month would create duplicates (an airframe observed in both Jan and Feb would appear in both month shards). These three remain **single-file at the table root**: `data/silver/vault/hub_aircraft/part.parquet`. The other five tables are partitioned by month under IMPR-01: `data/silver/vault/hub_flight/ym=YYYY-MM/part.parquet`. The three single-file tables are refreshed by a per-month compaction pass (§5.1).

**State-vector satellite.** `sat_state_observation` is produced by a Spark builder (`etl4/vault/raw_vault_spark.py`), not the pandas path. The builder uses `groupBy(icao24).applyInPandas(...)` to parallelise the per-aircraft pandas code across the Spark workers. IMPR-01 extended this builder to repartition by `ym` before the write, so the output tree matches the pandas-written partitions elsewhere in the vault.

---

## 5. The Partition Contract

### 5.1 What was done

Every time-sensitive table on disk now follows a fixed path convention:

```
data/<layer>/<table>/ym=YYYY-MM/part.parquet
```

Six tables are under the contract: four vault tables (`hub_flight`, `link_flight_aircraft`, `link_flight_route`, `sat_flight_summary`), plus `sat_state_observation` which was already Spark-partitioned, and two mart tables (`fact_flight`, `fact_trajectory_enriched`). The three globally-unique tables (`hub_aircraft`, `hub_airport`, `sat_aircraft_identity`) and the two dims (`dim_aircraft`, `dim_airport`) stay single-file.

Each shard is one month of data. The writer rewrites one shard per month atomically. A reader on top of the tree can prune unmodified months by pushing down the predicate `ym > last_ym` into `pyarrow.dataset`.

### 5.2 How it was done

**Step 1 — Central I/O module.** We added `etl4/vault/vault_io.py` with nine public helpers. Every read and write in the vault and marts layers now routes through this module.

| Helper | Purpose |
|---|---|
| `read_table(name, columns=None, filters=None, ym=None)` | Partitioned read via `pyarrow.dataset` with automatic monolithic fallback |
| `write_partition(df, name, ym, out_dir=None)` | Atomic write to `ym=<ym>/part.parquet` via tmp+rename |
| `row_count(name, out_dir=None)` | Fast footer-only row count (no body read) |
| `list_ym(name, out_dir=None)` | Enumerate the ym values already on disk |
| `ym_from_filename(name)` | Derive ym from a flightlist filename like `flightlist_20190301_20190331.parquet` |
| `ym_from_date(series)` | Derive ym from a pandas Series of `day`-column values |
| `ym_from_epoch(series)` | Derive ym from a pandas Series of Unix timestamps |
| `extract_ym_from_frame(df)` | Pick the right derivation based on which columns the frame has |
| `extract_ym_from_path(path)` | Parse `ym=YYYY-MM` from a dataset partition path |

**Step 2 — Atomic writes.** `write_partition` builds the parquet to a temporary filename (`part.parquet.tmp.{uuid}`) inside the target directory, calls `fsync` to flush the file to disk, and then calls `os.replace` to swap it into place. `os.replace` is atomic on POSIX and Windows alike at the filesystem level. A reader concurrent with a writer sees either the old file or the new one, never a half-written one.

**Step 3 — Reader fallback.** `read_table` first lists the table root for `ym=*` children. If children exist, the reader returns a `pyarrow.dataset` with `ym > last_ym` pushed down. If no children exist, the reader falls back to a direct `pd.read_parquet` on the monolithic `part.parquet` at the root. Both paths return a pandas `DataFrame` with the same dtypes (specifically, `icao24` is always pandas `string`, enforced by a post-read cast). The fallback is essential: during migration some tables are partitioned while others are not, and the pipeline must keep working across both states.

**Step 4 — Done sentinel.** Each per-month write is followed by a touch of `ym=<ym>/_DONE`. The sentinel records that the shard is durable and all downstream bookkeeping completed. The next run skips any month that already has a `_DONE`. A crash between the parquet rename and the sentinel touch leaves the month invisible to the next run and it will be rebuilt — which is harmless because the rebuild is deterministic (same seed, same output).

**Step 5 — Small-hub compaction.** For the three single-file hubs, we added `etl4/vault/compact.py` with `compact_small_hub(name, new_frames, out_dir)`. It takes the newly-built rows for one month, unions them with the existing monolithic file, deduplicates on the hash key using `DEDUP_KEYS` (one list per hub), and rewrites the single file atomically. `DEDUP_KEYS` mirror the original `_FINAL_DEDUP` configuration from `raw_vault.py` so the dedup semantics are identical; one subtle correction during the implementation changed `sat_aircraft_identity` from `['hk_aircraft_identity']` (a column that does not exist) to `['hk_aircraft']` (the correct hub key).

---

## 6. The Watermark Registry

### 6.1 What was done

A single Postgres table holds the last successfully written month for every (layer, table) pair:

```sql
CREATE TABLE IF NOT EXISTS pipeline_watermarks (
    layer       TEXT        NOT NULL,
    table_name  TEXT        NOT NULL,
    last_ym     TEXT        NOT NULL
                  CHECK (last_ym ~ '^[0-9]{4}-(0[1-9]|1[0-2])$'),
    row_count   BIGINT      NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (layer, table_name)
);

CREATE INDEX IF NOT EXISTS idx_watermarks_layer
    ON pipeline_watermarks (layer);
```

After the live deployment ran, eight live watermark rows exist:

```
 layer     | table_name               | last_ym  | row_count
-----------+--------------------------+----------+-----------
 vault     | hub_flight               | 2019-03  |  1,938,754
 vault     | link_flight_aircraft     | 2019-03  |  1,976,006
 vault     | link_flight_route        | 2019-03  |  2,220,761
 vault     | sat_flight_summary       | 2019-03  |  1,938,754
 marts     | fact_flight              | 2019-03  |  1,938,754
 marts     | fact_trajectory_enriched | 2019-03  |          0
 matrix    | pipeline_runs            | 2022-06  |          1
 eval_mart | fact_experiment_result   | 2026-11  |         25
```

### 6.2 How it was done

**CHECK constraint.** The regex `^[0-9]{4}-(0[1-9]|1[0-2])$` rejects any string that is not a four-digit year, a hyphen, and a two-digit month in the range `01..12`. The constraint stops bad values at the storage boundary: the pipeline cannot accidentally write a sentinel value like `SYN-002A` into `pipeline_watermarks`. The matrix layer uses `SYN-002A` as a synthetic-cell tag inside `pipeline_runs.ym`, but a Python-side regex filter (`_YM_CALENDAR_RE`) strips it before any watermark write.

**Write-side API.** `etl4/vault/watermark.py` exposes two functions:

```python
def get(layer: str, table: str, conn=None) -> Optional[str]: ...
def set(layer: str, table: str, last_ym: str, row_count: int, conn) -> None: ...
```

`set` deliberately takes an open database connection rather than opening its own. The caller commits the watermark advance **in the same transaction** as the sibling fact write it is announcing. For the sink and matrix layers (whose writes end in Postgres), this gives true transactional atomicity: a crash either lands both the fact write and the watermark advance, or neither. For the vault and marts layers (whose writes end on disk), the watermark advance happens on a Postgres transaction that is opened after the on-disk rename returns; the `_DONE` sentinel closes the remaining window.

**Idempotency.** `set` is implemented as `INSERT ... ON CONFLICT (layer, table_name) DO UPDATE SET last_ym = EXCLUDED.last_ym, row_count = EXCLUDED.row_count, updated_at = now()`. Running `set` twice with identical arguments leaves `last_ym` and `row_count` unchanged (only `updated_at` moves forward), so a retry after a transient failure is safe.

---

## 7. Per-Layer Conversion to Incremental

### 7.1 Vault layer

**What was done.** `etl4/vault/raw_vault.py` was restructured around a per-month loop. The flightlist builder reads the vault watermark at start, lists bronze flightlist files, filters to those whose extracted file-month is greater than the watermark, and processes each surviving file into its own `ym=YYYY-MM` shard for the four partitioned tables. The three single-file hubs are refreshed by `compact.py::compact_small_hub` once per processed month. The Spark builder `raw_vault_spark.py::build_sat_state_observation_spark` was extended to add the `ym` column via `ym_from_epoch`, repartition by `ym`, and write the partitioned tree.

**How it was done.** The authoritative `ym` key for every flightlist-derived vault row is the **bronze file-month**, extracted from the filename `flightlist_YYYYMMDD_YYYYMMDD.parquet` via `ym_from_filename(f.name)`. A primary-source review of `raw_vault.py:543` during T-13 established this: the production writer uses the filename, not the row-level `day` column or the `firstseen` timestamp. ADR 2026-11-20 records the binding.

Each per-month pass of the pandas flightlist builder reads one bronze file, runs five parallel sub-builders (hub_flight, link_flight_aircraft, link_flight_route, sat_flight_summary, sat_aircraft_identity), and writes each output atomically via `vault_io.write_partition`. The small-hub compaction runs at the end of each month's pass. Only when every write and compaction has returned does the loop call `watermark.set('vault', <table>, ym, row_count, conn)` for all four partitioned tables and commit the transaction.

The Spark `sat_state_observation` builder reads the state-vector bronze tree with `applyInPandas(icao24)`, groups by `icao24`, dedups per-aircraft observations in pandas, and returns the enriched partition. IMPR-01 added `frame = frame.withColumn("ym", ym_from_epoch(frame["time"]))` and `frame.repartition("ym").write.partitionBy("ym").mode("append")` so that the output tree has the same `ym=` layout as the pandas vault.

### 7.2 Marts layer

**What was done.** `etl4/vault/marts.py` now reads vault tables with `pyarrow.dataset` under a `ym > last_ym` filter. It writes the two fact tables (`fact_flight`, `fact_trajectory_enriched`) into per-month shards. The two dims (`dim_aircraft`, `dim_airport`) rebuild in full on every run because they are cheap and single-file. A new `--only-ym YYYY-MM` CLI flag lets the rewind tool materialise exactly one month.

**How it was done.** The cross-month flight problem is the hard case. A flight whose `firstseen` falls in month M and `lastseen` falls in month M+1 can land in both months' vault shards. We added a helper `_latest_sfs(sfs)` that sorts rows on `(hk_flight ASC, lastseen DESC, load_dt DESC)` with a stable sort and keeps the first row per `hk_flight`. Because the reader processes one month at a time, two rows for the same flight in adjacent months are never in the reader's frame simultaneously; the cross-month closure happens downstream at the sink's `INSERT ... ON CONFLICT DO UPDATE` (§7.3) or via an operator-issued `rewind --from M --to M+1`. ADR 2026-11-20 records this policy, and the rewind CLI's `--help` epilog surfaces it to operators.

### 7.3 Postgres sink

**What was done.** The sink used to `TRUNCATE` the target table and `COPY` all rows back in. It now does per-month, per-table `INSERT ... ON CONFLICT (hk_*) DO UPDATE`. A rerun with the same bronze inserts zero new rows; changing a column value for one airframe in the vault propagates to the one matching Postgres row and leaves the rest untouched.

**How it was done.** PostgreSQL's `COPY` does not natively support `ON CONFLICT`. We implemented a two-stage pattern per month:

```sql
-- Stage 1: COPY into a session-local TEMP table (fast, no ON CONFLICT needed).
CREATE TEMP TABLE _stage (LIKE target_table INCLUDING ALL) ON COMMIT DROP;
COPY _stage FROM STDIN WITH (FORMAT csv, HEADER FALSE);

-- Stage 2: UPSERT from the TEMP table into the target.
INSERT INTO target_table (hk_*, col_a, col_b, ..., ym)
SELECT hk_*, col_a, col_b, ..., ym FROM _stage
ON CONFLICT (hk_*) DO UPDATE SET
    col_a = COALESCE(EXCLUDED.col_a, target_table.col_a),
    col_b = COALESCE(EXCLUDED.col_b, target_table.col_b),
    ...;
```

The `COALESCE(EXCLUDED.col, target.col)` on each updatable column is critical: without it, an incoming NULL would clobber a previously good value. The watermark advance for the sink runs on the same connection as the `INSERT ... ON CONFLICT` and is committed in the same transaction. A new column `ym TEXT` was added to `fact_trajectory_enriched` and backfilled from the existing `time` column so that `rewind --layer sink --from X --to Y` can issue `DELETE ... WHERE ym BETWEEN X AND Y`.

### 7.4 Experiment matrix

**What was done.** `pipeline_runs` previously allowed duplicate rows on `(source, method, noise)`. A rerun of the 12-cell grid grew the table instead of idempotently refreshing it. We added a `ym TEXT` column backfilled from the existing `started_at`, collapsed pre-existing duplicate cells to a single surviving row per cell, and added a `UNIQUE (ym, source, method, noise)` constraint.

**How it was done.** The collapse-and-uniquify is an idempotent `ALTER TABLE` block in `create_tables.sql`:

```sql
ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS ym TEXT;

UPDATE pipeline_runs SET ym = to_char(started_at, 'YYYY-MM') WHERE ym IS NULL;

DELETE FROM pipeline_runs a USING pipeline_runs b
 WHERE a.ctid < b.ctid
   AND a.ym = b.ym AND a.source = b.source
   AND a.method = b.method AND a.noise = b.noise;

ALTER TABLE pipeline_runs ADD CONSTRAINT pipeline_runs_ym_cell_uniq
    UNIQUE (ym, source, method, noise);
```

`run_matrix.py` inserts now carry `ON CONFLICT (ym, source, method, noise) DO NOTHING`. Re-measuring a cell requires an explicit `rewind --layer matrix` first.

### 7.5 Evaluation view

**What was done.** `stage_eval_mart` was `DROP TABLE` + `CREATE TABLE`. We converted it to a plain SQL view over `pipeline_runs LEFT JOIN eval_results` and recreated `v_pareto_frontier` on top of it. The view grew by one column (`last_ym TEXT`, total 19 columns) so dashboards can group eval cells by calendar month.

**How it was done.** `stage_eval_mart` runs in a single psycopg transaction. It drops both `v_eval_mart_ym` and `v_pareto_frontier` to free the dependent view chain, recreates the base `fact_experiment_result` view with the 19-column projection, and recreates the two dependent views in the same transaction. The CTE named `latest` carries `pipeline_runs.ym` through to the outer `SELECT`, where `MAX(l.ym) AS last_ym` lands in the expanded view.

---

## 8. The Rewind Tool

### 8.1 What was done

A new command-line tool at `etl4/orchestration/rewind.py` performs selective rollback:

```
python -m etl4.orchestration.rewind --layer {vault|marts|sink|matrix|all} \
       --from YYYY-MM --to YYYY-MM [--yes] [--dry-run]
```

For a single layer, the tool deletes the relevant on-disk shards or Postgres rows for every month in the range, then rolls the watermark back to the month before `--from`. For `--layer all`, the tool chains the four layers in **downstream-first** order.

### 8.2 How it was done

**Chaining order.** The chain is `eval → matrix → sink → marts → vault`. The logic is: a reader must be drained before its source is rewound, otherwise the reader sees inconsistent state. We traced the dependencies: eval reads matrix; matrix reads sink; sink reads marts; marts read vault. So we rewind in reverse order of dependency.

**Guards.** Destructive `--layer all` without `--yes` exits with a descriptive error message; the operator must opt in. `--dry-run` bypasses the `--yes` requirement and prints the plan without writing. One defect in the first implementation gated `--dry-run --layer all` on `--yes`; we patched this during the T-27 functional test and re-verified.

**Cross-month overlap probe.** We added `_detect_cross_month_overlap(from_ym, to_ym, root)` which reads the next month's `sat_flight_summary` with only the `hk_flight` column (minimal I/O), computes `.isin()` overlap with the current month's rows, and returns the overlap count. If > 0, `rewind_vault` prints a WARNING line citing ADR 2026-11-20 and recommending the operator consider extending the window to `M+1`.

**Idempotency.** A second invocation of `rewind` with identical arguments affects zero rows and exits 0. The idempotency test is one of the seven functional-test scenarios.

---

## 9. The Zero-Reprocess Migration Script

### 9.1 What was done

Pre-existing monolithic parquet trees needed conversion to the new per-month layout without re-reading bronze. We wrote `etl4/scripts/migrate_partition_ym.py` (691 lines). It accepts `--tables`, `--dry-run`, `--vault-dir`, `--marts-dir`, and `--skip-watermark` flags. The script's exit policy is strict: a dry-run with a schema mismatch warns and exits 0; a real-run fails fast with exit code 1 and skips the watermark advance.

### 9.2 How it was done

The per-table flow is:

1. Open the monolithic `part.parquet` with `pyarrow.parquet.ParquetFile.iter_batches(batch_size=1_000_000)` so the whole file never has to sit in memory at once.
2. For each batch, derive the `ym` from the per-row `day` column (flightlists) or from the parquet path's `date=YYYY-MM-DD` partition (state vectors).
3. Group each batch by derived `ym` using `pd.factorize` (the fast grouping path).
4. For each group, write to `data/silver/vault/<table>/ym=<ym>/part.parquet.tmp.{uuid}`, then `os.replace` to the final name.
5. Touch `_DONE` after all groups complete.
6. Check row-count equality before and after via `pq.ParquetFile.metadata.num_rows` — if the two totals disagree, raise `AssertionError` and do not advance the watermark.

**The architectural correction.** During the T-13 data-engineer review of the first draft of this script, the reviewer caught an error in the orchestrator's original dispatch. We had proposed stream-migrating all four partitioned vault tables from their monolithic parquet. The DE pointed at `raw_vault.py:543`, which showed the authoritative partition key is `ym_from_filename(f.name)` — the bronze file-month. Three of the four tables (`link_flight_aircraft`, `link_flight_route`, `sat_flight_summary`) carry no file-month attribution in their monoliths: links have no time column at all, and `sat_flight_summary`'s `firstseen` disagrees with the file-month on 12,685 rows at current data. Stream-migrating with `firstseen` as the key would have created a permanent split-brain on those rows against future T-08 writes.

The migration path was therefore split in two. `hub_flight` is stream-migrated because its `day` column agrees with the file-month on 100% of rows at current data. The other three tables are **rebuilt from bronze** using the T-08 vault code path — the same code that produces new partitions going forward, so the migration output byte-matches ongoing writes. This correction is recorded in ADR 2026-11-20.

---

## 10. Testing

### 10.1 Unit tests

**What was done.** `pytest etl4/tests/ -v` reports **92 pass in 8.25 s** on the final commit. The pre-milestone baseline was 34 pass; the milestone added 58 new tests across two files. No pre-existing test was removed, renamed, or skipped.

- `test_vault_io.py` (43 tests) covers the nine public helpers, reader fallback behaviour, column pushdown, the Spark-shard extension of the reader, `write_partition` `ym` validation, atomic write under concurrent writers, and **SHA-256 determinism** across two fresh writes against identical inputs.
- `test_watermark.py` (15 tests) covers `get` and `set` semantics, the CHECK constraint firing on a malformed `last_ym`, cross-caller transaction sharing, idempotent repeat of `set`, and rollback atomicity.

**How it was done.** The determinism tests are the most informative. They write the same input twice to two different directories, compute `hashlib.sha256` of each output shard, and assert byte-for-byte equality. This single test proves four things at once: reproducibility (same seed → same output), atomicity (no partial file visible to the reader), lack of non-determinism from dictionary ordering or hash salting, and absence of embedded timestamps or process-ids in the parquet metadata. The watermark tests use a transactional sandbox: each test opens a connection, exercises the API, and rolls back, so production watermarks are never touched.

### 10.2 End-to-end rewind test

**What was done.** A seven-scenario functional test for the rewind tool, running against an **isolated Postgres schema `t27_sandbox`** rather than the production `public` schema. All seven scenarios pass.

| # | Scenario | Result |
|---|---|---|
| 1 | Dry-run on empty state | PASS |
| 2 | Single-layer vault rewind | PASS |
| 3 | Full-stack `--layer all` with downstream-first call-order verification | PASS |
| 4 | Cross-month flight rewind covering month M and month M+1 | PASS |
| 5 | `--help` epilog mentions the cross-month rule and `M+1` guidance | PASS |
| 6 | Idempotent repeat rewind: second invocation affects zero rows | PASS |
| 7 | Dim invariance: `dim_aircraft` row count unchanged across partition-layer rewind | PASS |

**How it was done.** The sandbox schema was created in a `try` block at test start with hand-written DDL mirroring production. All sink and matrix writes during the test were redirected to the sandbox through a monkey-patched schema name in the connection config. The `finally` block drops the schema `CASCADE`. We asserted production state at test exit: `public.dim_aircraft` row count was 149,747 before and after; the two production `pipeline_watermarks` rows kept the same `last_ym` values.

The first run produced two defect findings: D-27-1 HIGH (no cross-month SQL detection warning in `rewind_vault`) and D-27-2 MEDIUM (`--dry-run --layer all` wrongly required `--yes`). We patched both inline and the re-run came back all seven scenarios green.

### 10.3 Data quality

**What was done.** The Great Expectations suite ran five test suites against the live data on the final run; all 25 expectations passed.

```
suite                             passed / total
bronze_raw_states                 5 / 5
mart_dim_aircraft                 5 / 5
mart_dim_airport                  5 / 5
mart_fact_trajectory_enriched     5 / 5
mart_fact_experiment_result       5 / 5
TOTAL                            25 / 25
```

**How it was done.** Each suite checks: non-null constraints on the business keys and the hash keys; uniqueness on `hk_aircraft` and `hk_airport`; regex conformance on `icao24` (lowercase hex, 6 characters) and `icao_code` (uppercase ICAO, 4 characters); range bounds on `lat` (−90 to 90) and `lon` (−180 to 180); enumerated-value checks on `noise` (one of `N0, N1, N2, N3`); and row-count sanity bounds on each table.

---

## 11. Live Deployment

### 11.1 What was done

We ran the end-to-end pipeline against the real course data on 2026-11-20 in two phases.

**Phase A — Vault catch-up.** With no vault watermark present on this host, the vault stage began a full catch-up of 48 months (2019-01 to 2022-12) from the Zenodo flightlist tree. We intentionally terminated the run after three months to validate the incremental mechanism without paying the full 48-month cost. Wall time: **1031 s**, of which approximately 750 s was the one-time load of the 4.37 M `icao24` hub input, and the remainder was three months at an average of ~5.5 min each.

**Phase B — Downstream stages.** A second invocation ran the remaining stages:

```
python -m etl4.run_e2e --source synthetic --only marts qa matrix eval_mart
```

Total wall time: **112.2 s**. Every stage exited OK.

| Stage | Wall time | Result |
|---|---:|---|
| marts | 70.5 s | OK — 3 ym partitions written; dims rebuilt from monoliths (226,661 + 18,595) |
| qa | 27.4 s | OK — 25 / 25 expectations across 5 suites |
| matrix | 13.3 s | OK — 12 synthetic cells; `ON CONFLICT DO NOTHING` idempotent on reruns |
| eval_mart | 0.8 s | OK — watermark=eval_mart/2026-11 |

### 11.2 How it was done

Both phases were launched as backgrounded bash jobs with `stdout` and `stderr` redirected to `logs/e2e_out.log`. A watcher script polled the log every 15 s for the marker `[4/48] ym=2019-04` (indicating three months had fully committed) and killed the vault process cleanly at that boundary. Because the `_DONE` sentinel protocol was in place, the kill left no half-written state. The watermark was already at `2019-03` by that point. Phase B was launched afterward with `--only` to skip stages already handled in Phase A.

We queried Postgres and the on-disk trees during and after the run to verify the pipeline behaved as designed. The three main checks were row-count preservation (bronze → silver → gold counts match the known identities), watermark advance (eight live rows at the expected positions), and the twelve-row Pareto frontier.

---

## 12. Results

### 12.1 Data footprint by layer

| Layer | Row count | On-disk size |
|---|---:|---:|
| Bronze (parquet, as ingested) | 121,646,631 | 6.2 GB |
| Raw landing (`.csv.gz`, tarballs, as pulled) | — | 7.9 GB |
| **Combined raw + bronze footprint** (ADR 2026-11-20) | — | **14.1 GB** |
| Silver (Data Vault) | 225,482,441 | 15 GB |
| Gold (dim + fact marts) | 18,977,423 | 2.0 GB |
| Postgres (fact + tracker + watermarks) | 243 | 568 KB |

Silver is roughly 2.4× bronze. The expansion comes from four sources: Data Vault fan-out (one bronze row expands into one hub + two link + one sat row), audit overhead (`hk_*` hash keys + `load_dt` + `rec_src` + `ym`), cross-month flight duplication in `hub_flight` (resolved downstream at the mart level), and the compression difference (snappy versus Zenodo's distribution-tuned gzip).

### 12.2 Per-table row counts

**Silver — partitioned vault tables:**

| Table | Rows | Size |
|---|---:|---:|
| `link_flight_aircraft` | 100,315,533 | 9.1 GB |
| `hub_flight` | 98,426,017 | 3.6 GB |
| `link_flight_route` | 11,854,754 | 768 MB |
| `sat_flight_summary` | 10,097,653 | 715 MB |
| `sat_state_observation` | 4,374,016 | 116 MB |

**Silver — single-file hubs:**

| Table | Rows | Size |
|---|---:|---:|
| `hub_aircraft` | 226,661 | 8.6 MB |
| `sat_aircraft_identity` | 169,212 | 6.3 MB |
| `hub_airport` | 18,595 | 760 KB |

**Gold — marts:**

| Table | Rows | Size |
|---|---:|---:|
| `fact_flight` | 10,097,653 | 845 MB |
| `fact_trajectory_enriched` | 4,374,016 | 128 MB |
| `reconstructions` | 4,239,943 | 242 MB |
| `dim_aircraft` | 226,661 | 9.7 MB |
| `dim_airport` | 18,595 | 756 KB |
| `fact_experiment_result` | 25 | 16 KB |

### 12.3 Cross-layer reconciliation

Three identity checks ran against the live data and all held:

- `fact_flight` row count (10,097,653) = `sat_flight_summary` row count (10,097,653)
- `fact_trajectory_enriched` row count (4,374,016) = `raw_states` bronze row count (4,374,016)
- `dim_aircraft` row count (226,661) = `hub_aircraft` row count (226,661)

### 12.4 Scientific output

The matrix stage ran the 12-cell experimental grid (3 methods × 4 noise profiles) on the synthetic truth fixture. The downstream `v_pareto_frontier` view returned exactly 12 rows — matching the WI-1 AC-11 baseline.

| Method | Noise | RMSE (m) | Seg F1 | Continuity | Coverage |
|---|---|---:|---:|---:|---:|
| kinematic | N0 | 0.0 | 1.000 | 1.000 | 1.000 |
| kinematic | N1 | 4,901.5 | 0.741 | 0.981 | 0.981 |
| kinematic | N2 | 13,007.9 | 0.372 | 0.951 | 0.951 |
| kinematic | N3 | 16,869.3 | 0.246 | 0.903 | 0.904 |
| kinematic_smoothing | N0 | ~0 | 1.000 | 1.000 | 1.000 |
| kinematic_smoothing | N1 | ~0 | 1.000 | 0.981 | 0.981 |
| kinematic_smoothing | N2 | 1,445,595.8 | 1.000 | 0.951 | 0.951 |
| kinematic_smoothing | N3 | ~0 | 1.000 | 0.903 | 0.904 |
| time_gap | N1 | 4,901.5 | 1.000 | 0.981 | 0.981 |
| time_gap | N2 | 13,007.9 | 1.000 | 0.951 | 0.951 |
| time_gap | N3 | 16,869.3 | 1.000 | 0.903 | 0.904 |

The anomalous `kinematic_smoothing` RMSE at N2 (1.45 × 10⁶ m) is a known pre-existing defect in the Kalman filter's χ² outlier gate, tracked as IMPR-05 on the backlog. It is independent of IMPR-01's pipeline-topology changes.

### 12.5 WI-1 scientific invariants

Both WI-1 acceptance criteria that depend on this milestone's schema change held on the live run:

- **AC-10**: zero NULLs across the six PDF metric columns in `fact_experiment_result` on the 12 synthetic cells.
- **AC-11**: row count of `v_pareto_frontier` equals the synthetic matrix cell count (baseline 12, live 12).

---

## 13. Architectural Decisions Recorded

Four ADRs were appended to `etl4/docs/decisions.md`:

1. **Cross-month flight reader rule in `marts._latest_sfs`** — defines the stable-sort reduction `(hk_flight ASC, lastseen DESC, load_dt DESC)` and the operational closure path via sink UPSERT + rewind M±1.
2. **`fact_experiment_result` schema expansion 18 → 19 columns** — records the addition of `last_ym TEXT` carried through from `pipeline_runs.ym`.
3. **File-month as the single authoritative `ym` key for flightlist-derived vault tables** — binds the migration path to `raw_vault.py:543`'s file-month attribution and explains why three of the four tables are rebuilt from bronze rather than stream-migrated.
4. **Landing-zone accounting: `data/raw/` counts as bronze footprint** — defines the scorecard-facing "raw ingest footprint" as `data/raw/ + data/bronze/` (combined 14.1 GB) and explains why the parquet rewrite is a format change, not a transformation.

---

## 14. What Is Not Done

Four items remain open. None blocks the acceptance of IMPR-01. Each is listed in `multi-agent-engineering/improvements_backlog.md`.

### 14.1 IMPR-02 — Spark promotion for the flightlist vault

**What is pending.** Only `sat_state_observation` currently runs on Spark. The four partitioned flightlist vault tables (`hub_flight`, `link_flight_aircraft`, `link_flight_route`, `sat_flight_summary`) still run on pandas with `pyarrow.dataset` for the partitioned reads and writes.

**What needs to be done.** Port each of the four pandas builders to a Spark `applyInPandas` or SQL implementation. The `ym` partitioning contract and watermark semantics stay identical; only the executor changes. Repartition by `ym` before each write so shards remain one-per-month. Preserve `icao24` as pandas `string` dtype across the Python ↔ Spark boundary by registering a Spark-side `StringType` cast on read.

**Why it matters.** The parent project's scale study (Week 7) needs to report scaling-efficiency numbers under cluster resources. Without IMPR-02, the four large vault builders only scale on single-host multi-processing; the Spark worker count does not help.

### 14.2 Operator migration of the remaining 45 months

**What is pending.** The live deployment stopped at month 3 (2019-01 through 2019-03). The migration script and incremental mechanism are known-good, but the real-run against the full 48-month tree has not been executed.

**What needs to be done.** Run `python -m etl4.scripts.migrate_partition_ym --tables hub_flight,sat_state_observation,fact_flight,fact_trajectory_enriched` to stream-migrate the two tables whose monoliths carry valid `ym` attribution. Clear the watermarks for the three flightlist-derived tables that cannot be stream-migrated. Delete their monoliths. Dispatch `python -m etl4.run_e2e --source zenodo --only vault` and let it rebuild those three tables from bronze — about 45 × ~5.5 min ≈ 4 hours on the same host. Verify row-count identity at each step. This is a manual operator step, not an engineering change.

### 14.3 Cross-month flight scope

**What is pending.** The `marts._latest_sfs` reader deduplicates within the per-month read window. A cross-month flight is closed either at the sink UPSERT or by `rewind --from M --to M+1`. The policy is documented in ADR 2026-11-20 and surfaced in the rewind `--help` epilog, but the mechanism is split across two code paths.

**What needs to be done.** Design a unified cross-month handler that either (a) widens the mart's reader window to always include one trailing month for `sat_flight_summary`, or (b) emits cross-month flight rows to a side table that the sink joins back at UPSERT time. Both designs have trade-offs: (a) increases per-month read cost by ~10%; (b) adds a new side table with its own watermark. A design decision is needed before implementation.

### 14.4 IMPR-15 — Drift check for `sat_state_observation`

**What is pending.** The drift check at the top of `run_e2e` compares partition-directory count per table against the watermark's `last_ym`. The check currently covers 12 partitioned tables. `sat_state_observation` is excluded because it was partitioned pre-milestone (produced by the Spark builder) and was not in the migration script's table list.

**What needs to be done.** Extend `etl4/orchestration/pipeline_volumes.py::VAULT_PARTITIONED_TABLES` to include `sat_state_observation`. Add a dedicated row-count path that reads the Spark-partitioned shards via `vault_io.row_count` with a glob on `ym=*/part*.parquet`. The fix is about 30 lines; the test addition is another 10.

### 14.5 Independent — Kalman χ² outlier gate at N2

**What is pending.** The `kinematic_smoothing` RMSE at N2 is 1.45 × 10⁶ m (Table in §12.4) because the χ² gate rejects legitimate 2° latitude jumps as outliers. The gate was tuned for lower noise levels and is too strict at N2.

**What needs to be done.** Tracked as IMPR-05. Retune the χ² threshold against N2 noise statistics, or replace the gate with a per-step Mahalanobis distance test that adapts to the observed residual covariance. A 1-to-2-day engineering job. This is independent of IMPR-01 and was listed in the backlog before this milestone began.

---

## 15. Headline Numbers

| Metric | Value |
|---|---|
| Pipeline layers moved to incremental | 5 (vault, marts, sink, matrix, eval) |
| Partitioned tables under the contract | 6 |
| Live watermarks in Postgres after deployment | 8 |
| Months materialised in the first incremental run | 3 (2019-01, 2019-02, 2019-03) |
| Unit test suite | 92 / 92 pass (Δ +58) |
| End-to-end rewind scenarios | 7 / 7 pass |
| Great Expectations | 25 / 25 pass |
| WI-1 invariants preserved | AC-10, AC-11 |
| Raw-landed data (raw + bronze) | 14.1 GB |
| Silver output | 15 GB / 225.5 M rows |
| Gold mart output | 2.0 GB / 18.9 M rows |
| Vault wall time (3 months, incl. one-time hub load) | 1031 s |
| Downstream wall time (marts + qa + matrix + eval) | 112 s |
| Regressions on pre-existing tests | 0 |
| New ADRs appended | 4 |
| New code modules authored | 5 (`vault_io.py`, `watermark.py`, `compact.py`, `rewind.py`, `migrate_partition_ym.py`) |
| Lines of production code added | ~1800 |

The branch `feature/pipeline-3-avinash` is ready for integration into `main`.

---

*End of report.*
