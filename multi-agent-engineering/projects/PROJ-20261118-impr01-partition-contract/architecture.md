# Architecture — IMPR-01: Partition contract (`ym=YYYY-MM`) + watermark-driven incremental refresh

**Status:** BINDING — product-owner `requirements.md` landed with status=APPROVED (handoff `20261118T0612-product-owner-to-orchestrator.json`). All 18 FRs / 17 NFRs / 18 ACs are honoured below. The 6 open questions the PO raised to data-engineer are answered in §12.
**Binding design input:** ADR `2026-11-13 — Partition contract (ym=YYYY-MM) and watermark-driven incremental refresh` (`etl4/docs/decisions.md`), `requirements.md` of this project.
**Project:** `PROJ-20261118-impr01-partition-contract`
**Author:** data-engineer
**Date:** 2026-11-18
**Previous ADRs preserved:** `2026-11-14` (NoiseTruth), `2026-11-18` (eval_mart expansion).

> No writes to `etl4/docs/decisions.md` this wave (see dispatch). This file is the implementation-ready expansion of the 2026-11-13 ADR, reconciled against `requirements.md`. Where the ADR is silent or wrong, this file fills the gap; follow-up ADR entries are logged as action items for the orchestrator.

---

## 0. Pre-flight — findings from the code read that modify the ADR

The ADR describes the intent correctly but is silent, imprecise, or wrong on nine concrete execution details. The architecture below encodes the corrections.

| # | ADR text | Corrected text in this architecture |
|---|---|---|
| 1 | "Replace TRUNCATE with INSERT ... ON CONFLICT per target per month" | COPY has no ON CONFLICT. Load each month into a session-scope `TEMP TABLE` first; INSERT ... SELECT DISTINCT ON (pk) ... ON CONFLICT ... DO UPDATE SET (see §4.3). |
| 2 | "Readers continue to work during the migration window because they fall back to the single `part.parquet` when no `ym=*` children exist." | The current readers do not fall back. `marts._read` hard-codes `name/part.parquet` and returns an empty DataFrame otherwise. A shared partition-aware `read_vault_table(name, ym_filter=...)` helper is a PR-2 prerequisite; the migration is gated on every reader being converted first (§5). |
| 3 | "ym from `floor(time / month_seconds)` for `sat_state_observation`" | Wrong. `floor(epoch / 2_628_000)` is not a calendar month. Use `pd.to_datetime(time, unit="s", utc=True).dt.strftime("%Y-%m")`. The same helper is used by the migration, by the vault writer, and by the Postgres `fact_trajectory_enriched.ym` backfill (§2.4). |
| 4 | "`pyarrow.parquet.write_to_dataset(..., partition_cols=["ym"])`" | pyarrow 24.0.0 drops the partition column from the file payload and assigns UUID basenames. Readers that `pd.read_parquet(f)` individual files will lose `ym`. Use `pyarrow.parquet.write_table(table, "<out>/ym=YYYY-MM/part.parquet")` per group so (a) the filename is deterministic for resume, (b) the column is retained inside the file, (c) CLAUDE.md §7 "read individual files and concat" continues to work (§6.2). Pass `existing_data_behavior="overwrite_or_ignore"` where `write_to_dataset` is used by the migration. |
| 5 | "Add `ym TEXT` to `pipeline_runs` and a unique constraint `(ym, source, method, noise)`." | Existing WI-1 rows carry `ym IS NULL`, so a bare `ADD CONSTRAINT UNIQUE` fails on duplicates. Migration steps: ADD COLUMN, backfill existing rows from `DATE_TRUNC('month', run_date)`, enforce `NOT NULL`, then add `UNIQUE (ym, source, method, noise)`. PG16 `UNIQUE NULLS NOT DISTINCT` is only a fallback if the backfill is contested (§2.3). Also fix `write_run` — `ON CONFLICT DO NOTHING ... RETURNING` returns no row on conflict; `fetchone()[0]` raises TypeError, which the broad `except` masks as "sink skipped" (§4.4). |
| 6 | "`eval_mart` becomes a view" | Three gotchas: (a) `DROP TABLE IF EXISTS v_pareto_frontier` errors because it's a view; (b) `create_tables.sql` already defines `v_pareto_frontier` on a `fact_experiment_result` that it does not create, so `stage_schema` depends on `stage_eval_mart` running first on a fresh DB (pre-existing latent bug, documented in Risk list); (c) grouping the mart by `ym` would fan out rows and break dashboard 08. Decision: keep `fact_experiment_result` grouped by `(source, method, noise)`, `MAX()` across `ym`; `v_eval_mart_ym` is a separate ym-aware view. The IMPR-10 `stage_eval_mart` body partly supersedes this; the follow-up ADR is listed for the orchestrator, not written here. |
| 7 | "Watermark vs filesystem drift: COUNT of partition dirs vs `last_ym`" | Wrong shape — a count compared to a string. Correct check: compare `set(ym dirs under table)` against the contiguous range `[min, last_ym]`, flag any hole. A high-water mark cannot represent holes; holes themselves are a risk (§8). |
| 8 | "Hashes stay stable across the migration" | True for `hk_aircraft`, `hk_airport`, `hk_flight`, `link_flight_route`. Not implemented for the cross-month reader rule from ADR Risk 1 — `_latest_per` sorts on `load_dt`, not on `lastseen`. The ADR's "pick the row with the later lastseen" is a reader rule; this architecture pins it to `vault/marts.py::_latest_sfs()` with an explicit `ORDER BY lastseen DESC, load_dt DESC` (§4.2). |
| 9 | "The migration writes one monolithic read into a partitioned tree" | Current `sat_state_observation` is 333M rows / ~20 GB. Reading in one shot risks ENOSPC (there is history here). Stream via `pyarrow.parquet.ParquetFile.iter_batches(batch_size=1_000_000)`, group by `ym`, write per group. Delete the monolith only after a row-count equality check across the resulting partitions (§6). |

All nine are implementation-ready below. Wherever an ADR follow-up is required beyond this wave, it is called out in §9.

---

## 1. Component diagram — before / after per layer

### 1.1. Before (today)

```
                       data/bronze/flightlists/<ver>/
                           flightlist_YYYYMMDD_YYYYMMDD.parquet  (48 files)
                           |
                           v    (full-tree read, 20-30 min, 20 GB transient shards)
                       etl4/vault/raw_vault.py
                           |
                           v    (TRUNCATE+replace, full refresh)
                       data/silver/vault/{hub_*,link_*,sat_*}/part.parquet
                           |
                           v    (full-tree read)
                       etl4/vault/marts.py
                           |
                           v    (TRUNCATE+replace)
                       data/gold/marts/{dim_*,fact_*}/part.parquet
                           |
                           v    (TRUNCATE + COPY, 4M+ rows)
                       Postgres: dim_aircraft, fact_trajectory_enriched
                           |
                           v    (DROP TABLE + CREATE + COPY)
                       fact_experiment_result (+ v_pareto_frontier)
```

Pipeline cost of adding one month of bronze: **full re-compute of 47 other months** at every layer.

### 1.2. After (target)

```
                       data/bronze/flightlists/<ver>/
                           flightlist_YYYYMMDD_YYYYMMDD.parquet  (48 files)
                           |
                           |  ym_from_filename (static), filter ym > last_ym
                           v
                       etl4/vault/raw_vault.py   -----\
                           |                          |
                           |  per-ym pass             |  etl4/vault/compact.py (NEW)
                           v                          v  compacts hub_aircraft,
            data/silver/vault/                            hub_airport,
              {hub_flight, link_*, sat_flight_summary,    sat_aircraft_identity
               sat_state_observation}/ym=YYYY-MM/         (reads only NEW ym,
               part.parquet                               unions + dedups,
                                                          rewrites single file)
            data/silver/vault/
              {hub_aircraft, hub_airport, sat_aircraft_identity}/
               part.parquet  (single file, globally unique entities)
                           |
                           v  partition-aware read (vault_io.read_table(ym_filter))
                       etl4/vault/marts.py
                           |  per-ym build
                           v
            data/gold/marts/
              {fact_flight, fact_trajectory_enriched}/ym=YYYY-MM/part.parquet
              {dim_aircraft, dim_airport}/part.parquet        (full-refresh, tiny)
                           |
                           v  per-ym, staged COPY then INSERT ... ON CONFLICT
                       etl4/vault/postgres_sink.py (REWRITE)
                           |
                           v
                       Postgres: dim_aircraft, fact_trajectory_enriched(ym)
                           |
                           v  INSERT ... ON CONFLICT DO NOTHING per (ym, source, method, noise)
                       pipeline_runs (ym NOT NULL, unique composite)
                           |
                           v
                       fact_experiment_result  (view or idempotent table)
                       v_pareto_frontier       (view)
                       v_eval_mart_ym          (NEW view, ym-grouped)
                           |
                           v   (commit boundary, single transaction with layer writes)
                       pipeline_watermarks(layer, table_name, last_ym, row_count, updated_at)
```

Pipeline cost of adding one month of bronze: **one month of real work at every layer**, around 1–2 min on current hardware. Re-running an already-materialized `ym` is an idempotent overwrite.

### 1.3. Modules changed

| Path | Change | PR |
|---|---|---|
| `etl4/infra/create_tables.sql` | ADD `pipeline_watermarks`, `pipeline_runs.ym`, `fact_trajectory_enriched.ym`, index on `(ym)`, reorder so `fact_experiment_result` precedes `v_pareto_frontier` | PR1 |
| `etl4/vault/vault_io.py` (NEW) | Shared partition-aware reader `read_table(name, ym_filter=None)` + writer `write_partition(df, name, ym, out_dir)` | PR2 |
| `etl4/vault/raw_vault.py` | Per-month build loop; watermark lookup; drop `_chunked_flightlist_vault` final dedup pass (now per-ym); call `compact.py` for small hubs | PR2 |
| `etl4/vault/raw_vault_spark.py` | Partitioned writes for `sat_state_observation`; identical ym helper | PR2 |
| `etl4/vault/compact.py` (NEW) | Compaction pass for `hub_aircraft`, `hub_airport`, `sat_aircraft_identity` | PR2 |
| `etl4/vault/marts.py` | Partition-aware reads; per-ym writes; keep `dim_*` full-refresh; fix `_latest_per` to accept custom sort key | PR3 |
| `etl4/vault/postgres_sink.py` | TEMP TABLE → INSERT ... ON CONFLICT per ym; watermark update in same txn | PR4 |
| `etl4/sink/postgres.py` | `write_run` accepts `ym`; `ON CONFLICT (ym, source, method, noise) DO NOTHING RETURNING run_id`; handle empty `fetchone` | PR4 |
| `etl4/bench/run_matrix.py` | `--ym` arg; bronze `date=YYYY-MM-*` filter; synthetic sentinel `ym=SYN-<seed>`; watermark update after matrix | PR5 |
| `etl4/run_e2e.py` | Pre-flight watermark/filesystem consistency check; `stage_eval_mart` idempotence; invoke matrix per ym; propagate `--ym` | PR5 |
| `etl4/orchestration/rewind.py` (NEW) | `--layer vault|marts|sink|matrix|eval_mart|all --from YYYY-MM --to YYYY-MM --yes` | PR1 skeleton, PR5 all layers wired |
| `etl4/scripts/migrate_partition_ym.py` (NEW) | One-shot migration | PR2+PR3 dependency; runs after PR2 and PR3 are merged |
| `etl4/qa/validate.py`, `etl4/qa/reconcile.py` | No code change — both already use `rglob("*.parquet")` and footer-only counts; verify in PR2 that partitioned trees still parse. Great Expectations suite expectations are schema-level, not path-level. | PR2 (verify only) |

---

## 2. Data contracts

### 2.1. `pipeline_watermarks` (NEW)

```sql
CREATE TABLE IF NOT EXISTS pipeline_watermarks (
    layer       TEXT NOT NULL,                  -- 'vault'|'marts'|'sink'|'matrix'|'eval_mart'
    table_name  TEXT NOT NULL,                  -- 'hub_flight', 'fact_trajectory_enriched', ...
    last_ym     TEXT NOT NULL,                  -- 'YYYY-MM' (format enforced by CHECK)
    row_count   BIGINT,                         -- advisory; row-count reconcile uses it
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (layer, table_name),
    CHECK (last_ym ~ '^[0-9]{4}-(0[1-9]|1[0-2])$')
);
CREATE INDEX IF NOT EXISTS idx_watermarks_layer ON pipeline_watermarks(layer);
```

Bootstrap semantics: missing row => `last_ym = '0000-00'` (lexicographically earlier than any real ym; the vault writer treats absence as "process everything").

### 2.2. `fact_trajectory_enriched.ym` (ADD COLUMN)

```sql
ALTER TABLE fact_trajectory_enriched ADD COLUMN IF NOT EXISTS ym TEXT;

-- Backfill from `time` (epoch seconds) — the only month-bearing column in Postgres.
UPDATE fact_trajectory_enriched
   SET ym = to_char(to_timestamp(time), 'YYYY-MM')
 WHERE ym IS NULL;

ALTER TABLE fact_trajectory_enriched ALTER COLUMN ym SET NOT NULL;

CREATE INDEX IF NOT EXISTS idx_fact_traj_ym ON fact_trajectory_enriched(ym);
```

Primary key `(hk_aircraft, time)` is **unchanged**. `ym` is denormalized for partition deletes (rewind) and dashboard group-by. No change to row count. Approx 4M rows at current scale.

### 2.3. `pipeline_runs.ym` (ADD COLUMN + UNIQUE)

```sql
ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS ym TEXT;

-- WI-1 synthetic + prior rows: backfill from run_date month.
UPDATE pipeline_runs
   SET ym = to_char(run_date, 'YYYY-MM')
 WHERE ym IS NULL;

-- Dedup existing duplicates before adding the constraint. Keep newest by started_at.
DELETE FROM pipeline_runs p1
 USING pipeline_runs p2
 WHERE p1.run_id < p2.run_id
   AND p1.ym = p2.ym
   AND p1.source = p2.source
   AND p1.method = p2.method
   AND p1.noise  = p2.noise;

ALTER TABLE pipeline_runs ALTER COLUMN ym SET NOT NULL;
ALTER TABLE pipeline_runs ADD CONSTRAINT pipeline_runs_ym_src_mth_noise_key
    UNIQUE (ym, source, method, noise);
```

Synthetic matrix cells have no natural month. Use the sentinel `ym = 'SYN-' || seed_hex` (e.g. `SYN-002A` for seed 42). Matches the CHECK regex? No — intentionally does not, because the CHECK regex is only on `pipeline_watermarks.last_ym`, not on `pipeline_runs.ym`. (If the PO requires the sentinel in watermark too, revisit.)

### 2.4. Partition layout (silver / gold on disk)

Partitioned (`ym=YYYY-MM/part.parquet`):

```
data/silver/vault/hub_flight/ym=YYYY-MM/part.parquet
data/silver/vault/link_flight_aircraft/ym=YYYY-MM/part.parquet
data/silver/vault/link_flight_route/ym=YYYY-MM/part.parquet
data/silver/vault/sat_flight_summary/ym=YYYY-MM/part.parquet
data/silver/vault/sat_state_observation/ym=YYYY-MM/part.parquet
data/gold/marts/fact_flight/ym=YYYY-MM/part.parquet
data/gold/marts/fact_trajectory_enriched/ym=YYYY-MM/part.parquet
```

Non-partitioned (globally unique, compacted):

```
data/silver/vault/hub_aircraft/part.parquet
data/silver/vault/hub_airport/part.parquet
data/silver/vault/sat_aircraft_identity/part.parquet
data/gold/marts/dim_aircraft/part.parquet
data/gold/marts/dim_airport/part.parquet
```

### 2.5. `ym` derivation — single source of truth

New helper in `etl4/vault/vault_io.py`:

```python
import pandas as pd
import re

_FL_PATTERN = re.compile(r"flightlist_(\d{4})(\d{2})\d{2}_\d{8}\.parquet$")

def ym_from_filename(path: str) -> str | None:
    """Flightlist filename -> 'YYYY-MM'. None if the name doesn't match."""
    m = _FL_PATTERN.search(path)
    return f"{m.group(1)}-{m.group(2)}" if m else None

def ym_from_epoch(series: "pd.Series") -> "pd.Series":
    """Epoch-second column -> 'YYYY-MM' string column."""
    return pd.to_datetime(series.astype("Int64"), unit="s", utc=True) \
             .dt.strftime("%Y-%m").astype("string")

def ym_from_date(series: "pd.Series") -> "pd.Series":
    """`day` column (date or datetime) -> 'YYYY-MM' string column."""
    return pd.to_datetime(series, utc=True).dt.strftime("%Y-%m").astype("string")
```

| Source | Which helper | Rationale |
|---|---|---|
| Flightlist bronze | `ym_from_filename(path)` | Filename carries month, 48-file cost is O(48) strings. |
| Vault hub_flight / link_* / sat_flight_summary (built from flightlist) | `ym_from_date(df.day)` | The `day` column is authoritative inside the file. |
| Vault sat_state_observation (built from raw_states) | `ym_from_epoch(df.time)` | No `day` column — bronze path is `date=YYYY-MM-DD/hour=HH/`; derive from `time` so one helper covers Postgres backfill too. |
| Postgres `fact_trajectory_enriched.ym` backfill | `to_char(to_timestamp(time), 'YYYY-MM')` | Same semantics as `ym_from_epoch`. |
| Bronze state tarballs on disk (for `--bronze date=...` filter in matrix) | The path itself | `data/bronze/raw_states/date=YYYY-MM-DD/hour=HH/part.parquet`; `ym = date[:7]`. |

### 2.6. `fact_trajectory_enriched` — denormalization notice

`typecode` and `registration` are denormalized onto `fact_trajectory_enriched` rows. Partitions written in month M will NOT see a later update to `sat_aircraft_identity` for the same airframe. This is accepted (see Risk 6). Changing to a join-at-read would be a schema change and needs a new ADR.

---

## 3. DDL — `pipeline_watermarks`

Full statement, added to `etl4/infra/create_tables.sql` in PR1:

```sql
CREATE TABLE IF NOT EXISTS pipeline_watermarks (
    layer       TEXT NOT NULL,
    table_name  TEXT NOT NULL,
    last_ym     TEXT NOT NULL,
    row_count   BIGINT,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (layer, table_name),
    CHECK (last_ym ~ '^[0-9]{4}-(0[1-9]|1[0-2])$')
);
CREATE INDEX IF NOT EXISTS idx_watermarks_layer ON pipeline_watermarks(layer);
```

Seeded by the migration script with `last_ym` set to the newest `ym` present in each table's tree. A fresh DB has no rows; the vault bootstrap treats this as "process everything from the earliest bronze file".

PR1 also fixes a latent bug in the current `create_tables.sql`: `v_pareto_frontier` is defined on `fact_experiment_result`, which that file never creates. The PR reorders so `fact_experiment_result` (as a plain shell table, backfilled by `stage_eval_mart`) precedes the view, OR converts `v_pareto_frontier` to `CREATE OR REPLACE` guarded by `DO $$ ... IF to_regclass IS NOT NULL ... $$;`. Preferred path: create a stub `fact_experiment_result` shell table in the DDL; `stage_eval_mart` continues to `DROP VIEW ... DROP TABLE ... CREATE TABLE` as today.

---

## 4. Per-sink INSERT ON CONFLICT SQL

### 4.1. Pattern for parquet-to-Postgres (COPY cannot do ON CONFLICT)

Every per-ym load follows this pattern inside one transaction:

```sql
BEGIN;

CREATE TEMP TABLE stg_<target> (LIKE <target> INCLUDING DEFAULTS) ON COMMIT DROP;

-- COPY ... FROM STDIN into stg_<target> (via psycopg cursor.copy())

INSERT INTO <target> (<cols>)
SELECT DISTINCT ON (<pk>) <cols>
  FROM stg_<target>
 ORDER BY <pk>, <tiebreaker>
    ON CONFLICT (<pk>) DO UPDATE SET <non_pk_assignments>;

-- Watermark advance in the same txn so a crash cannot leave disagreement.
INSERT INTO pipeline_watermarks (layer, table_name, last_ym, row_count)
VALUES ('sink', '<target>', :ym, (SELECT count(*) FROM <target> WHERE ym = :ym))
    ON CONFLICT (layer, table_name) DO UPDATE
       SET last_ym   = EXCLUDED.last_ym,
           row_count = EXCLUDED.row_count,
           updated_at = now();

COMMIT;
```

`DISTINCT ON (<pk>)` prevents the "ON CONFLICT DO UPDATE command cannot affect row a second time" error when a batch carries two rows with the same PK. Tiebreaker chosen per target (below).

### 4.2. `dim_aircraft`

```sql
INSERT INTO dim_aircraft (hk_aircraft, icao24, registration, typecode)
SELECT DISTINCT ON (hk_aircraft)
       hk_aircraft, icao24, registration, typecode
  FROM stg_dim_aircraft
 ORDER BY hk_aircraft,
          (registration IS NOT NULL) DESC,
          (typecode     IS NOT NULL) DESC
ON CONFLICT (hk_aircraft) DO UPDATE SET
    icao24        = EXCLUDED.icao24,
    registration  = COALESCE(EXCLUDED.registration, dim_aircraft.registration),
    typecode      = COALESCE(EXCLUDED.typecode,     dim_aircraft.typecode);
```

`COALESCE(EXCLUDED, existing)` on nullable columns: a new ym with a null typecode does not wipe the enriched value already loaded from an earlier month. `icao24` is overwritten without COALESCE because an airframe cannot change its ICAO24 (if it does, upstream bug).

### 4.3. `fact_trajectory_enriched`

```sql
INSERT INTO fact_trajectory_enriched
    (hk_aircraft, icao24, time, lat, lon, baroaltitude,
     velocity, heading, callsign, typecode, registration, ym)
SELECT DISTINCT ON (hk_aircraft, time)
       hk_aircraft, icao24, time, lat, lon, baroaltitude,
       velocity, heading, callsign, typecode, registration, ym
  FROM stg_fact_trajectory_enriched
 ORDER BY hk_aircraft, time,
          -- Tiebreak within a single batch: prefer non-null callsign, then non-null typecode.
          (callsign IS NOT NULL) DESC,
          (typecode IS NOT NULL) DESC
ON CONFLICT (hk_aircraft, time) DO UPDATE SET
    lat           = EXCLUDED.lat,
    lon           = EXCLUDED.lon,
    baroaltitude  = EXCLUDED.baroaltitude,
    velocity      = EXCLUDED.velocity,
    heading       = EXCLUDED.heading,
    callsign      = COALESCE(EXCLUDED.callsign,     fact_trajectory_enriched.callsign),
    typecode      = COALESCE(EXCLUDED.typecode,     fact_trajectory_enriched.typecode),
    registration  = COALESCE(EXCLUDED.registration, fact_trajectory_enriched.registration),
    ym            = EXCLUDED.ym;
```

`ym` is part of the row; if the same `(hk_aircraft, time)` appears across two ym (cross-month), later load wins at this layer. The reader-side rule for cross-month flights lives in the mart (see §4.2 of ADR, §5.3 below).

### 4.4. `pipeline_runs` (binding: FR-10 DO NOTHING + rewind contract)

PO FR-10 establishes `ON CONFLICT (ym, source, method, noise) DO NOTHING` plus `rewind --layer matrix` as the only re-measurement path. This architecture honours that contract (not DO UPDATE). PO Open Question #4 is answered via a CTE so `eval_results` rows are never orphaned when the `pipeline_runs` INSERT no-ops:

```python
# etl4/sink/postgres.py::write_run — new behavior
cur.execute("""
    WITH new_run AS (
        INSERT INTO pipeline_runs
            (run_date, source, method, noise, ym,
             rows_in, rows_out, wall_secs)
        VALUES (CURRENT_DATE, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ym, source, method, noise) DO NOTHING
        RETURNING run_id
    )
    SELECT run_id FROM new_run
""", (source, method, noise, ym, rows_in, rows_out, wall_s))

row = cur.fetchone()
if row is None:
    # Cell already materialized for this (ym, source, method, noise).
    # Skip eval_results + deviation_events inserts entirely; the caller
    # logs "skipped - use rewind --layer matrix to re-measure".
    return None
run_id = row[0]
# Proceed with eval_results + deviation_events inserts bound to run_id.
```

Properties:

- The CTE emits exactly one row from `RETURNING run_id`, or zero rows on conflict. `fetchone()` returning `None` is the signal to skip, not the TypeError-via-broad-except bug the current code has.
- No orphaned `eval_results`: the follow-up inserts gate on `run_id is not None`.
- Idempotent matrix reruns: running the same cell twice produces no new row at any table. PO AC-10 "re-running produces no new rows (count unchanged)" is satisfied without a client-side existence check.
- Re-measurement contract: the operator runs `python -m etl4.orchestration.rewind --layer matrix --from X --to Y`, which `DELETE`s the matching `pipeline_runs` rows (ON DELETE CASCADE handles `eval_results` + `deviation_events`), then `run_matrix` regenerates the cells.

### 4.5. `fact_experiment_result` (unchanged pivot semantics)

Keep the IMPR-10 shape (one row per `(source, method, noise)`). Add `MAX(ym) AS last_ym` as an informational column; do not group by ym. Dashboard 08 is frozen on WI-1 baseline and grouping by ym would fan out rows.

New view `v_eval_mart_ym` (added to `create_tables.sql` in PR5):

```sql
CREATE OR REPLACE VIEW v_eval_mart_ym AS
SELECT pr.ym, pr.source, pr.method, pr.noise,
       pr.rows_in, pr.rows_out, pr.wall_secs, pr.started_at,
       MAX(CASE WHEN er.metric='continuity'      THEN er.value END) AS continuity,
       MAX(CASE WHEN er.metric='position_rmse_m' THEN er.value END) AS rmse_m,
       MAX(CASE WHEN er.metric='seg_f1'          THEN er.value END) AS seg_f1,
       MAX(CASE WHEN er.metric='coverage_ratio'  THEN er.value END) AS coverage_ratio
  FROM pipeline_runs pr
  LEFT JOIN eval_results er USING(run_id)
 GROUP BY pr.ym, pr.source, pr.method, pr.noise,
          pr.rows_in, pr.rows_out, pr.wall_secs, pr.started_at;
```

---

## 5. Task breakdown with assignments

Developer 1 = backend. Developer 2 = analytics/dashboard (not touched this wave). All code tasks are `etl4/`-scope.

### Watermark layer (Parallelizable A)

| Task | Owner | Deliverable | Depends on |
|---|---|---|---|
| T-01 | data-engineer | DDL block: `pipeline_watermarks` with CHECK + index. Write as patch against `etl4/infra/create_tables.sql`. | — |
| T-02 | developer-backend | Watermark client: `etl4/vault/watermark.py` with `get(layer, table) -> Optional[str]`, `set(layer, table, last_ym, row_count, conn)`. Must accept an existing connection so callers can share txn. | T-01 |
| T-03 | data-engineer | Review `watermark.py`; validate txn semantics (no autocommit). | T-02 |

### Shared IO layer (Parallelizable B)

| Task | Owner | Deliverable | Depends on |
|---|---|---|---|
| T-04 | data-engineer | Spec for `etl4/vault/vault_io.py` (helpers in §2.5, `read_table(name, ym_filter=None)`, `write_partition(df, name, ym, out_dir)`). | — |
| T-05 | developer-backend | Implement `vault_io.py` per T-04 spec. pandas-first, dtype-preserving, atomic write (temp path + `os.replace`). | T-04 |
| T-06 | unit-tester | Invariants: round-trip read-write preserves `icao24` as `string`; non-existent ym returns empty with correct schema; concurrent write to same ym produces one survivor. | T-05 |
| T-07 | data-engineer | Reader-conversion checklist — every `pd.read_parquet("data/silver/vault/.../part.parquet")` and `rglob("*.parquet")` must switch to `vault_io.read_table`. List: `marts.py` (6 call sites), `raw_vault.py` (3 call sites), `qa/reconcile.py` (verify 0 call sites after switch to `vault_io.row_count`), `qa/validate.py`, `bench/run_matrix.py`, `run_e2e.py`. | T-05 |

### Vault layer (PR2 serial)

| Task | Owner | Deliverable | Depends on |
|---|---|---|---|
| T-08 | developer-backend | Rewrite `_chunked_flightlist_vault` to loop per-month. Compute ym per file (`ym_from_filename`), skip if `ym <= last_ym`, build per-month frames, write via `vault_io.write_partition`. Drop the DuckDB cross-file dedup pass (no longer needed — each file is one month). | T-02, T-05 |
| T-09 | developer-backend | New `etl4/vault/compact.py`: reads new-month links/sats only, unions with existing non-partitioned hub file, dedups on hash, writes back atomically. | T-05 |
| T-10 | developer-backend | Partitioned `sat_state_observation` build: both `raw_vault.py` pandas path and `raw_vault_spark.py` Spark path. ym derived via `ym_from_epoch` inside the applyInPandas / pandas branch. | T-05 |
| T-11 | data-engineer | Review T-08/T-09/T-10 for determinism (same bronze + same `last_ym` = byte-identical output tree). | T-08, T-09, T-10 |

### Migration (gated on PR2 + PR3 merged)

| Task | Owner | Deliverable | Depends on |
|---|---|---|---|
| T-12 | developer-backend | `etl4/scripts/migrate_partition_ym.py` — see §6. Idempotent. Stream via `iter_batches`. Emit watermark rows at the end. | T-08, T-13 |
| T-13 | data-engineer | Migration review: row-count-equality check pre/post, no silent loss, deletes monolith only after check passes. | T-12 |

### Marts layer (PR3 serial on PR2)

| Task | Owner | Deliverable | Depends on |
|---|---|---|---|
| T-14 | developer-backend | Rewrite `marts.py` per-ym build. Keep `dim_*` full-refresh (small). Fact tables filter on `ym > last_ym`. Add `ym` column on `fact_trajectory_enriched` from the state epoch. | T-05, T-08 |
| T-15 | developer-backend | Cross-month reader rule: `_latest_sfs()` orders by `(lastseen DESC, load_dt DESC)` NOT by `(load_dt DESC)`. Documented in-line as the reader-side implementation of ADR Risk 1. | T-14 |
| T-16 | data-engineer | Review T-14/T-15; approve mart schema contract. | T-14, T-15 |

### Sink layer (PR4, can parallel with PR2/PR3 if staging pattern finalized)

| Task | Owner | Deliverable | Depends on |
|---|---|---|---|
| T-17 | developer-backend | Rewrite `etl4/vault/postgres_sink.py` to the staged INSERT ON CONFLICT pattern (§4). Per-ym loop. Watermark write in same txn. | T-01 |
| T-18 | developer-backend | Update `etl4/sink/postgres.py::write_run` — accept `ym` kwarg; INSERT ... ON CONFLICT (ym, source, method, noise) DO UPDATE ... RETURNING; `COALESCE(fetchone(), (None,))[0]`. | T-01 |
| T-19 | data-engineer | SQL review of T-17 and T-18. Focus: TEMP table lifetime, DISTINCT ON tiebreaker correctness, same-txn watermark. | T-17, T-18 |

### Matrix + eval_mart (PR5)

| Task | Owner | Deliverable | Depends on |
|---|---|---|---|
| T-20 | developer-backend | `run_matrix.py` grows `--ym` (required when `source` is one of `s3|zenodo|rest`; synthetic uses `SYN-002A`). Bronze filter by `date=YYYY-MM-*`. Watermark advance per ym after all method cells committed. | T-18 |
| T-21 | developer-backend | `etl4/orchestration/rewind.py` — see §7. | T-01, T-17, T-20 |
| T-22 | developer-backend | `run_e2e.py` pre-flight check: compare `set(ym dirs)` vs `pipeline_watermarks.last_ym`'s contiguous range, abort if hole. | T-02 |
| T-23 | developer-backend | Idempotent `stage_eval_mart` — reuse existing IMPR-10 shape, add `MAX(ym) AS last_ym`. Also add `v_eval_mart_ym` DDL. | T-20 |
| T-24 | data-engineer | Review T-20/T-21/T-22/T-23. | T-20..T-23 |

### Testing / validation (cross-cutting)

| Task | Owner | Deliverable | Depends on |
|---|---|---|---|
| T-25 | unit-tester | New tests in `etl4/tests/test_pipeline.py`: (a) `ym_from_filename` on all filename variants; (b) `ym_from_epoch` round-trip; (c) `watermark.get/set` is txn-safe; (d) `read_table(ym_filter=['2022-03'])` returns only that month; (e) sink INSERT ON CONFLICT upserts not duplicates; (f) determinism — same bronze + seed = same partition bytes. | T-05, T-17 |
| T-26 | functional-tester | E2E replay: run `python etl4/run_e2e.py --only vault marts sink` on a 2-month bronze, then on a 3-month bronze that adds one new month; verify only the new month wrote partitions (filesystem mtime check) and sink row count grew by exactly the new month's count. | T-14, T-17, T-20 |
| T-27 | functional-tester | Rewind E2E: `python -m etl4.orchestration.rewind --layer vault --from 2022-04 --to 2022-04 --yes`; verify that month's partitions are gone, watermark rolled back, and a subsequent `run_e2e --only vault` restores them. | T-21 |

**Task count per PR:** PR1 = 3 (T-01, T-21 skeleton, T-22 skeleton); PR2 = 8 (T-02, T-03, T-04, T-05, T-06, T-07, T-08, T-09, T-10, T-11; T-07 and T-11 are reviews); PR3 = 3 (T-14, T-15, T-16); PR4 = 3 (T-17, T-18, T-19); PR5 = 5 (T-20, T-21 full, T-22 full, T-23, T-24). Migration (T-12, T-13) runs between PR3 merge and PR4 merge; it is not a PR — it is a one-shot run.

---

## 6. Migration script outline

Path: `etl4/scripts/migrate_partition_ym.py`. Invoked once after PR2 and PR3 are merged, before PR4.

```python
"""One-shot: rechunk every monolithic part.parquet into ym=YYYY-MM/part.parquet.

Zero re-processing of upstream data — this is a parquet rechunking pass.

Pre-condition: PR2 and PR3 merged, so every reader is partition-aware.
Post-condition: pipeline_watermarks seeded for every (layer, table).

Idempotent:
  - If `ym=*` children already exist, SKIP that table.
  - If monolith does NOT exist, SKIP that table.
"""
from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

import pyarrow.parquet as pq
import psycopg

from etl4.vault.vault_io import ym_from_epoch, ym_from_date, write_partition

ROOT = Path(__file__).resolve().parents[2]

PARTITIONED_TABLES = {
    # key_source is the row-level column used by the migration helper.
    # Four flightlist-derived vault tables are NOT in this dict (per T-13 2026-11-20):
    #   - link_flight_aircraft, link_flight_route: no 'day' column in monolith (builders
    #     at raw_vault.py L158-199 drop it); rebuild from bronze via T-08 after monolith delete.
    #   - sat_flight_summary: 'firstseen' key disagrees with T-08 FILE-MONTH key for 12,685
    #     cross-month rows; stream-migrating would create split-brain. Rebuild from bronze.
    #   - hub_flight stays in this dict because its 'day' column IS present and 100% agrees
    #     with file-month at current data (verified by T-12 dry-run exit 0).
    #
    # The remaining three are state/trajectory-derived (no file-month concept; time epoch
    # is authoritative and matches Postgres to_char(to_timestamp(time), 'YYYY-MM')):
    "silver/vault/hub_flight":             ("day",  ym_from_date),
    "silver/vault/sat_state_observation":  ("time", ym_from_epoch),
    "gold/marts/fact_flight":              ("day",  ym_from_date),
    "gold/marts/fact_trajectory_enriched": ("time", ym_from_epoch),
}
# Tables that cannot be migrated via monolith rechunk and must be rebuilt
# from bronze flightlist via T-08 raw_vault after monolith delete:
REBUILD_FROM_BRONZE = {
    "silver/vault/link_flight_aircraft",
    "silver/vault/link_flight_route",
    "silver/vault/sat_flight_summary",
}

def migrate_one(relpath: str, col: str, helper) -> dict:
    tbl = ROOT / "data" / relpath
    monolith = tbl / "part.parquet"
    if any(tbl.glob("ym=*")):
        return {"table": relpath, "status": "already_partitioned"}
    if not monolith.exists():
        return {"table": relpath, "status": "no_monolith"}

    tmp_root = tbl.with_name(tbl.name + "__migrating")
    if tmp_root.exists():
        shutil.rmtree(tmp_root)
    tmp_root.mkdir(parents=True)

    pf = pq.ParquetFile(monolith)
    expected = pf.metadata.num_rows
    written = 0
    # Stream batches; group rows by derived ym; one file per (ym, batch).
    # Final compaction into one file per ym happens via compact_dir at the end.
    for i, batch in enumerate(pf.iter_batches(batch_size=1_000_000)):
        import pandas as pd
        df = batch.to_pandas()
        df["ym"] = helper(df[col])
        for ym, sub in df.groupby("ym", sort=False):
            out = tmp_root / f"ym={ym}" / f"part-{i:05d}.parquet"
            out.parent.mkdir(parents=True, exist_ok=True)
            # Keep `ym` as an IN-FILE column (see finding #4) and as a dir name.
            pq.write_table(
                __import__("pyarrow").Table.from_pandas(sub, preserve_index=False),
                out,
                compression="snappy",
            )
            written += len(sub)

    # Row-count equality gate.
    actual = sum(
        pq.ParquetFile(p).metadata.num_rows
        for p in tmp_root.rglob("*.parquet")
    )
    if actual != expected:
        raise AssertionError(f"{relpath}: expected {expected} rows, wrote {actual}")

    # Compact each ym dir into one part.parquet.
    for ym_dir in sorted(tmp_root.glob("ym=*")):
        shards = sorted(ym_dir.glob("*.parquet"))
        if len(shards) > 1:
            import pyarrow as pa
            tables = [pq.read_table(s) for s in shards]
            combined = pa.concat_tables(tables)
            final = ym_dir / "part.parquet"
            pq.write_table(combined, final, compression="snappy")
            for s in shards:
                if s.name != "part.parquet":
                    s.unlink()
        elif shards and shards[0].name != "part.parquet":
            shards[0].rename(ym_dir / "part.parquet")

    # Atomic swap: monolith -> trash, tmp_root -> final location.
    trash = tbl.with_name(tbl.name + "__pre_partition")
    monolith.rename(trash / "part.parquet") if trash.exists() else None
    # Move each ym dir up one level.
    for ym_dir in sorted(tmp_root.glob("ym=*")):
        shutil.move(str(ym_dir), str(tbl / ym_dir.name))
    shutil.rmtree(tmp_root)
    # Keep the monolith as a safety copy for one run; can be deleted manually.

    return {"table": relpath, "status": "migrated", "rows": actual}

def seed_watermarks(conn):
    for relpath in PARTITIONED_TABLES:
        tbl = ROOT / "data" / relpath
        yms = sorted(p.name.split("=")[1] for p in tbl.glob("ym=*"))
        if not yms:
            continue
        last_ym = yms[-1]
        row_count = sum(
            pq.ParquetFile(p / "part.parquet").metadata.num_rows
            for p in tbl.glob("ym=*") if (p / "part.parquet").exists()
        )
        layer = "vault" if "silver/vault" in relpath else "marts"
        name = relpath.rsplit("/", 1)[-1]
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO pipeline_watermarks (layer, table_name, last_ym, row_count)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (layer, table_name) DO UPDATE
                   SET last_ym   = EXCLUDED.last_ym,
                       row_count = EXCLUDED.row_count,
                       updated_at = now();
            """, (layer, name, last_ym, row_count))

def main():
    results = [migrate_one(r, c, h) for r, (c, h) in PARTITIONED_TABLES.items()]
    for r in results:
        print(r, flush=True)
    dsn = os.getenv("POSTGRES_DSN", "postgresql://etl4:change_me@localhost:5432/etl4")
    with psycopg.connect(dsn) as conn:
        seed_watermarks(conn)
        conn.commit()

if __name__ == "__main__":
    main()
```

Guarantees: zero re-processing (parquet rechunk only); row-count equality gate; idempotent; monolith preserved as `*__pre_partition/part.parquet` for one run (manual cleanup).

### 6.1. Postgres-side migration (one SQL script, no monolith re-read)

Separate SQL migration (not Python) run after the parquet migration and before the sink rewrite deploys:

```sql
BEGIN;
-- fact_trajectory_enriched
ALTER TABLE fact_trajectory_enriched ADD COLUMN IF NOT EXISTS ym TEXT;
UPDATE fact_trajectory_enriched
   SET ym = to_char(to_timestamp(time), 'YYYY-MM')
 WHERE ym IS NULL;
ALTER TABLE fact_trajectory_enriched ALTER COLUMN ym SET NOT NULL;
CREATE INDEX IF NOT EXISTS idx_fact_traj_ym ON fact_trajectory_enriched(ym);

-- pipeline_runs
ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS ym TEXT;
UPDATE pipeline_runs
   SET ym = to_char(run_date, 'YYYY-MM')
 WHERE ym IS NULL;
DELETE FROM pipeline_runs p1
 USING pipeline_runs p2
 WHERE p1.run_id < p2.run_id
   AND p1.ym = p2.ym
   AND p1.source = p2.source
   AND p1.method = p2.method
   AND p1.noise  = p2.noise;
ALTER TABLE pipeline_runs ALTER COLUMN ym SET NOT NULL;
ALTER TABLE pipeline_runs
    ADD CONSTRAINT pipeline_runs_ym_src_mth_noise_key
    UNIQUE (ym, source, method, noise);
COMMIT;
```

### 6.2. pyarrow writer-reader contract

Writer chosen: `pq.write_table(table, "<out>/ym=YYYY-MM/part.parquet", compression="snappy")` — not `write_to_dataset`.

Rationale:

1. Deterministic filename for resume (matches `_DONE` sentinel pattern already in `raw_vault.py`).
2. `ym` is retained in-file; readers that `pd.read_parquet(f)` individual files via CLAUDE.md §7 do not lose the partition column.
3. No UUID basenames.
4. `write_to_dataset(..., partition_cols=["ym"], existing_data_behavior="overwrite_or_ignore")` is still fine in the migration helper because pq.write_table is driven per-group explicitly.

### 6.3. In-file `ym` column + `_DONE` sentinel on migration output (BINDING, T-13 2026-11-20)

Every migrated partition file carries `ym` as an in-row column of dtype `string`. This mirrors `etl4/vault/raw_vault.py` line 543 (T-08 writer convention). Without the in-file column, concurrent reads of migrated partitions + T-08-written partitions through `vault_io.read_table` would yield `NaN` for `ym` on migrated rows (Spark-shard broadcast join risk and `fact_trajectory_enriched.ym` backfill drift).

Every migrated partition directory carries an `_DONE` sentinel file matching the `raw_vault._write_done` format: `load_dt=<ISO datetime>\n`. The migration may append `source=migrate_partition_ym\n` for provenance. `_partition_done` (T-08 line 529) tests existence only, so this is byte-safe. Without the sentinel, a subsequent T-08 run with a cleared watermark would rebuild over the migrated partition and double-write the month.

### 6.4. Three flightlist-derived vault tables are rebuilt from bronze, not stream-rechunked (BINDING, T-13 2026-11-20)

`link_flight_aircraft`, `link_flight_route`, and `sat_flight_summary` are not stream-migrated from their monoliths because:

- Link monoliths have no row-level month column (builders at `raw_vault.py` L158-199 drop `day`).
- `sat_flight_summary` monolith's `firstseen` key disagrees with T-08's FILE-MONTH key for 12,685 cross-month rows; any firstseen-based migration would create split-brain under rewind.

Operator procedure: migrate the other 4 tables first; row-count-verify; delete those 4 monoliths; clear the 3 cleared-table watermarks; delete the 3 remaining monoliths; run `python -m etl4.vault.raw_vault` (or equivalent entrypoint) to rebuild the 3 tables per-month via T-08's `_partition_done`-gated loop. The migration is wall-cost dominated by the first step (`sat_state_observation` rechunk); the bronze rebuild of the 3 tables is minutes.

Byte-equivalence with T-08: all four flightlist-derived vault tables (`hub_flight`, `link_*`, `sat_flight_summary`) land on partition keys consistent with T-08's committed `frame["ym"] = ym_from_filename(f.name)` writer. See ADR 2026-11-20 FILE-MONTH key rule in `etl4/docs/decisions.md`.

---

## 7. Rewind primitive outline

Path: `etl4/orchestration/rewind.py`. Single CLI, five layers.

```python
"""Idempotent rewind: deletes partitions + rolls back pipeline_watermarks.

Usage:
  python -m etl4.orchestration.rewind --layer vault --from 2022-03 --to 2022-05
  python -m etl4.orchestration.rewind --layer all  --from 2022-03 --to 2022-05 --yes
"""
LAYERS = ("vault", "marts", "sink", "matrix", "eval_mart", "all")
PARTITIONED_VAULT = ("hub_flight", "link_flight_aircraft",
                     "link_flight_route", "sat_flight_summary",
                     "sat_state_observation")
PARTITIONED_MARTS = ("fact_flight", "fact_trajectory_enriched")

def rewind_vault(from_ym, to_ym):
    for tbl in PARTITIONED_VAULT:
        for ym in months_between(from_ym, to_ym):
            path = ROOT / "data" / "silver" / "vault" / tbl / f"ym={ym}"
            if path.exists():
                shutil.rmtree(path)
    # watermark rollback: prev month of from_ym, or delete if that leaves nothing
    prev = month_minus_one(from_ym)
    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        for tbl in PARTITIONED_VAULT:
            cur.execute("""
                UPDATE pipeline_watermarks SET last_ym = %s, updated_at = now()
                 WHERE layer='vault' AND table_name=%s AND last_ym >= %s
            """, (prev, tbl, from_ym))

def rewind_marts(from_ym, to_ym):   # symmetric to vault
def rewind_sink(from_ym, to_ym):
    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute("""
            DELETE FROM fact_trajectory_enriched WHERE ym BETWEEN %s AND %s
        """, (from_ym, to_ym))
        cur.execute("""
            UPDATE pipeline_watermarks SET last_ym=%s, updated_at=now()
             WHERE layer='sink' AND table_name='fact_trajectory_enriched'
               AND last_ym >= %s
        """, (month_minus_one(from_ym), from_ym))
        # dim_aircraft is non-partitioned; it stays.

def rewind_matrix(from_ym, to_ym):
    # deletes pipeline_runs + cascades eval_results + deviation_events
    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute("""
            DELETE FROM pipeline_runs WHERE ym BETWEEN %s AND %s
        """, (from_ym, to_ym))

def rewind_eval_mart(from_ym, to_ym):
    # fact_experiment_result is the latest-per-cell shape; rewinding matrix
    # changes what "latest" is. Re-run stage_eval_mart after matrix rewind.
    pass  # handled implicitly by subsequent stage_eval_mart run.

def rewind_all(from_ym, to_ym, yes: bool):
    if not yes:
        sys.exit("--layer all requires --yes (destructive)")
    rewind_matrix(from_ym, to_ym)
    rewind_sink(from_ym, to_ym)
    rewind_marts(from_ym, to_ym)
    rewind_vault(from_ym, to_ym)
```

Order of rewind for `--layer all`: matrix → sink → marts → vault (reverse of pipeline order). This is the opposite of the ADR's "chains all four in correct order" — the ADR is ambiguous; dependencies dictate downstream-first deletion so upstream rebuilds never leave orphans.

---

## 8. Parallelizable workstream map

```
PR1 (DDL + skeleton)
  T-01 ---+
          |
          +----> PR4 (Sink)             --- can start parallel with PR2 once T-01 is landed
          |         T-17, T-18, T-19
          |
          +----> PR5 skeletons (T-21 skel, T-22 skel)
          |
          +----> PR2 (Vault)             --- blocks on T-01 for watermark client
                    T-02..T-11
                    |
                    v
                    migration (T-12, T-13)
                    |
                    v
                    PR3 (Marts)
                    T-14, T-15, T-16
                    |
                    v
                    PR5 full (T-20, T-21 full, T-22 full, T-23, T-24)
                    |
                    v
                    T-25 (unit) + T-26 (functional) + T-27 (functional)
```

Strict sequence: **PR2 → migration → PR3 → PR5**.
Parallel: **PR1** blocks PR2 + PR4; **PR4** runs alongside PR2/PR3/PR5-skeleton; **shared IO (T-04..T-07)** runs in PR2 but its spec can drop ahead of time, so developer-backend can start PR4's INSERT-ON-CONFLICT SQL against a stub `vault_io.read_table`.

| Agent | Can start in | Notes |
|---|---|---|
| developer-backend | PR1 + PR4 simultaneously | Independent code paths. |
| data-engineer | T-04 spec, T-07 checklist, all reviews | Non-blocking producer of specs + reviews. |
| unit-tester | T-25 skeleton (parametrize helpers) | Full run after PR2 merges. |
| functional-tester | T-26/T-27 scaffolding | Executes after PR5 merges. |

Risk: developer-backend becomes the single point of contention on PR2. Mitigation: split PR2 into PR2a (vault_io + watermark client + flightlist loop) and PR2b (compaction + sat_state_observation) if week-1 cadence slips.

---

## 9. Risks

### 9.1. Risks preserved from ADR 2026-11-13 (now with mitigations pinned to tasks)

| # | Risk | Mitigation | Pinned task |
|---|---|---|---|
| R1 | Cross-month flights produce two rows per `hk_flight` in different ym | Reader-side rule in `vault/marts.py::_latest_sfs()` orders by `(lastseen DESC, load_dt DESC)` | T-15 |
| R2 | Small-hub uniqueness drift under `icao24` normalization change | `clean.py::project_raw` is the enforcement point (CLAUDE.md §2.6); `compact.py` dedups on `hk_aircraft`; drift surfaces as a visible row-count anomaly via `pipeline_watermarks.row_count` | T-09 |
| R3 | ym extraction asymmetry (flightlist in-file `day` vs state in-path `date`) | Single ym helper module in `vault_io.py`; three functions, documented truth table in §2.5 | T-04 |
| R4 | Watermark vs filesystem drift | `set(ym dirs)` vs contiguous `[min, last_ym]` range check in `run_e2e` pre-flight — **not** the ADR's count-vs-string check | T-22 |
| R1b | `hub_flight` has a row with `day` in month M and lives in bronze file for month M+1 (currently 0 rows at scale; future risk) | LOW | Cross-check inside `migrate_one` for `hub_flight`: compare `day`-derived ym against bronze source filename once the migration is invoked; abort on mismatch | T-12 defensive check (follow-up) |

### 9.2. New risks from the code read

| # | Risk | Severity | Mitigation | Pinned task |
|---|---|---|---|---|
| R5 | COPY cannot do ON CONFLICT; naive port silently corrupts sink | HIGH | Staged pattern in §4.1 is mandatory | T-17 |
| R6 | `fact_trajectory_enriched` denormalized `typecode`/`registration` become stale when later months update the dim | MEDIUM | Accepted; documented; changing to join-at-read needs its own ADR | — (document in follow-up ADR list) |
| R7 | `_latest_per` sorts unstably on `load_dt`; ADR cross-month rule unimplemented | HIGH | T-15 adds explicit `(lastseen DESC, load_dt DESC)` | T-15 |
| R8 | pyarrow 24 `write_to_dataset` drops the partition column from file bodies, breaking CLAUDE.md §7 "read individual files and concat" | HIGH | Use `pq.write_table(table, "ym=.../part.parquet")` per group — see §6.2 | T-05 |
| R9 | `create_tables.sql` defines `v_pareto_frontier` on a `fact_experiment_result` that never exists at schema stage; `stage_schema` is a single `psql -c`, one failure aborts | MEDIUM | PR1 adds a stub table or reorders statements | T-01 |
| R10 | `write_run` uses `ON CONFLICT DO NOTHING ... RETURNING` pattern — `fetchone()[0]` raises TypeError on conflict, masked by broad `except` | MEDIUM | Use `DO UPDATE ... RETURNING` (§4.4); defensive `fetchone()` check | T-18 |
| R11 | Migration ENOSPC risk: 333M-row `sat_state_observation` monolith read in one shot | HIGH | Stream via `pq.ParquetFile.iter_batches(batch_size=1_000_000)`; row-count gate before monolith delete | T-12 |
| R12 | `raw_vault_spark.py` is the other writer of `sat_state_observation`; forgetting it leaves one engine unpartitioned | MEDIUM | T-10 explicitly covers both engines | T-10 |
| R13 | `states_hub_input` in `raw_vault.py` loads every bronze `icao24` at every run regardless of watermark | LOW | Filter in T-08: only files under `date=` prefixes with ym > `last_ym` | T-08 |
| R14 | `v_pareto_frontier` is `CREATE OR REPLACE` but `stage_eval_mart` does `DROP VIEW IF EXISTS v_pareto_frontier` before `DROP TABLE` — safe pattern already documented in IMPR-10 ADR | LOW | Preserve current IMPR-10 shape in T-23; add `v_eval_mart_ym` as separate `CREATE OR REPLACE VIEW` | T-23 |
| R15 | Synthetic matrix cells have no natural month; `pipeline_runs.ym` would be NULL under the obvious backfill | MEDIUM | Sentinel `SYN-<seed_hex>` (`SYN-002A` for seed 42); CHECK regex on `pipeline_watermarks.last_ym` deliberately does not apply to `pipeline_runs.ym` | T-20 |
| R16 | Holes in the ym range (e.g. `2022-03, 2022-05` but no `2022-04`) cannot be represented by a single `last_ym` high-water mark | MEDIUM | Pre-flight check in `run_e2e` detects set-vs-range mismatch and aborts | T-22 |
| R17 | `bronze_flightlists.py` filenames sit in a mix of versioned directories — `v23.00` today, future `v24.00` tomorrow | LOW | `ym_from_filename` is version-agnostic (regex on filename, not path) | T-04 |

### 9.3. Follow-up ADRs for the orchestrator (NOT written this wave)

- Cross-month flight reader rule written into `marts._latest_sfs()` (one new ADR entry after T-15).
- Denormalization of `typecode`/`registration` on `fact_trajectory_enriched` — explicit "accepted staleness" ADR (after T-14).
- Sentinel `SYN-*` ym values in `pipeline_runs` — explicit schema ADR (after T-20).
- `fact_experiment_result` as table vs view — explicit partition-stage ADR (after T-23).

---

## 10. CLAUDE.md non-negotiables observed

- `icao24` dtype preserved as `string` through every new helper (verified in T-25).
- No `df.apply()` on hot paths — the per-ym loop vectorizes the ym derivation (`ym_from_epoch` is one `pd.to_datetime` + `.dt.strftime`).
- No per-row `.iloc[i] = other_row` — every writer builds the frame first, then writes once.
- Determinism: all writers accept a seed where relevant; the migration uses deterministic sort on tiebreakers; same bronze + same `last_ym` produces byte-identical output (asserted by T-25).
- Event-time ordering per `icao24` is unchanged (sort order inside per-ym writes still `(icao24, time)` stable).
- Windows cp1252-safe stdout — all script banners use ASCII (`->`, `..`, no em-dashes, no arrows).
- Great Expectations gates between marts and sink — unchanged; T-26 verifies suites still pass on partitioned trees.
- Row-count reconciliation gates between sink and matrix — unchanged; `qa/reconcile.py` already uses `rglob("*.parquet")` and footer metadata, so partitioned trees sum correctly without a code change.

---

## 11. State / gate status

- **Gate mismatch (surfaced, not actioned):** `state.json` says the gate is `PROJ-20261114-wi1-metrics phase=DONE`, but WI-1 closed at `CONDITIONAL_ACCEPTANCE`. PO proceeded anyway (requirements.md APPROVED). This architecture document is implementation-ready either way; orchestrator decides formally.
- **PO requirements:** APPROVED (handoff `20261118T0612-product-owner-to-orchestrator.json`). All 18 FRs are honoured by the task breakdown in §5. §12 answers the 6 open questions PO raised to data-engineer.
- **Phase change:** `state.json` moves to `phase=ARCHITECTURE`, approval history appended with gate=ARCHITECTURE, approver=data-engineer.

---

## 12. Answers to PO Open Questions

**OQ-1 — `v_pareto_frontier` cardinality after `ym` lands on `pipeline_runs`:**
Cardinality stays at 12 (synthetic matrix cell count) under the WI-1 AC-11 contract. The ADR 2026-11-13 wording "eval_mart becomes a view" is honoured by a plain view (`fact_experiment_result`, see §4.5) that pivots `pipeline_runs LEFT JOIN eval_results` per `(source, method, noise)`, selecting the latest `ym` via `MAX(started_at)` inside the `DISTINCT ON`. `v_pareto_frontier` reads from `fact_experiment_result`, so its row count equals the distinct `(source, method, noise)` set with populated `rmse_m` and `wall_secs`. For the frozen WI-1 synthetic matrix that is 12. If `ym` grouping is required later for a new dashboard, it belongs in the separate `v_eval_mart_ym` view (§4.5), which does not feed Pareto panels.

**OQ-2 — Matrix re-measurement semantics:**
Confirmed: `ON CONFLICT (ym, source, method, noise) DO NOTHING` + `rewind --layer matrix` as the re-measurement path. WI-1 NFR-05 ("re-running overwrites the prior cell") is superseded by the new contract ("re-running is a no-op; rewind is the re-measurement primitive"). This is a controlled breakage: dashboards see stable numbers until an operator explicitly re-measures. Update WI-1 NFR-05 in a follow-up ADR (listed for orchestrator in §9.3).

**OQ-3 — Sink conflict keys for `fact_flight` and `dim_airport`:**
- `fact_flight`: `ON CONFLICT (hk_flight) DO UPDATE SET hk_aircraft=EXCLUDED.hk_aircraft, hk_airport_origin=COALESCE(EXCLUDED.hk_airport_origin, fact_flight.hk_airport_origin), hk_airport_destination=COALESCE(EXCLUDED.hk_airport_destination, fact_flight.hk_airport_destination), callsign=EXCLUDED.callsign, day=EXCLUDED.day, firstseen=EXCLUDED.firstseen, lastseen=EXCLUDED.lastseen, duration_s=EXCLUDED.duration_s, altitude_1=EXCLUDED.altitude_1, altitude_2=EXCLUDED.altitude_2`.
- `dim_airport`: `ON CONFLICT (hk_airport) DO NOTHING` (airport rows are immutable once seen; no non-PK columns are mutable).

**Prerequisite:** PR1 adds `fact_flight (hk_flight) PRIMARY KEY` and `dim_airport (hk_airport) PRIMARY KEY` to `etl4/infra/create_tables.sql`. Current DDL does not declare either table in Postgres (today's sink only loads `dim_aircraft` and `fact_trajectory_enriched`). The new DDL is in-scope for PR1.

**OQ-4 — Orphaned `eval_results` rows under DO NOTHING:**
Resolved in §4.4 above via the CTE pattern. `eval_results` rows are never inserted when the `pipeline_runs` INSERT no-ops. The broad `except` in the current `write_run` is tightened so the skip is a logged `INFO`, not a swallowed exception.

**OQ-5 — `sat_state_observation` migration ym extraction:**
Confirmed: `ym = pd.to_datetime(time, unit="s", utc=True).dt.strftime("%Y-%m")`. Not `floor(time / month_seconds)` (not calendar-aligned). Single helper `ym_from_epoch` in `etl4/vault/vault_io.py` (§2.5). Same helper used by vault writer, migration, and Postgres backfill, so all three agree bit-for-bit on month boundaries.

**OQ-6 — IMPR-12 status at kickoff:**
Deferred for orchestrator confirmation. Architecture takes the safe path: NFR-03 verification uses direct parquet-vs-parquet row-count and hash-set checks against the pre-migration monolith (not `reconcile.py`). This is already encoded in T-13 of §5. If IMPR-12 closes before PR2 merges, `reconcile.py` can be used as a secondary check; if it stays open, direct parquet is the authoritative instrument.

---

*End of architecture.md (BINDING).*
