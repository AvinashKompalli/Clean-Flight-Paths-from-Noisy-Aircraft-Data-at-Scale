# vault_io Specification - `etl4/vault/vault_io.py`

**Project:** `PROJ-20261118-impr01-partition-contract`
**Task:** T-04 (shared IO layer spec)
**Author:** data-engineer
**Date:** 2026-11-18
**Status:** BINDING - developer-backend implements `vault_io.py` against this spec in T-05.
**Scope:** Single source of truth for reading / writing silver and gold parquet tables under the partition contract committed by ADR `2026-11-13`.

Normative references:
- `architecture.md` sections 2.4 (partition layout), 2.5 (ym derivation), 2.6 (fact_trajectory_enriched denormalization), 6.2 (pyarrow writer-reader contract)
- `requirements.md` FR-02, FR-03, FR-06, FR-13, FR-14, FR-16, NFR-05, NFR-06, NFR-09, NFR-11
- ADR `2026-11-13 - Partition contract (ym=YYYY-MM) and watermark-driven incremental refresh`
- CLAUDE.md sections 2.6 (`icao24` is `string`), 2.7 (Windows cp1252), 3.4 (pandas nullable dtypes), 7 ("read individual files and concat")

---

## 1. Module purpose

`etl4/vault/vault_io.py` is the single public reader and writer surface over the silver vault (`data/silver/vault/`) and the gold marts (`data/gold/marts/`). It encapsulates three concerns that are today scattered across six call sites across `marts.py`, `raw_vault.py`, `qa/reconcile.py`, `qa/validate.py`, `postgres_sink.py`, and `bench/run_matrix.py`:

1. **Partition-aware reads** of `ym=YYYY-MM/part.parquet` trees, with optional month filter (single ym, list of ym, or `(from, to)` inclusive range), optional column pushdown via pyarrow, and dtype preservation (`icao24` stays `string`).
2. **Reader fallback** to the pre-migration monolithic `<root>/<name>/part.parquet` when no `ym=*` children are present on disk. This honours ADR 2026-11-13 Consequences last paragraph and `requirements.md` FR-14, so the migration window (PR2 merged, migration script not yet run) does not break any reader.
3. **ym derivation** symmetric across the two writer paths - flightlist bronze carries `day` inside the file; raw-states bronze carries `date=YYYY-MM-DD/` in the partition path. One module exports both helpers so `raw_vault.py`, `raw_vault_spark.py`, the migration script, the mart writer, and the Postgres backfill all agree bit-for-bit on month boundaries (NFR-08).

Non-goals for this module: deduplication (caller's responsibility), watermark table I/O (`watermark.py` owns that), Postgres (`postgres_sink.py` owns that), GE suites (`qa/validate.py` owns that). The module is pure-parquet, pure-pandas / pure-pyarrow.

---

## 2. Public API

All signatures are `pandas 2.x` nullable-dtype safe. `pathlib.Path` is used everywhere; strings are accepted and coerced at the boundary. Imports consumers rely on:

```python
from etl4.vault.vault_io import (
    read_table,
    write_partition,
    row_count,
    list_ym,
    ym_from_filename,
    ym_from_date,
    ym_from_epoch,
    extract_ym_from_frame,
    extract_ym_from_path,
    SILVER_VAULT_ROOT,
    MARTS_ROOT,
    PARTITIONED_TABLES,
    NON_PARTITIONED_TABLES,
)
```

### 2.1. Module-level constants

```python
from pathlib import Path

ROOT: Path = Path(__file__).resolve().parents[2]
SILVER_VAULT_ROOT: Path = ROOT / "data" / "silver" / "vault"
MARTS_ROOT:        Path = ROOT / "data" / "gold"   / "marts"

PARTITIONED_TABLES: frozenset[str] = frozenset({
    "hub_flight",
    "link_flight_aircraft",
    "link_flight_route",
    "sat_flight_summary",
    "sat_state_observation",
    "fact_flight",
    "fact_trajectory_enriched",
})

NON_PARTITIONED_TABLES: frozenset[str] = frozenset({
    "hub_aircraft",
    "hub_airport",
    "sat_aircraft_identity",
    "dim_aircraft",
    "dim_airport",
    "fact_experiment_result",
})
```

`PARTITIONED_TABLES` and `NON_PARTITIONED_TABLES` are advisory - the reader is layout-driven (it inspects the directory), not name-driven. The sets exist for `list_ym` guards and developer sanity-checking (`assert name in PARTITIONED_TABLES | NON_PARTITIONED_TABLES`).

### 2.2. `read_table`

```python
def read_table(
    name: str,
    *,
    ym_filter: str | list[str] | tuple[str, str] | None = None,
    columns: list[str] | None = None,
    root: Path = SILVER_VAULT_ROOT,
) -> "pd.DataFrame":
    """Read a vault / mart table; partition-aware with monolithic fallback.

    `ym_filter` accepts:
      - None            -> all partitions (or the monolith under fallback)
      - 'YYYY-MM'       -> exactly one partition
      - list['YYYY-MM'] -> explicit set of partitions
      - (from, to)      -> inclusive calendar-month range, both 'YYYY-MM'

    `columns` is pushed down to pyarrow (`pq.read_table(..., columns=...)`),
    so tables with ~100 M rows can be read at <5% of full-column cost.

    `root` defaults to the silver vault; callers reading from the gold mart
    layer must pass `root=MARTS_ROOT`.

    Returns an empty DataFrame (no columns inferred) when the table dir
    does not exist, is empty, or all `ym_filter`-matched partitions miss.
    `icao24`, when present in the output, is `string` dtype.
    """
```

Internal flow:

1. Resolve `table_dir = root / name`.
2. Discover `ym_dirs = sorted(d for d in table_dir.glob("ym=*") if (d / "part.parquet").is_file())`.
3. If `ym_dirs` is empty and `(table_dir / "part.parquet").is_file()`:
   - **Fallback**: if `ym_filter is None`, read the monolith; else return empty DataFrame (`ym_filter` is unsatisfiable on a monolithic layout).
4. If `ym_dirs` is empty and no monolith: return empty DataFrame.
5. Else filter `ym_dirs` by `ym_filter` (`_select_ym_dirs(ym_dirs, ym_filter)` internal helper).
6. Read each surviving `<ym_dir>/part.parquet` with `pyarrow.parquet.read_table(path, columns=columns)`, convert to pandas with `types_mapper=pd.ArrowDtype` *only when needed to preserve `icao24` as `string`* (actual implementation: use `to_pandas(types_mapper=...)` once and cast `icao24` explicitly after concat to guarantee `string` under every pyarrow version).
7. `pd.concat(frames, ignore_index=True)` - never read a partitioned tree with `pd.read_parquet(tree_path)` (CLAUDE.md section 7).
8. Dtype normalisation pass: `if "icao24" in df.columns: df["icao24"] = df["icao24"].astype("string")`. This defends against pyarrow dataset readers that default to `object`.

Ordering: files are read in `sorted(ym_dirs)` order (lexicographic ym). This gives deterministic row order per ym boundary, which `raw_vault.py` and `marts.py` already assume for the `_latest_per` / `drop_duplicates(keep="first")` pattern. Callers that need event-time order inside a month must still sort explicitly - `read_table` does not sort by `(icao24, time)`; the per-icao24 sort invariant from CLAUDE.md section 3.6 remains the caller's job.

### 2.3. `write_partition`

```python
def write_partition(
    df: "pd.DataFrame",
    name: str,
    ym: str,
    *,
    out_dir: Path = SILVER_VAULT_ROOT,
    compression: str = "snappy",
) -> Path:
    """Write one month's partition atomically as <out_dir>/<name>/ym=YM/part.parquet.

    Write pattern (crash-safe, concurrent-safe within one host):
      1. Build target = out_dir/name/ym=<ym>/part.parquet
      2. Build tmp    = out_dir/name/ym=<ym>/.part.parquet.<pid>.tmp
      3. mkdir -p target.parent
      4. pq.write_table(pa.Table.from_pandas(df, preserve_index=False), tmp,
                        compression=compression)
      5. os.replace(tmp, target)   # atomic on same filesystem (Windows + POSIX)

    Returns the final `target` Path.

    Enforces that `ym` matches the CHECK regex `^[0-9]{4}-(0[1-9]|1[0-2])$`
    OR the sentinel pattern `^SYN-[0-9A-Fa-f]+$` (synthetic matrix cells
    have no natural calendar month per architecture.md section 2.3; the
    sentinel is permitted on the matrix path only but write_partition is
    layer-agnostic so both are accepted).

    Writer uses `pyarrow.parquet.write_table` per group (NOT
    `pq.write_to_dataset(..., partition_cols=['ym'])`) so that:
      - the filename is deterministic for resume (matches _DONE sentinel
        pattern in raw_vault.py),
      - `ym` is retained inside the file body (write_to_dataset drops the
        partition column), so CLAUDE.md section 7 "read individual files and
        concat" keeps working even if a developer bypasses this module.
    """
```

Atomicity choice: `os.replace` is atomic on Windows and POSIX when source and target are on the same filesystem. The temp path shares the final directory for exactly this reason; do not route through a system temp dir. The `.<pid>.tmp` suffix de-collides concurrent workers running the same ym (last-writer-wins, no half-written file ever surfaces as `part.parquet`).

### 2.4. `row_count`

```python
def row_count(
    name: str,
    *,
    ym_filter: str | list[str] | tuple[str, str] | None = None,
    root: Path = SILVER_VAULT_ROOT,
) -> int:
    """Row count of a vault / mart table from parquet footer metadata only.

    O(files), not O(rows). No dataframe allocation. Supersedes the direct
    pyarrow calls in qa/reconcile.py and orchestration/pipeline_volumes.py
    for silver/gold readers (bronze call sites keep their inline footer
    reads - they are not under the partition contract).

    Partition / fallback resolution is identical to read_table:
      - ym=*/part.parquet children present   -> sum over the filtered set
      - monolithic part.parquet (fallback)    -> single-file count
      - nothing on disk                        -> 0
    """
```

Implementation shells out to `pyarrow.parquet.ParquetFile(f).metadata.num_rows`, summed. Does NOT open the full file - metadata is in the footer and parsed lazily.

### 2.5. `list_ym`

```python
def list_ym(name: str, *, root: Path = SILVER_VAULT_ROOT) -> list[str]:
    """Sorted list of existing ym partitions for a table.

    Returns:
      - sorted list of 'YYYY-MM' strings (and 'SYN-*' sentinels if any)
        when the table is partitioned,
      - [] when the table is non-partitioned (monolith present, no ym=*
        children) OR the table dir does not exist.

    Used by `run_e2e.check_watermark_vs_filesystem` (FR-17, NFR-07):
    compare `set(list_ym(name))` against the contiguous range
    [min_ym, pipeline_watermarks.last_ym]; any hole exits non-zero.
    """
```

Caller of FR-17's drift check: compare `set(list_ym(name))` against `_contiguous_months(min(list_ym(name)), watermark.last_ym)`. The drift check is a *set-vs-range* mismatch, NOT the ADR 2026-11-13 original count-vs-string check (which is numerically incompatible). See architecture.md Pre-flight finding #7.

### 2.6. ym derivation helpers (single source of truth)

```python
import re
_FL_PATTERN = re.compile(r"flightlist_(\d{4})(\d{2})\d{2}_\d{8}\.parquet$")
_PATH_DATE_PATTERN = re.compile(r"date=(\d{4})-(\d{2})-\d{2}")
_PATH_YM_PATTERN   = re.compile(r"ym=([0-9]{4}-(?:0[1-9]|1[0-2])|SYN-[0-9A-Fa-f]+)")

def ym_from_filename(path: str | Path) -> str | None:
    """Flightlist filename -> 'YYYY-MM'. None when the name does not match.

    Version-agnostic (regex is on the filename, not the parent dir), so
    bronze trees mixing v23.00 and future v24.00 subtrees both resolve.
    """

def ym_from_epoch(series: "pd.Series") -> "pd.Series":
    """Epoch-second int column -> 'YYYY-MM' string column (nullable).

    Uses UTC. Implementation:
      pd.to_datetime(series.astype('Int64'), unit='s', utc=True) \
        .dt.strftime('%Y-%m').astype('string')

    Vectorised; matches Postgres' `to_char(to_timestamp(time), 'YYYY-MM')`
    for the fact_trajectory_enriched backfill.
    """

def ym_from_date(series: "pd.Series") -> "pd.Series":
    """Date / datetime column (pandas or arrow) -> 'YYYY-MM' string column.

    Implementation:
      pd.to_datetime(series, utc=True).dt.strftime('%Y-%m').astype('string')
    """

def extract_ym_from_frame(df: "pd.DataFrame", source: str) -> "pd.Series":
    """Dispatch to the right helper based on the frame's origin.

    source in {
        'flightlist',             # df['day'] -> ym_from_date
        'sat_flight_summary',     # df['firstseen'] (epoch s) -> ym_from_epoch
        'sat_state_observation',  # df['time'] (epoch s) -> ym_from_epoch
        'fact_trajectory_enriched', # df['time'] (epoch s) -> ym_from_epoch
    }

    Raises ValueError on an unknown source so the mart writer never
    silently derives ym from the wrong column.
    """

def extract_ym_from_path(path: str | Path) -> str:
    """Path -> 'YYYY-MM' from the first matching segment.

    Accepts (in precedence order):
      - 'ym=YYYY-MM'              (already partitioned shards, migrations)
      - 'date=YYYY-MM-DD'         (bronze raw_states layout)
      - 'flightlist_YYYYMMDD_...' (bronze flightlist filename)

    Raises ValueError when no segment matches. Used by run_matrix.py
    (bronze `date=YYYY-MM-*` filter -> ym) and by the migration script
    (per-file source-ym tag).
    """
```

### 2.7. Internal helper (private)

```python
def _select_ym_dirs(ym_dirs: list[Path], ym_filter) -> list[Path]:
    """Filter a sorted list of ym=... Paths by the ym_filter argument.

    None             -> all
    'YYYY-MM'        -> exactly one
    list['YYYY-MM']  -> subset by set membership
    (from_ym, to_ym) -> inclusive lexical range (assumes 'YYYY-MM' format)

    Unknown filter type raises TypeError - callers get a visible failure,
    not a silent full-read. Tuple of length != 2 raises ValueError.
    """
```

---

## 3. Reader fallback contract

The fallback behaviour is specified precisely so no caller has to re-implement it. Three concrete cases:

1. **Monolithic layout only** (pre-migration, or a table that stays non-partitioned by design such as `hub_aircraft`):
   - `read_table("hub_aircraft")` reads `<root>/hub_aircraft/part.parquet`.
   - `read_table("hub_aircraft", ym_filter="2022-07")` returns an empty DataFrame. Rationale: the monolith has no month attribution on disk; synthesising one would require loading the whole file and running `ym_from_date(df["day"])` for every call, which breaks the O(files) contract of `row_count` and the "read only what you need" contract of `ym_filter`. An explicit empty return is the safe default - callers that want monolith content pass `ym_filter=None`.

2. **Partitioned layout only** (post-migration or a freshly-materialised table):
   - `read_table("hub_flight")` reads every `<root>/hub_flight/ym=*/part.parquet`, concats, returns.
   - `read_table("hub_flight", ym_filter="2022-07")` reads only `<root>/hub_flight/ym=2022-07/part.parquet`. Missing month -> empty DataFrame.
   - `read_table("hub_flight", ym_filter=("2022-03", "2022-06"))` reads the four partitions `ym=2022-03..2022-06` inclusive, in sorted order.

3. **Mixed layout** (`ym=*` present AND a stale monolithic `part.parquet` from before the migration script ran):
   - **Partitioned wins**. `read_table` reads only `ym=*/part.parquet`; the stale monolith is ignored. Rationale: during the migration window the monolith is intentionally preserved for rollback, but after the migration script writes the ym shards it is a historical artefact. Reading both would double-count.
   - This is the same semantics as the migration script's own idempotence check (`if any(tbl.glob("ym=*")): status='already_partitioned'`).

4. **Empty layout** (table dir exists but holds no `part.parquet` and no `ym=*`, or table dir does not exist at all):
   - `read_table` returns `pd.DataFrame()` (no columns). Rationale: the current `marts._read` already returns empty-and-no-columns in this case and every caller handles it (`if hub.empty: return pd.DataFrame(columns=[...])`). Raising `FileNotFoundError` would force every caller to wrap in a try/except purely for the bootstrap / fresh-DB case. Callers that want a strict-mode read pass an outer `assert not df.empty` or inspect `list_ym` first.
   - `row_count` returns 0. `list_ym` returns `[]`.

---

## 4. Partition layout reference (quote-through from architecture.md section 2.4)

### 4.1. Partitioned on disk

```
data/silver/vault/hub_flight/ym=YYYY-MM/part.parquet
data/silver/vault/link_flight_aircraft/ym=YYYY-MM/part.parquet
data/silver/vault/link_flight_route/ym=YYYY-MM/part.parquet
data/silver/vault/sat_flight_summary/ym=YYYY-MM/part.parquet
data/silver/vault/sat_state_observation/ym=YYYY-MM/part.parquet
data/gold/marts/fact_flight/ym=YYYY-MM/part.parquet
data/gold/marts/fact_trajectory_enriched/ym=YYYY-MM/part.parquet
```

### 4.2. Non-partitioned (single-file, globally unique)

```
data/silver/vault/hub_aircraft/part.parquet
data/silver/vault/hub_airport/part.parquet
data/silver/vault/sat_aircraft_identity/part.parquet
data/gold/marts/dim_aircraft/part.parquet
data/gold/marts/dim_airport/part.parquet
data/gold/marts/fact_experiment_result/part.parquet   # when materialised; may be view
```

### 4.3. ym extraction truth table (quote-through from architecture.md section 2.5)

| Source | Helper | Rationale |
|---|---|---|
| Flightlist bronze filename | `ym_from_filename(path)` | Filename carries month; 48-file cost is O(48) strings. |
| Vault hub_flight / link_flight_* / sat_flight_summary (built from flightlist) | `extract_ym_from_frame(df, 'flightlist')` i.e. `ym_from_date(df['day'])` | The `day` column is authoritative inside the flightlist frame. |
| Vault sat_state_observation (built from raw_states) | `extract_ym_from_frame(df, 'sat_state_observation')` i.e. `ym_from_epoch(df['time'])` | No `day` column; derive from `time` so one helper covers Postgres backfill too. |
| Postgres `fact_trajectory_enriched.ym` backfill | `to_char(to_timestamp(time), 'YYYY-MM')` SQL | Bit-identical to `ym_from_epoch` under UTC. |
| Bronze state tarballs on disk (for `--bronze date=...` filter in matrix) | `extract_ym_from_path(path)` | Path carries `date=YYYY-MM-DD/` segment. |
| Pre-existing ym shard rediscovery (resume, drift check) | `extract_ym_from_path(path)` matches `ym=YYYY-MM` first | Same helper covers both bronze and silver path shapes. |

---

## 5. Invariants, edge cases, dtype contract

### 5.1. `icao24` is `string` on every read

The reader runs a defensive post-concat cast:

```python
if "icao24" in df.columns:
    df["icao24"] = df["icao24"].astype("string")
```

Reason: `pyarrow.dataset`-backed readers default to `object`, which breaks the hex-value invariant (`0a1b2c` silently parses to `10534700`). The architecture pins the dtype everywhere; this reader enforces it. Unit test: T-25 (e) asserts `.dtype.name == 'string'` on the result of `read_table("hub_aircraft")`.

### 5.2. Mixed layout during migration

During the migration window:
- PR2 + PR3 are merged (every reader is partition-aware via this module), but
- The migration script has not yet run on some tables.

The reader resolves to the monolithic fallback for those tables and the partitioned layout for the migrated ones. No code change is required between those two states. See Reader fallback contract section 3 for the precedence rules.

### 5.3. Empty table

Returns `pd.DataFrame()` with no inferred columns. Callers in `marts.py` (`build_fact_flight`, `build_dim_aircraft`, ...) already handle this by returning a frame with explicit columns:

```python
if hub.empty:
    return pd.DataFrame(columns=[...])
```

Justification for not raising: see Reader fallback contract item 4.

### 5.4. Concurrent write to the same ym

`write_partition` writes to `.part.parquet.<pid>.tmp` in the target directory, then `os.replace`s it. Two workers writing the same ym race at the `os.replace` step; last-writer-wins (atomic). No half-written `part.parquet` ever surfaces. Operationally this does not happen in the matrix (one method per ym per run); it is defence-in-depth against future parallel month-per-worker schemes.

### 5.5. Determinism (NFR-11)

Given a fixed input DataFrame and a fixed compression codec, `pq.write_table` is deterministic. Running `write_partition` twice on the same ym produces byte-identical `part.parquet` files (modulo filesystem mtime). Unit test T-25 (f).

### 5.6. Windows / Posix parity

- `os.replace` is atomic on both.
- Paths are built with `pathlib`, so slashes work either way.
- No non-cp1252 characters in log lines (CLAUDE.md section 2.7).

---

## 6. Non-goals

- **Dedup**: not performed by this module. `raw_vault.py` and the DuckDB finalize step own dedup on hash keys. The reader does not deduplicate across ym (that is the mart's `_latest_sfs()` cross-month rule, T-15).
- **Watermark table I/O**: `pipeline_watermarks` reads/writes live in `etl4/vault/watermark.py` (T-02). `vault_io` has no Postgres awareness.
- **Postgres**: `vault_io` is pure parquet. `postgres_sink.py` continues to own COPY and INSERT ... ON CONFLICT.
- **Bronze layer**: this module is for the silver vault and gold marts only. Bronze readers (`raw_vault.py::_load_parquet_tree`, `bronze_flightlists.py`, `replay_producer.py`, `ingest_runner.py`) are out of scope because bronze is not under the partition contract (bronze already has its own `date=YYYY-MM-DD/hour=HH/` layout and its row counts are source-of-truth; adding a vault_io wrapper would be churn without benefit).
- **GE suites**: `qa/validate.py::_load_parquet_tree` migrates to `vault_io.read_table` for the one silver/gold suite source (dim_airport) but the GE evaluator itself stays untouched.
- **Sort by `(icao24, time)`**: the caller sorts if it needs event-time order. `read_table` concats in sorted ym order; within one ym the file row order is preserved.
- **`sat_state_observation` Spark write path**: that writer lives in `etl4/vault/raw_vault_spark.py` and uses Spark's own partitioned `write.partitionBy("ym").parquet(...)`. The reader side still goes through `vault_io.read_table("sat_state_observation")`, which handles the Spark-emitted filenames transparently (per-ym subdir with one or more `part-*.parquet` shards - `read_table` globs `<ym_dir>/*.parquet` as a documented extension when `part.parquet` is absent).

Spark writer note: when the Spark path writes `ym=.../part-00000-*.parquet` instead of `ym=.../part.parquet`, `read_table` reads every `*.parquet` under that ym dir, not just `part.parquet`. This is a documented reader extension to accommodate the Spark engine's filename conventions. `write_partition` (pandas path) still writes `part.parquet` deterministically for resume.

---

## 7. Dependencies and risks

### Dependencies (no new runtime deps)

- `pandas >= 2.0` (already in `etl4/requirements.txt`)
- `pyarrow` (already in `etl4/requirements.txt`)
- Stdlib: `os`, `pathlib`, `re`, `typing`

No new entries to `etl4/requirements.txt`. No new ADR needed for this module alone (the parent ADR `2026-11-13` already registers the partition contract and the reader-fallback NFR).

### Risks pinned to this module

- **R3 (ym extraction asymmetry)**: resolved by three explicit helpers + the frame-vs-path dispatch. Verified by T-25 (a) and T-25 (b).
- **R8 (pyarrow write_to_dataset drops ym from file body)**: resolved by `write_partition` using `pq.write_table` per group (architecture.md finding #4).
- **R17 (bronze_flightlists versioned subdirs)**: `ym_from_filename` is version-agnostic (regex on filename, not path).

Risks NOT pinned to this module (owned elsewhere):
- R4 (watermark vs filesystem drift) - `list_ym` provides the input; the comparison lives in `run_e2e` (T-22).
- R5 (COPY has no ON CONFLICT) - owned by `postgres_sink.py` (T-17).
- R11 (migration ENOSPC) - owned by the migration script (T-12).

---

## 8. Call-site migration

The authoritative list of reader call sites that must switch to this module is `reader_conversion_checklist.md` (T-07). Every reader in `etl4/vault/marts.py`, `etl4/vault/raw_vault.py` (writer side), `etl4/vault/postgres_sink.py`, `etl4/qa/reconcile.py`, `etl4/qa/validate.py`, `etl4/orchestration/pipeline_volumes.py`, and `etl4/run_e2e.py` (new pre-flight check) must route through `vault_io`. After migration, a grep for `pd.read_parquet(.*silver/vault)` or `pd.read_parquet(.*gold/marts)` outside `vault_io.py` must return zero matches - enforced by a CI check added in PR5.

---

## 9. Acceptance criteria for T-05 (implementation)

T-05 is approved when:

1. `etl4/vault/vault_io.py` exposes exactly the public symbols listed in section 2.0.
2. `read_table` passes the five invariants in T-25:
   (a) round-trip write-then-read preserves `icao24` as `string`;
   (b) non-existent ym returns empty with no columns;
   (c) `ym_filter=('2022-03','2022-05')` returns exactly three partitions (inclusive);
   (d) monolithic fallback returns the full monolith when no `ym=*` present;
   (e) `columns=['icao24','time']` reads only those columns (pyarrow pushdown verified by a profile assertion or a `pq.read_table` call-count mock).
3. `write_partition` passes:
   (a) atomic (`os.replace` observed; no half-file ever visible under `part.parquet`);
   (b) deterministic (same input bytes-identical twice);
   (c) retains `ym` as an in-file column (regression test for finding #4).
4. `row_count` returns the sum of `pq.ParquetFile(f).metadata.num_rows` across the matched partitions, verified against a hand-counted fixture.
5. `list_ym` returns `[]` on an empty table dir and `[]` on a monolith-only table.
6. ym helpers pass T-25 (a) and T-25 (b).
7. `pytest etl4/tests/test_pipeline.py -v` is green.

---

*End of vault_io_spec.md.*
