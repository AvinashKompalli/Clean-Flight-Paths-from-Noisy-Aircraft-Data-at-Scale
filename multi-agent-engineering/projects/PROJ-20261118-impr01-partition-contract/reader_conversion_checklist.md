# Reader-conversion checklist - `vault_io.read_table` / `row_count` migration

**Project:** `PROJ-20261118-impr01-partition-contract`
**Task:** T-07 (reader-conversion checklist)
**Author:** data-engineer
**Date:** 2026-11-18
**Status:** BINDING - every reader in this list switches to `etl4/vault/vault_io.py` in the PR identified below. After PR5 merges, a repo-wide grep for `pd.read_parquet\(.*data/(silver|gold)/` outside `etl4/vault/vault_io.py` must return zero hits.

Normative references:
- `vault_io_spec.md` (T-04) - this file's target API.
- `architecture.md` section 5 (task breakdown PR assignments).
- `requirements.md` FR-02, FR-06, FR-13, FR-14.

Grep used to seed the list (run 2026-11-18 against `feature/pipeline-3-avinash` HEAD `80e63e7`):

```
rg -nE "read_parquet|rglob\(.\*\\.parquet.\)|ParquetFile\(" \
   etl4/vault/marts.py etl4/vault/raw_vault.py etl4/vault/postgres_sink.py \
   etl4/qa/reconcile.py etl4/qa/validate.py \
   etl4/bench/run_matrix.py etl4/run_e2e.py \
   etl4/orchestration/pipeline_volumes.py etl4/orchestration/ingest_runner.py
```

Scope rule: a call site is in scope iff it reads under `data/silver/vault/` or `data/gold/marts/` (i.e., the trees under the partition contract). Bronze readers (`data/bronze/**`), `data/silver/cleaned_states/` (the matrix's noise-prepared silver, NOT the DV vault silver), and `etl4/data/reference/**` are out of scope and stay on `pd.read_parquet` / `rglob`.

---

## 1. `etl4/vault/marts.py` - 9 call sites (reader + writer helpers)

Source-of-truth reader. Every call hits silver/vault; every write lands in gold/marts.

| # | Line(s) | Current call | Target call | Notes |
|---|---|---|---|---|
| 1 | L30 (`_read` helper) | `pd.read_parquet(VAULT_DIR / name / "part.parquet")` | `vault_io.read_table(name, root=SILVER_VAULT_ROOT)` | Replace the whole `_read` helper body with a `return vault_io.read_table(name, root=SILVER_VAULT_ROOT)`. Reader-fallback semantics preserved by `vault_io` (the current behaviour "return empty DataFrame if monolith missing" becomes "return empty DataFrame if neither monolith nor ym=* present"). |
| 2 | L54 | `_read("hub_aircraft")` | `vault_io.read_table("hub_aircraft")` | Non-partitioned - reader falls back to monolith. No ym filter. |
| 3 | L55 | `_read("sat_aircraft_identity")` | `vault_io.read_table("sat_aircraft_identity")` | Non-partitioned. |
| 4 | L64 | `_read("hub_airport")` | `vault_io.read_table("hub_airport")` | Non-partitioned. |
| 5 | L71 | `_read("hub_flight")` | `vault_io.read_table("hub_flight", ym_filter=ym_range)` | Partitioned. When `marts` is invoked per-ym via `--only-ym YYYY-MM` (FR-07), caller passes `ym_filter="YYYY-MM"`; when running full-rebuild caller passes `ym_filter=None`. T-14 wires the ym_range plumbing. |
| 6 | L72 | `_read("link_flight_aircraft")` | `vault_io.read_table("link_flight_aircraft", ym_filter=ym_range)` | Partitioned. Same ym plumbing as hub_flight. |
| 7 | L73 | `_read("link_flight_route")` | `vault_io.read_table("link_flight_route", ym_filter=ym_range)` | Partitioned. Cross-month reader rule (FR-15, T-15) implemented inside `_latest_sfs` after the read, not inside vault_io. |
| 8 | L74 | `_read("sat_flight_summary")` | `vault_io.read_table("sat_flight_summary", ym_filter=ym_range)` | Partitioned. T-15's `(lastseen DESC, load_dt DESC)` sort runs on the result. |
| 9 | L106 | `_read("sat_state_observation")` | `vault_io.read_table("sat_state_observation", ym_filter=ym_range, columns=STATE_COLS)` | Partitioned, 333M rows at current scale. Column pushdown MANDATORY (hits 15-column rows but only ~10 are consumed) to keep memory bounded; `STATE_COLS = ['hk_aircraft','icao24','time','lat','lon','baroaltitude','velocity','heading','callsign']`. |

Writer-side call sites (same file, L34-37 `_write`):

| # | Line(s) | Current call | Target call | Notes |
|---|---|---|---|---|
| W1 | L36 | `df.to_parquet(MARTS_DIR / name / "part.parquet")` for `dim_aircraft` (L128) | `df.to_parquet(MARTS_DIR / "dim_aircraft" / "part.parquet")` (unchanged) | Non-partitioned dim - stays monolithic. |
| W2 | L36 | same for `dim_airport` (L134) | unchanged | Non-partitioned dim. |
| W3 | L36 | same for `fact_flight` (L139) | `vault_io.write_partition(df, "fact_flight", ym, out_dir=MARTS_ROOT)` in a loop over the built ym range | Partitioned - rewrite in T-14. |
| W4 | L36 | same for `fact_trajectory_enriched` (L144) | `vault_io.write_partition(df, "fact_trajectory_enriched", ym, out_dir=MARTS_ROOT)` in a loop | Partitioned. Add `ym` column from `ym_from_epoch(df['time'])` before write (FR-09). |

Architecture.md said "6 call sites" for marts.py; the actual count is 9 reader-side (the 8 `_read` invocations plus the 1 implementation inside `_read`) + 4 `_write` invocations. The architecture's "6" undercounted because it did not include the single pd.read_parquet implementation in `_read` and merged the four `_write` sites. All are listed above for completeness.

---

## 2. `etl4/vault/raw_vault.py` - 8 call sites (writer side + two in-scope reads)

The vault writer. Bronze reads at L42, L46, L348, L458, L483 are out of scope (bronze is not under the partition contract). Only the writer calls and the silver-side verification queries are in scope.

| # | Line(s) | Current call | Target call | Notes |
|---|---|---|---|---|
| 1 | L49-54 (`_write` helper) | `df.to_parquet(VAULT_DIR / name / "part.parquet")` | `vault_io.write_partition(df, name, ym, out_dir=SILVER_VAULT_ROOT)` for partitioned; `df.to_parquet(SILVER_VAULT_ROOT / name / "part.parquet")` for non-partitioned | T-08 / T-09 / T-10 split this helper into partitioned vs non-partitioned paths. Non-partitioned (hub_aircraft, hub_airport, sat_aircraft_identity) keeps the atomic `.tmp + os.replace` pattern - vault_io exports an internal `_atomic_write(df, path)` that both sides share. |
| 2 | L265-269 (DuckDB `read_parquet(shard_glob)`) | `read_parquet('{shard_glob}', filename=true)` SQL | unchanged | DuckDB-internal shard-dedup SQL. Operates on `_shards/` transients, not on the silver vault output. Stays. |
| 3 | L277 (DuckDB `read_parquet(final_out)`) | `read_parquet('{final_out}')` SQL | unchanged | Count-after-finalize SQL. Stays. |
| 4 | L348 | `pd.read_parquet(f)` for flightlist bronze | unchanged | Bronze read - out of scope. |
| 5 | L395-397 (DuckDB resume count) | `read_parquet('{final_path}')` SQL | **replace with** `vault_io.row_count(name, root=SILVER_VAULT_ROOT)` | Only reads the silver-side final `part.parquet` for a resume count. Vault_io.row_count is 10 lines cheaper and uses the same underlying `pq.ParquetFile.metadata.num_rows`. T-08 change. |
| 6 | L39-46 (`_load_parquet_tree`) | `pd.read_parquet(path)` / `pd.concat([pd.read_parquet(f) for f in files])` | unchanged | Bronze read - out of scope. Argument: the only two callers (states_hub_input at L454, sat_state_observation at L479) read bronze. Confirmed by grep. |
| 7 | L452-462 | state-vector bronze `pd.read_parquet(f, columns=['icao24'])` loop | unchanged | Bronze. Out of scope. Leave as-is - this is the deliberate per-file loop from CLAUDE.md section 7. |
| 8 | L479-486 | state-vector bronze `pd.read_parquet(f, columns=state_cols)` loop | unchanged | Bronze. Out of scope. |

Architecture.md said "3 call sites" for raw_vault.py. The 3 it meant are the silver-side writer + the resume count + one of the silver-side reads via `_write`. Counting is imprecise because `_write` is one implementation called 8 times; this checklist lists it once with a scope-split note.

Spark variant `etl4/vault/raw_vault_spark.py::build_sat_state_observation_spark` (T-10) writes the Spark path. `vault_io.write_partition` is pandas-only; the Spark writer uses `df.write.partitionBy("ym").mode("overwrite").parquet(...)` directly. Reader still routes through `vault_io.read_table` (see spec section 6 "Spark writer note").

---

## 3. `etl4/vault/postgres_sink.py` - 1 call site (reader into Postgres COPY)

| # | Line(s) | Current call | Target call | Notes |
|---|---|---|---|---|
| 1 | L71 | `pd.read_parquet(parquet)` where `parquet = marts_dir / cfg["parquet"]` | `vault_io.read_table(name, ym_filter=ym, root=MARTS_ROOT)` | Called per-ym by T-17's rewrite. For `dim_aircraft` (non-partitioned) the fallback reads the monolith transparently. For `fact_trajectory_enriched` (partitioned) the caller passes `ym_filter=ym`. The `TABLES` dict changes from `{"dim_aircraft": {"parquet": "dim_aircraft/part.parquet", ...}, ...}` to `{"dim_aircraft": {"cols": [...]}, "fact_trajectory_enriched": {"cols": [...]}}`; the parquet-path formation moves inside the sink loop. |

---

## 4. `etl4/qa/reconcile.py` - 1 call site -> 0 direct calls

| # | Line(s) | Current call | Target call | Notes |
|---|---|---|---|---|
| 1 | L48-49 (`_count_rows` helper) | `sum(pq.ParquetFile(f).metadata.num_rows for f in files)` with `files = [path] if path.is_file() else sorted(path.rglob("*.parquet"))` | For bronze roots (BRONZE_FLIGHTLISTS, BRONZE_RAW_STATES): **unchanged** (bronze is not under vault_io). For silver/mart roots (VAULT_HUB_FLIGHT, VAULT_SAT_STATE_OBS, MART_FACT_TRAJECTORY): `vault_io.row_count(name, root=<root>)` | Simplest form: split `_count_rows` into `_count_bronze_rows(path)` (keeps current impl) and route the three silver/mart counts through `vault_io.row_count`. Architecture says "should go to 0 direct calls after switch to `vault_io.row_count`" - that holds for the three silver/mart counts. Bronze `_count_rows` stays because bronze is deliberately out of vault_io's scope. |

Post-T-19 grep check: `rg "pq\.ParquetFile|rglob" etl4/qa/reconcile.py` should show zero hits over silver/gold paths. The remaining hits (if any) are bronze-only and marked with a comment.

---

## 5. `etl4/qa/validate.py` - 1 call site (one expectations-suite source is silver/gold)

| # | Line(s) | Current call | Target call | Notes |
|---|---|---|---|---|
| 1 | L45-52 (`_load_parquet_tree`) | `files = sorted(path.rglob("*.parquet")); frames = [pd.read_parquet(f) for f in files]; df = pd.concat(frames)` | For `parquet_tree` sources under `data/gold/marts/**` or `data/silver/vault/**`: `vault_io.read_table(table_name, root=<root>)`. For `data/bronze/**`: unchanged. | Current `SUITES` in `etl4/qa/expectations.py`: bronze/raw_states (unchanged) and gold/marts/dim_airport (switches to `vault_io.read_table("dim_airport", root=MARTS_ROOT)`). Only one in-scope suite today; T-07 reviewer re-grep after PR5 merges to catch new suites. |

---

## 6. `etl4/bench/run_matrix.py` - 4 call sites (all out of partition-contract scope, FLAG for reviewer)

The matrix's silver layer is `data/silver/cleaned_states/<source>/<noise>/part.parquet`, which is NOT the DV vault silver and is NOT in the partition-contract scope (`architecture.md` section 2.4 only lists `data/silver/vault/*` and `data/gold/marts/*`). These reads stay on raw `pd.read_parquet`.

| # | Line(s) | Current call | Target call | Notes |
|---|---|---|---|---|
| 1 | L75 (`_load` helper) | `pd.read_parquet(path)` | unchanged | Bronze source parameter from CLI `--bronze`. |
| 2 | L77 (`_load` helper) | `pd.concat([pd.read_parquet(f) for f in files])` | unchanged | Bronze source parameter. |
| 3 | L87 (`_worker`) | `pd.read_parquet(silver_path)` | unchanged | Reads `data/silver/cleaned_states/...` - outside vault_io scope. |
| 4 | L88 (`_worker`) | `pd.read_parquet(truth_path)` | unchanged | Reads `etl4/data/reference/synthetic_truth/<date>.parquet` - outside vault_io scope. |

T-20 adds `--ym YYYY-MM` plumbing and a `date=YYYY-MM-*` bronze filter; the filter helper uses `extract_ym_from_path` from `vault_io`, but the bronze read itself stays on pandas.

---

## 7. `etl4/run_e2e.py` - 0 existing call sites; 1 new pre-flight check

No existing `pd.read_parquet` / `rglob` in this file (confirmed by grep). T-22 adds a pre-flight drift check that must use `vault_io.list_ym`:

| # | Line(s) | Current call | Target call | Notes |
|---|---|---|---|---|
| 1 | (new) in a `check_watermark_vs_filesystem()` helper | n/a | `vault_io.list_ym(table, root=SILVER_VAULT_ROOT)` for each partitioned silver table; `vault_io.list_ym(table, root=MARTS_ROOT)` for each partitioned mart table | Compare `set(list_ym(table))` against the contiguous range `[min_ym, pipeline_watermarks.last_ym]`; any hole exits non-zero with a message pointing at `rewind --layer <layer>`. Honours FR-17, NFR-07. |

---

## 8. SURPRISE call sites not listed in the architecture

### 8.1. `etl4/orchestration/pipeline_volumes.py` - 1 call site (SURPRISE, must migrate)

Powers the "ETL4 - DE Pipeline" dashboard. Scans silver/vault and gold/marts directories to count rows/bytes/files.

| # | Line(s) | Current call | Target call | Notes |
|---|---|---|---|---|
| 1 | L70-78 (`_scan_parquet_tree`) | `files = [f for f in path.rglob("*.parquet") if "_shards" not in f.parts]; rows = sum(pq.ParquetFile(f).metadata.num_rows for f in files); bytes_ = sum(f.stat().st_size for f in files)` | For `silver_vault` and `gold_marts` entries in `PARQUET_TARGETS`: `rows = vault_io.row_count(name, root=<layer_root>)`; file-count and bytes stay on the rglob (vault_io does not expose those). | SURPRISE: not called out in architecture.md section 5 Modules changed table, but it is a direct reader of the exact same trees and must migrate or it will keep reporting stale counts if `read_table` ever introduces caching. Flag to orchestrator: add this file as a new task under PR5 (call it T-24a: pipeline_volumes.py migration review), owner data-engineer, 10 LOC change. |

### 8.2. `etl4/orchestration/ingest_runner.py` - 0 in-scope call sites

| # | Line(s) | Current call | Scope | Notes |
|---|---|---|---|---|
| - | L34, L42-44 (`_du_bytes`, `_rows_in_tree`) | `rglob("*.parquet")` | Out of scope | Both helpers only run against `data/raw/opensky_flightlists` and `data/bronze/flightlists` / `data/bronze/raw_states`. Bronze - stays as-is. |

### 8.3. `etl4/bench/scale_study.py` - 0 in-scope call sites

| # | Line(s) | Current call | Scope | Notes |
|---|---|---|---|---|
| - | L102-105 | `files = sorted(bronze_dir.rglob("*.parquet")); frames = [pd.read_parquet(f) for f in files]` | Out of scope | Bronze reads for scale study. Stays. |

### 8.4. `etl4/bench/run_experiment.py` - 0 in-scope call sites

| # | Line(s) | Current call | Scope | Notes |
|---|---|---|---|---|
| - | L286-287 | `bronze = pd.read_parquet(bronze_path); truth = pd.read_parquet(a.truth)` | Out of scope | Bronze + reference. Stays. |

### 8.5. `etl4/run_pipeline.py` - 0 in-scope call sites

| # | Line(s) | Current call | Scope | Notes |
|---|---|---|---|---|
| - | L34-38 | `pd.read_parquet(path)` / `pd.concat([pd.read_parquet(f) for f in files])` | Out of scope | Bronze reads. Stays. |

### 8.6. `etl4/ingest/replay_producer.py` - 0 in-scope call sites

| # | Line(s) | Current call | Scope | Notes |
|---|---|---|---|---|
| - | L36-39 | `pd.read_parquet(p)` / per-file concat | Out of scope | Bronze state replay. Stays. |

### 8.7. `etl4/vault/bronze_flightlists.py` - 0 in-scope call sites

| # | Line(s) | Current call | Scope | Notes |
|---|---|---|---|---|
| - | L57 (rglob over `.csv.gz`), L62 (`pd.read_parquet(dst, columns=["icao24"])`) | n/a | Out of scope | Writes bronze flightlists; the parquet read is a side-channel idempotency check, not a vault read. Stays. |

### 8.8. Tests - 0 in-scope call sites

| File | Line | Current call | Scope |
|---|---|---|---|
| `etl4/tests/test_pipeline.py` | L34 | `pd.read_parquet(f)` for synthetic bronze | Out of scope - bronze fixture. |
| `etl4/tests/test_smoke.py` | L19 | `pd.read_parquet(f)` | Out of scope - bronze smoke. |
| `etl4/tests/inspect_bronze.py` | L10, L16 | `pd.read_parquet(f)` | Out of scope - bronze inspection. |
| `etl4/tests/smoke_kafka.py` | L22, L24 | `pd.read_parquet(files[0]).head(N)` | Out of scope - bronze smoke. |

New tests (T-25) that exercise `vault_io` are added under `etl4/tests/test_pipeline.py` and are the one place in `etl4/tests/` that imports `vault_io`.

---

## 9. Summary

| Layer | File | In-scope call sites | PR |
|---|---|---|---|
| Mart reader | `etl4/vault/marts.py` | 9 read + 4 write | PR3 (T-14) |
| Vault writer | `etl4/vault/raw_vault.py` | 1 writer helper (split partitioned / non-partitioned) + 1 resume-count SQL swap | PR2 (T-08 / T-09 / T-10) |
| Sink | `etl4/vault/postgres_sink.py` | 1 | PR4 (T-17) |
| QA reconcile | `etl4/qa/reconcile.py` | 1 helper (3 downstream count sites pulled through vault_io) | PR2 verify (per architecture §1.3) / PR4 follow-up (T-19 SQL review) |
| QA validate | `etl4/qa/validate.py` | 1 (one suite source today) | PR2 verify |
| run_e2e | `etl4/run_e2e.py` | 1 new pre-flight (T-22) | PR5 |
| DE dashboard volumes | `etl4/orchestration/pipeline_volumes.py` | 1 SURPRISE - add task T-24a | PR5 |
| run_matrix | `etl4/bench/run_matrix.py` | 4 silver_cleaned_states / bronze / truth reads - OUT of scope | - |
| Everything else | bronze readers / tests / scale_study | 0 - OUT of scope | - |

**Total in-scope call sites:** 15 reader + 4 writer + 1 new pre-flight = **20 sites across 7 files**, plus 1 surprise in `pipeline_volumes.py`.

Post-PR5 enforcement grep (add to CI or pre-commit):

```
rg "pd\.read_parquet\(.*data/(silver/vault|gold/marts)" etl4/ --glob '!etl4/vault/vault_io.py'
rg "rglob\(.\*\\.parquet.\)" etl4/ --glob '!etl4/vault/vault_io.py' --glob '!etl4/qa/reconcile.py' --glob '!etl4/bench/**' --glob '!etl4/ingest/**' --glob '!etl4/orchestration/ingest_runner.py' --glob '!etl4/tests/**' --glob '!etl4/vault/raw_vault.py'
```

Both must return zero hits. The exception globs are the bronze readers that remain out of vault_io scope and the reconcile bronze-count path.

---

*End of reader_conversion_checklist.md.*
