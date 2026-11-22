# T-19 SQL Review — PR4 (T-17 `postgres_sink.py` + T-18 `sink/postgres.py::write_run`)

**Reviewer:** data-engineer
**Date:** 2026-11-18
**Scope:** SQL-level correctness review against architecture.md §4 (binding patterns 4.1, 4.2, 4.3, 4.4). No code is modified by this review.
**Verdict: APPROVED.**

Sources read:
- `etl4/vault/postgres_sink.py` (T-17, 295 lines)
- `etl4/sink/postgres.py` (T-18, 215 lines)
- `etl4/vault/watermark.py` (T-02, consumed by T-17)
- `etl4/infra/create_tables.sql` tail (T-01 `pipeline_watermarks`, T-01B `pipeline_runs.ym` + UNIQUE)
- `multi-agent-engineering/projects/PROJ-20261118-impr01-partition-contract/architecture.md` §4
- `multi-agent-engineering/projects/PROJ-20261118-impr01-partition-contract/handoffs/20261118T1700-developer-backend-to-orchestrator.json`

---

## A. TEMP table lifetime — PASS

`etl4/vault/postgres_sink.py:143-144` and `:171-174` both emit:

```sql
CREATE TEMP TABLE stg_<target> (LIKE <target> INCLUDING DEFAULTS) ON COMMIT DROP
```

- `ON COMMIT DROP` is correct: the TEMP table disappears at the next COMMIT or ROLLBACK, so repeated calls within the same connection cannot collide across tables.
- Driver pattern: `main()` opens `with psycopg.connect(dsn, autocommit=False) as conn:` (`postgres_sink.py:286`); `load_table()` calls the per-table loader then `conn.commit()` (`:234`, `:258`). The staged INSERT, the `SELECT count(*)` and `watermark.set(..., conn)` all run inside the per-table cursor block that precedes that commit; so the TEMP table survives exactly long enough for the upsert + watermark, then drops at `conn.commit()`. Correct.
- `LIKE <target> INCLUDING DEFAULTS` deliberately omits PK + NOT NULL + indices. That is what the design wants: duplicate PKs in the staging payload are tolerated so `DISTINCT ON` can resolve them. Any NOT NULL violation surfaces at the target `INSERT ... SELECT` instead — informative, not silent. Accepted.
- Between tables the per-table commit means `stg_dim_aircraft` is already dropped before `stg_fact_trajectory_enriched` is created. No namespace collision risk.

No defect.

## B. DISTINCT ON tiebreaker correctness — PASS

**`dim_aircraft`** (`postgres_sink.py:148-161`):

```sql
INSERT INTO dim_aircraft (hk_aircraft, icao24, registration, typecode)
SELECT DISTINCT ON (hk_aircraft)
       hk_aircraft, icao24, registration, typecode
  FROM stg_dim_aircraft
 ORDER BY hk_aircraft,
          (registration IS NOT NULL) DESC,
          (typecode     IS NOT NULL) DESC
ON CONFLICT (hk_aircraft) DO UPDATE SET
    icao24       = EXCLUDED.icao24,
    registration = COALESCE(EXCLUDED.registration, dim_aircraft.registration),
    typecode     = COALESCE(EXCLUDED.typecode,     dim_aircraft.typecode)
```

Byte-for-byte match to architecture §4.2. Leading ORDER BY key = DISTINCT ON key (`hk_aircraft`). Prefer-non-null tiebreakers are last, correctly. `icao24` is unconditionally overwritten (an airframe's ICAO24 is fixed); nullable text columns COALESCE so a later month with a NULL doesn't wipe enriched state. Correct.

**`fact_trajectory_enriched`** (`postgres_sink.py:176-198`):

```sql
INSERT INTO fact_trajectory_enriched
    (hk_aircraft, icao24, time, lat, lon, baroaltitude,
     velocity, heading, callsign, typecode, registration, ym)
SELECT DISTINCT ON (hk_aircraft, time)
       hk_aircraft, icao24, time, lat, lon, baroaltitude,
       velocity, heading, callsign, typecode, registration, ym
  FROM stg_fact_trajectory_enriched
 ORDER BY hk_aircraft, time,
          (callsign IS NOT NULL) DESC,
          (typecode IS NOT NULL) DESC
ON CONFLICT (hk_aircraft, time) DO UPDATE SET
    lat          = EXCLUDED.lat,
    lon          = EXCLUDED.lon,
    baroaltitude = EXCLUDED.baroaltitude,
    velocity     = EXCLUDED.velocity,
    heading      = EXCLUDED.heading,
    callsign     = COALESCE(EXCLUDED.callsign,     fact_trajectory_enriched.callsign),
    typecode     = COALESCE(EXCLUDED.typecode,     fact_trajectory_enriched.typecode),
    registration = COALESCE(EXCLUDED.registration, fact_trajectory_enriched.registration),
    ym           = EXCLUDED.ym
```

Byte-for-byte match to architecture §4.3. DISTINCT ON key `(hk_aircraft, time)` is the leading prefix of ORDER BY — safe. Kinematic columns (`lat`, `lon`, `baroaltitude`, `velocity`, `heading`) overwrite unconditionally — latest observation wins, which is the right semantic for a re-materialized month. Text columns COALESCE. `ym = EXCLUDED.ym` matches architecture §4.3 "later load wins at this layer". Correct.

No defect.

## C. Same-txn watermark — PASS

`watermark.set` (`etl4/vault/watermark.py:68-91`) accepts an external `conn`, opens only a cursor (`with conn.cursor() as cur:`), issues one `INSERT ... ON CONFLICT DO UPDATE` against `pipeline_watermarks`, and does **not** call `conn.commit()`. Docstring is explicit: "Does NOT commit; the caller commits (or rolls back) so the watermark advance travels with the layer write."

In both `_load_dim_aircraft` (`postgres_sink.py:164`) and `_load_fact_trajectory` (`postgres_sink.py:205`) the call site is `watermark.set("sink", "<target>", ym, row_count, conn)`, same `conn` that ran `CREATE TEMP TABLE`, COPY, the staged INSERT, and the row-count SELECT. One `conn.commit()` follows in `load_table` (`:234`, `:258`). Properties:

- Rollback drops both: if the staged INSERT (or the watermark INSERT) raises, the `with conn.cursor() as cur:` exits, `conn.commit()` is never reached, and `with psycopg.connect(..., autocommit=False) as conn:` rolls back the full transaction on `__exit__`. The TEMP table is discarded, the `dim_aircraft` / `fact_trajectory_enriched` INSERT is rolled back, and the `pipeline_watermarks` row is rolled back. All three move together. Correct.
- "Committed main INSERT without watermark" is unreachable: both statements execute before the single `conn.commit()`.
- Cross-table: `dim_aircraft` commits before `fact_trajectory_enriched` begins. Per-table atomicity is the intended contract and matches architecture §4.1's "inside one transaction" phrasing (per target).

Minor observation: `_load_dim_aircraft` counts the full `dim_aircraft` table (non-partitioned); `_load_fact_trajectory` counts `WHERE ym = %s` (partitioned). This asymmetry is correct given each table's partition status — the watermark row reflects the "known state at this ym". Noted, not a defect.

No defect.

## D. `write_run` CTE correctness (T-18) — PASS

`etl4/sink/postgres.py:90-114` emits:

```sql
WITH new_run AS (
    INSERT INTO pipeline_runs
        (run_date, source, method, noise, ym,
         rows_in, rows_out, wall_secs)
    VALUES (CURRENT_DATE, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (ym, source, method, noise) DO NOTHING
    RETURNING run_id
)
SELECT run_id, TRUE AS created FROM new_run
UNION ALL
SELECT run_id, FALSE
  FROM pipeline_runs
 WHERE ym = %s AND source = %s AND method = %s AND noise = %s
   AND NOT EXISTS (SELECT 1 FROM new_run)
```

Branch analysis:

| Scenario | `new_run` content | First SELECT | Second SELECT | Output |
|---|---|---|---|---|
| New insert | one row `(run_id)` | `(run_id, TRUE)` | `NOT EXISTS (SELECT 1 FROM new_run)` is FALSE → no rows | exactly `(run_id, TRUE)` |
| Conflict (DO NOTHING) | empty | zero rows | `NOT EXISTS (SELECT 1 FROM new_run)` is TRUE; UNIQUE guarantee from T-01B returns exactly one pre-existing row | exactly `(run_id, FALSE)` |

Both branches return exactly one row; `run_id` is never NULL because it is bound to a real `pipeline_runs` row in both paths. Correct.

CTE visibility caveat: PostgreSQL's data-modifying CTE fires before the outer SELECT reads the snapshot; the second SELECT does **not** see rows the INSERT just wrote (same-statement visibility rules) — but that is exactly what the author wants, because the `NOT EXISTS` guard switches off the fallback branch whenever `new_run` is non-empty. Correct.

Duplicate historical rows for the same `(ym, source, method, noise)`: impossible after T-01B landed. `etl4/infra/create_tables.sql` tail shows the duplicate-collapse CTE then `ADD CONSTRAINT pipeline_runs_ym_src_mth_noise_key UNIQUE (ym, source, method, noise)` guarded by a `pg_constraint` existence check. Verified. If a defect ever slipped through, the second SELECT would return multiple rows and the eval_results gate below would still single-bind to `cur.fetchone()[0]` — a latent `run_id` ambiguity — but the UNIQUE constraint rules it out.

Orphan protection (architecture §4.4): `postgres.py:116-132` reads the `created` flag; if FALSE, logs the skip message and `return run_id` BEFORE the `eval_results`/`deviation_events` `executemany` block. `eval_results` and `deviation_events` are inserted only on new-run path (`:134-157`). Correct.

Defensive `fetchone` handling: `row = (cur.fetchone() or (None, False))` at `:115` is belt-and-suspenders — unreachable in practice because the CTE always returns a row; harmless fallback that logs "pipeline_runs insert returned no row" and returns `None`. Fine.

No defect.

## E. Transactional boundaries — PASS

**T-17 (`postgres_sink.py`):**
- Connection: `main()` uses `with psycopg.connect(a.dsn, autocommit=False) as conn:` (`:286`). psycopg3's context manager rolls back on exception at `__exit__`. Correct.
- Commit cadence: `load_table()` calls `conn.commit()` once per target after `_load_X` returns successfully (`:234`, `:258`). This gives per-target atomicity: TEMP drop, staged INSERT, watermark advance all commit together or all roll back together.
- Error path: exception inside `_load_X` → cursor closes with exception → `load_table` never reaches `conn.commit()` → the `with` connection's `__exit__` triggers rollback. TEMP table never materialized at disk (and would be dropped anyway via `ON COMMIT DROP`), target INSERT rolls back, watermark rolls back. Correct.
- One acceptable cross-table property: after `dim_aircraft` has committed, a failure in `fact_trajectory_enriched` leaves `dim_aircraft` + its watermark persisted and `fact_trajectory_enriched` unchanged. This is the designed granularity.

**T-18 (`sink/postgres.py`):**
- Connection: `conn = psycopg.connect(dsn, connect_timeout=3)` (`:82`) then `with conn:` (`:88`) — psycopg3's `Connection.__enter__` begins a transaction; `__exit__` commits on normal exit, rolls back on exception. Default `autocommit=False`. Correct.
- The connection is closed in `finally: conn.close()` (`:159-160`). Correct.
- `psycopg.OperationalError` on `connect()` is caught and returns `None` with a skip log — intended "dev without Postgres" behaviour. Narrow catch at the right boundary. Correct.
- Error path: exception inside the cursor block (e.g. UndefinedColumn pre-migration) propagates through `with conn:` → rollback. No `pipeline_runs` row, no `eval_results`, no `deviation_events`. Correct and matches "fail informatively" contract in the handback.

No defect.

---

## Cross-cutting checks

- Windows cp1252 safety: all new strings use ASCII (`'-->'`, `'--'`). Verified by grep for `0x80-0xff` non-ASCII glyphs — clean. PASS.
- Parameter binding: every `%s` placeholder goes through `cursor.execute` with a tuple; no string concatenation of user-controlled values. PASS.
- `icao24` dtype: `postgres_sink.py:124-125` and `:135-136` cast to `"string"` before COPY. CLAUDE.md §2.6 honoured. PASS.
- No `df.apply()` on hot paths; `_copy_into` chunks via `iloc[start:start+chunk_rows]` and `to_csv(buf, ...)` — vectorized. PASS.
- Idempotence: running the same ym twice produces no new `dim_aircraft` row (ON CONFLICT DO UPDATE is a state-not-count operation); on `fact_trajectory_enriched` same `(hk_aircraft, time)` is overwritten in place; on `pipeline_runs` same cell is a true no-op. Matches architecture §4.4 and PO AC-10. PASS.

---

## Overall recommendation

**APPROVED.** T-17 and T-18 implement architecture §4 as written. TEMP lifetime, DISTINCT ON tiebreakers, same-txn watermark, CTE branches and transactional boundaries are all correct. No SQL defects identified. No changes required of developer-backend.

Unblocks: T-20 (`run_matrix.py` passes `ym` into `write_run` explicitly), T-22 (`run_e2e` wires `--ym` through to `postgres_sink`), T-27 (rewind E2E exercises DO UPDATE path).

Risks carried forward (not defects, architectural reminders):
- R5 remains closed: the staged pattern is in place, no COPY-with-ON-CONFLICT regression possible.
- R10 remains closed: `write_run` is now DO NOTHING + CTE fallback, not the pre-IMPR-01 "fetchone()[0] on empty" TypeError path.
- Open per-table atomicity semantics (dim_aircraft commits independently of fact_trajectory_enriched within one `postgres_sink` invocation) is the designed granularity; operator rerun handles cross-table partial-failure state.

*End of t19_sql_review.md.*
