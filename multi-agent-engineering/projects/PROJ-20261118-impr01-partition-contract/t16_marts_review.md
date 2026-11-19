# T-16 Marts Review (T-14 per-ym build + T-15 cross-month `_latest_sfs`)

**Reviewer:** data-engineer
**Project:** PROJ-20261118-impr01-partition-contract
**Date:** 2026-11-20
**Review targets:**
- Commit `8161cd4` (T-14 `etl4/vault/marts.py` per-ym rewrite, +198 / -118)
- Commit `40f1fb4` (T-15 `_latest_sfs` cross-month reducer, +24 / -3)
**Verdict:** **APPROVED with a MEDIUM follow-up (ADR entry; not blocking T-24 review of PR5)**

Dependencies read alongside: `etl4/vault/vault_io.py` (T-05), `etl4/vault/watermark.py` (T-02), `etl4/vault/raw_vault.py` (T-08 / T-10 on-disk schema), `etl4/vault/raw_vault_spark.py` (T-10 on-disk schema), `reader_conversion_checklist.md` §1, `architecture.md` §§2.4 / 2.6 / 5 / 9.1 R1 / 9.3.

---

## A. Contract conformance

**PASS.**

### A.1 Per-ym build loop matches architecture §2.4 and §2.6

- `build_marts` runs dims once as full-refresh (`_write_monolith` writes `<out>/dim_*/part.parquet`), then enters a per-ym loop driven by `_yms_to_process(vault_root, last_ym)`. Both facts land under `<out>/<name>/ym=<YM>/part.parquet` via `vault_io.write_partition`, which is the canonical path from `vault_io_spec.md` §2.
- The loop driver reads `vault_io.list_ym("hub_flight")` and filters `[y for y in yms if y > last_ym]`. `hub_flight` is the correct calendar authority — flightlist is the source of record for which months exist (§2.4 bullet list). Months present only in `sat_state_observation` are intentionally NOT materialised here; that reconciliation lives in the T-22 run_e2e pre-flight check (R16, FR-17). Correct deferral.
- Column pushdown (`columns=_STATE_COLS`) is applied on the large fact read. Matches `reader_conversion_checklist.md` §1 row 9's mandate: pushdown is MANDATORY on `sat_state_observation` at 333M-row scale.

### A.2 `fact_trajectory_enriched.ym` column derivation matches architecture §2.5

- `build_fact_trajectory_enriched_for_ym` does `enriched = enriched.assign(ym=vault_io.ym_from_epoch(enriched["time"]))` AFTER the `hk_aircraft` merge. Re-deriving from `time` (rather than pushing down the on-disk `ym` column) is a defensive choice documented in T-14 contract_deviation 1: by construction the result equals the partition name because `sat_state_observation` was written with the same helper, but re-derivation protects against a future writer regressing to omit the in-file `ym`.
- Verified against both writer paths: `raw_vault.py::build_sat_state_observation` emits `_SAT_COLS = ["hk_aircraft","time","load_dt","rec_src","lat","lon","baroaltitude","velocity","heading","vertrate","callsign","onground"]`; `raw_vault_spark.py` emits `_SAT_COLS_WITH_YM` (same + `ym`). Smoke RUN 1 asserts `set(df["ym"].dropna().unique()) == {ym}` on each `fact_trajectory_enriched/ym=.../part.parquet` round-trip. PASS.

### A.3 dim_* full-refresh preserved

- `build_dim_aircraft` reads the globally-compacted `hub_aircraft` + `sat_aircraft_identity` monoliths, applies `_latest_per(["hk_aircraft"])` to the identity satellite (satellite convention: pick max `load_dt`), then left-merges onto the hub. This matches §4.5 for row shape and §4.2 sink-side `COALESCE` precedence. `dim_aircraft` is rebuilt ONCE per `build_marts` invocation and passed into every per-ym fact enrichment — stable across months in the same run. Correct per T-14 spec_gaps_resolved #2.
- `build_dim_airport` is a projection of `hub_airport` with reset_index. No satellite enrichment required; immutable once seen. Correct.

### A.4 Empty-partition path

- `build_fact_trajectory_enriched_for_ym` returns an empty frame with `_FACT_T_COLS` schema when `sat_state_observation/ym=<YM>/` is missing. `vault_io.write_partition` happily writes an empty parquet. Watermark still advances. The resume gate then idempotently keeps it empty on subsequent runs. Matches T-14 spec_gaps_resolved #1. Non-blocking: the T-22 drift check (R16) will catch any irrecoverable asymmetry.

---

## B. Watermark transactional safety

**PASS.**

```python
with psycopg.connect(dsn) as conn:
    watermark.set("marts", "fact_flight", ym, vault_io.row_count(...), conn)
    watermark.set("marts", "fact_trajectory_enriched", ym, vault_io.row_count(...), conn)
    conn.commit()
```

- Both fact-table upserts share one `psycopg.Connection` and one explicit `conn.commit()`. `watermark.set` is documented as "does NOT commit". Any exception between the two `set` calls (or after both, before `commit`) exits the `with` block without committing, triggering psycopg's implicit rollback and dropping BOTH upserts atomically. Matches the `watermark.py` module docstring contract: "the watermark write MUST happen inside the caller's transaction".
- Read gate keys only on `fact_trajectory_enriched` per the task objective; symmetric two-table write mirrors T-08's pattern and is correct for T-21 per-table rewind rollback (T-14 contract_deviation 2). This is a design-aligned expansion, not a deviation.

### B.1 Crash-safety audit of the write/commit order

Partition write order: `write_partition(fact_f)` → `write_partition(fact_t)` → `conn.commit()` (watermarks).

| Crash point | State on disk | State in Postgres | Resume behaviour |
|---|---|---|---|
| Before `write_partition(fact_f)` | no files for ym | no advance | ym re-processed from scratch (idempotent) |
| Between the two `write_partition` calls | fact_flight only | no advance | ym re-processed; both partitions atomically overwritten via `tmp + os.replace` (idempotent) |
| After both writes, before `conn.commit()` | both files present | no advance | ym re-processed; both partitions atomically overwritten (idempotent) |
| After commit | both files present | both watermarks advanced | ym skipped by resume gate (correct) |

No permanent inconsistency is reachable. The `tmp + os.replace` writer guarantees partition-write atomicity; the watermark txn guarantees sink-registry atomicity across the two tables.

### B.2 Residual observation (NON-BLOCKING)

`vault_io.row_count(..., ym_filter=ym, root=out_dir)` is called INSIDE the Postgres txn body, which means it reads the parquet footer under an open connection. If the parquet read raises (corrupted file, permission), the `with psycopg.connect` block exits via exception path → rollback → no watermark advance. Correct. No action.

---

## C. `_latest_sfs` correctness

**PASS as-written; MEDIUM gap flagged for follow-up ADR (not blocking T-24).**

### C.1 Sort key matches ADR R1

```python
sfs.sort_values(["hk_flight", "lastseen", "load_dt"],
                ascending=[True, False, False],
                kind="stable")
  .drop_duplicates(subset=["hk_flight"], keep="first")
```

- Primary key ASC + two DESC secondaries + `drop_duplicates(keep="first")` = `ROW_NUMBER() OVER (PARTITION BY hk_flight ORDER BY lastseen DESC, load_dt DESC) = 1`. Matches architecture finding #8 and §9.1 R1 exactly.
- `kind="stable"` preserves input order on triple ties (identical `hk_flight`, `lastseen`, `load_dt`) — necessary for deterministic byte output across reruns.
- T-15 smoke case (b) is the critical regression test: Jan row has newer `load_dt` (ingest rerun), Feb row has newer `lastseen` — the new reducer picks Feb; the pre-T-15 `_latest_per(['hk_flight'])` would have miscalled. Verified by smoke stdout.

### C.2 Known limit per T-15 handoff: per-ym scope MEDIUM

The reducer is called inside `build_fact_flight_for_ym` with `sfs = vault_io.read_table("sat_flight_summary", ym_filter=ym, ...)`. Jan and Feb partitions are never read into the same DataFrame, so the Jan/Feb cross-month pair never co-occurs inside a single `_latest_sfs` call. The reducer therefore closes the ADR R1 gap ONLY when both rows share a frame (migration multi-ym reads, or a future global post-build reducer).

In the current per-ym build:
- Both rows reach the gold layer: Jan → `fact_flight/ym=2019-01/part.parquet`, Feb → `fact_flight/ym=2019-02/part.parquet`. Each partition correctly contains "the latest row for its ym".
- Actual cross-month dedup happens at `postgres_sink` UPSERT: `ON CONFLICT (hk_flight) DO UPDATE SET lastseen=EXCLUDED.lastseen` (§4 line 862). Forward-chronological runs are correct (Feb overwrites Jan at sink).
- Rewind-and-rebuild of month M alone re-loads the Jan row and clobbers Feb's `lastseen` at sink. This is a rewind-contract issue (T-21), not a `_latest_sfs` defect.

**Recommendation:** APPROVED; add an ADR entry to make the per-ym scope explicit and pin the rewind-contract rule. Options for closing R1 fully, ranked by effort:

1. (CHEAPEST) ADR-level documentation: rewind of month M MUST rewind M+1 too when either month contains a cross-month flight — enforced by T-21 CLI help text + a one-shot sql query to detect.
2. (MEDIUM) Globally-scoped post-build reducer: a `compact_fact_flight_cross_month` pass that reads `fact_flight/ym=*` with a neighbour-pair ym_filter and re-runs `_latest_sfs` across the union. Runs once per build after the per-ym loop. Negligible cost (sat_flight_summary is small vs. state obs).
3. (HEAVIEST) Multi-ym window read inside `build_fact_flight_for_ym`: read `sfs` with `ym_filter=(prev_ym, ym)` and reduce. Rejected — doubles the sat_flight_summary read cost per month.

My preference is option 1 for this project and option 2 in a follow-up wave if the rewind-contract turns out to be too easy to violate in practice. Not blocking.

### C.3 `_latest_per` left untouched is correct

The remaining three `_latest_per` call sites (`sat_aircraft_identity` in `build_dim_aircraft`, `link_flight_aircraft` + `link_flight_route` in `build_fact_flight_for_ym`) do NOT carry a `lastseen` column. Extending `_latest_per` to sniff for `lastseen` would be hidden behaviour. Introducing a specifically-named `_latest_sfs` is the right factoring. Matches T-15 contract_deviation 1.

---

## D. Column pushdown + `icao24` dtype

**PASS.**

### D.1 `_STATE_COLS` correctness

```python
_STATE_COLS = ["hk_aircraft", "time", "lat", "lon", "baroaltitude",
               "velocity", "heading", "callsign"]
```

- Verified against BOTH writer paths:
  - `etl4/vault/raw_vault.py::build_sat_state_observation` → `_SAT_COLS = ["hk_aircraft","time","load_dt","rec_src","lat","lon","baroaltitude","velocity","heading","vertrate","callsign","onground"]` — NO `icao24`.
  - `etl4/vault/raw_vault_spark.py::_build_group` → `_SAT_COLS_WITH_YM = _SAT_COLS + ["ym"]` — also NO `icao24`.
- `reader_conversion_checklist.md` §1 row 9 listed `icao24` in `STATE_COLS`; this was a spec error in the checklist, not in the code. The developer caught it. `icao24` is enriched downstream via the `hk_aircraft` merge from `dim_aircraft`. Correct.
- `ym` is OMITTED from the pushdown list; `build_fact_trajectory_enriched_for_ym` re-derives it from `time` via `ym_from_epoch`. Matches §2.5 single-source-of-truth and the T-14 contract_deviation rationale (defensive against future regressions in the state-obs writer).

**Recommendation:** fix `reader_conversion_checklist.md` §1 row 9 to drop `icao24` from the documented STATE_COLS list. Low-priority housekeeping; no code change.

### D.2 `icao24` dtype preservation

- `vault_io.read_table` casts `icao24` to pandas `"string"` whenever the column is present in the read result (line 264-265, `_read_one_file` too at line 215-216). CLAUDE.md §2.6 preserved.
- In `build_fact_trajectory_enriched_for_ym`, `icao24` enters via the `dim_aircraft` merge. `dim_aircraft` is built from `hub_aircraft`, which was written by `raw_vault.py::build_hub_aircraft` with `icao24` already cast to `"string"` (line 119). The merge preserves dtype.
- `_FACT_T_COLS` includes `icao24`; the final `return enriched[_FACT_T_COLS]` keeps it.

### D.3 `callsign` normalisation

`enriched["callsign"] = enriched["callsign"].astype("string").str.strip()` is applied once before the `assign(ym=...)`. The source callsign (state observation) is kept; the dim-aircraft merge does NOT overwrite it (no `callsign` column in the merge). Matches the WI-1 pre-existing semantics ("callsign changes mid-day per aircraft" — keep per-observation).

---

## E. Idempotency / resume

**PASS.**

- `_yms_to_process` returns `[]` when every `hub_flight` ym is `<= last_ym`; `build_marts` prints "no new vault months to process" and returns with `counts["fact_flight"] = counts["fact_trajectory_enriched"] = 0`. Dims are still rewritten each run as a monolith — this is the intentional full-refresh contract on the two small tables (NON_PARTITIONED_TABLES set in vault_io.py).
- T-14 smoke evidence (AC-4) directly verifies the resume no-op:
  - RUN 2 counts: `fact_flight=0, fact_trajectory_enriched=0`
  - `st_mtime_ns` unchanged on EVERY `fact_*/ym=*/part.parquet` across RUN 1 → RUN 2
  - Watermark rows unchanged (both still `last_ym = '2019-02'`)
- Watermark comparison is a lexicographic string compare on `YYYY-MM`, which is correct for ISO-month ordering. `SYN-*` sentinels are intentionally excluded from `fact_*` partitions (synthetic path does not materialise marts per plan.md).
- `vault_io.read_table` returns `pd.DataFrame()` for a missing partition, so empty months propagate cleanly through the empty-frame path in §A.4 above without raising.

### E.1 Determinism (CLAUDE.md §3.5 check)

- `pd.concat` + `sort_values` + `drop_duplicates(keep="first")` are deterministic given stable input order and `kind="stable"`. `vault_io.read_table` enumerates `_parquet_files` via `sorted(...)`. No `apply()`, no `iterrows()`, no per-row `iloc`. Scales correctly to Spark promotion if future scale demands it.
- Pre-existing `load_dt` non-determinism in `raw_vault.py` (flagged in T-11 §A.1) is NOT propagated to fact tables here: `_FACT_T_COLS` and `_FACT_F_COLS` do not include `load_dt`. The marts layer is `load_dt`-clean.

---

## F. Follow-up ADR

**RECOMMENDED. Draft wording below. Not blocking T-24 PR5 review.**

Architecture §9.3 bullet 1 explicitly names a post-T-15 ADR as orchestrator homework. Proposed entry (to be appended to `etl4/docs/decisions.md` by the orchestrator, not written in this review):

```
## 2026-11-20 - Cross-month flight reader rule in marts._latest_sfs

Context:
  Under IMPR-01 partition contract, sat_flight_summary is read per-ym
  inside build_fact_flight_for_ym. A flight that departs month M and
  arrives M+1 lands rows in BOTH ym=M and ym=M+1 sat_flight_summary
  partitions with identical hk_flight, different (lastseen, load_dt).
  ADR 2026-11-13 Risk 1 proposed a reader-side mitigation in
  marts._latest_sfs.

Decision:
  - marts._latest_sfs orders by (hk_flight ASC, lastseen DESC, load_dt DESC)
    with stable sort + drop_duplicates(keep='first'). Implemented in T-15
    commit 40f1fb4.
  - The per-ym read scope means both cross-month rows never share a frame
    inside a single build_fact_flight_for_ym invocation. The reducer
    therefore only closes R1 when both rows co-occur (migration multi-ym
    reads, future global reducer).
  - Operationally R1 is closed by two downstream contracts:
    (a) postgres_sink INSERT ... ON CONFLICT (hk_flight) DO UPDATE SET
        lastseen=EXCLUDED.lastseen -- forward-chronological runs pick
        Feb over Jan at sink.
    (b) Rewind contract: rewind of month M MUST rewind M+1 too when
        either month contains a cross-month flight; T-21 CLI help text
        and a SQL detection snippet should surface this.

Consequences:
  - Running months in chronological order produces correct fact_flight
    at both parquet and Postgres layers.
  - A rewind of a single mid-range month without its neighbour will
    temporarily revert lastseen; a subsequent rebuild of M+1 restores
    it.
  - Open follow-up: a globally-scoped post-build reducer across
    (M-1, M, M+1) ym_filter tuples would close the gap without relying
    on sink-side UPSERT. Deferred; not required for current scale.

Links:
  - T-15 task JSON risks_for_t16
  - architecture.md section 9.1 R1, 9.3 bullet 1
  - t16_marts_review.md section C.2
```

The orchestrator should also:
- Reconcile `reader_conversion_checklist.md` §1 row 9 (drop `icao24` from the documented STATE_COLS; code is correct, doc is stale).

Neither is a blocker for T-24 PR5 review or for PR3 merge.

---

## Severity triage

| # | Finding | Severity | Action | Blocks |
|---|---|---|---|---|
| F.1 | `_latest_sfs` per-ym scope does not close R1 for cross-month rows that sit in separate partitions | **MEDIUM** | Capture in a new ADR entry (draft above); rely on sink UPSERT + rewind contract as the operational mitigation | NO — not T-24, not PR3 merge |
| F.2 | `reader_conversion_checklist.md` §1 row 9 lists `icao24` in STATE_COLS but the code (correctly) omits it | **LOW** | One-line doc fix | NO |
| F.3 | dim_* full-refresh re-writes the monoliths on every `build_marts` run even when no new ym exists | **LOW** | Accepted: dims are small; rewrite cost is sub-second. Matches §2.4. | NO |

No HIGH-severity findings. No defects blocking PR3 merge or T-24 PR5 review.

---

## Blockers for T-24 (PR5 review)

**NONE.**

T-24 reviews T-20 / T-21 / T-22 / T-23 (run_matrix --ym, rewind CLI, run_e2e drift check, stage_eval_mart). Nothing in T-14 or T-15 surfaces a blocker for those tasks. The MEDIUM finding (F.1) touches T-21's rewind contract via a documentation update, but the rewind code itself is not affected: T-21 already lands per-table watermark rollback for both fact tables (see commit `d90f60b` referenced in T-21 handoff). T-24 should merely note that rewind help text should mention the cross-month neighbour rule; this is a doc-only ask.

Risks previously flagged for T-24 in T-15 handoff are handled by F.1's ADR entry.

---

## Evidence summary

- Read: `etl4/vault/marts.py` (full file, 291 lines, post-T-15 state), `etl4/vault/vault_io.py` (340 lines), `etl4/vault/watermark.py` (92 lines), `etl4/vault/raw_vault.py::build_sat_state_observation` (lines 250-265), `etl4/vault/raw_vault_spark.py::_build_group` (lines 60-125), T-14 smoke (`artifacts/t14_marts_smoke.py` lines 1-291), T-15 smoke (`artifacts/t15_latest_sfs_smoke.py` header + case_b), `reader_conversion_checklist.md` §1, `architecture.md` §§2.4-2.6, §5 PR3 rows, §9.1 R1, §9.3 bullet 1, T-14 JSON + T-15 JSON (full task files), prior T-11 vault review (§A-E pattern reused here).
- Commits inspected: `8161cd4` (T-14 marts.py rewrite +198/-118, +291 lines of smoke), `40f1fb4` (T-15 _latest_sfs +24/-3, +232 lines of smoke).
- Smoke stdout re-asserted via task-JSON evidence chain: `MARTS SMOKE OK` (T-14 AC-3, AC-4, AC-5), `LATEST_SFS SMOKE OK` (T-15 AC-3), `pytest etl4/tests/ -q` = 92/0/0 both commits. Not re-executed under this static review.
- No code written, no file under `etl4/` touched.

---

## Verdict

**APPROVED.** T-14 is a clean per-ym rewrite that honours architecture §2.4 / §2.6 / §4.5 / §9.1 R1 verbatim; dim_* full-refresh is preserved, fact_* partitions follow the canonical path, watermarks advance for both fact tables inside one psycopg transaction per ym. T-15 adds the ADR R1 reader-side sort correctly. The one MEDIUM gap (per-ym read means cross-month rows don't co-occur inside `_latest_sfs`, so the reducer alone does not fully close R1) is operationally covered by the sink-side UPSERT and the rewind-contract rule; it deserves a new ADR entry (draft above) but does not block PR3 merge, T-16 approval, or T-24 PR5 review. All seven T-14 acceptance criteria and all six T-15 acceptance criteria stand under this review.
