# T-11 Vault Review (T-08 `_chunked_flightlist_vault` rewrite)

**Reviewer:** data-engineer
**Project:** PROJ-20261118-impr01-partition-contract
**Date:** 2026-11-19
**Review target:** commit `a008205` (T-08 only; T-20 run_matrix rewrite is out of scope for T-11)
**Verdict:** **APPROVED** (T-09, T-10, T-14 unblocked)

Dependencies reviewed alongside: `etl4/vault/vault_io.py` (T-05, DONE),
`etl4/vault/watermark.py` (T-02, DONE). Both are consumed as-is by T-08;
no deviations found in their surface area that invalidate T-08.

---

## A. Determinism

### A.1 Rerun on identical bronze + identical `last_ym`

**PASS (modulo pre-existing `load_dt` non-determinism, outside T-08 scope).**

- Bronze files are enumerated by `_flightlist_files` -> `sorted(flightlist_dir.rglob("*.parquet"))`. Sort key is the lexical path, which includes the `flightlist_YYYYMMDD_...` prefix: order is stable run-to-run.
- Within each per-ym iteration the builders (`build_hub_flight` / `build_link_*` / `build_sat_flight_summary`) are deterministic: `dropna + astype("string") + str.strip + str.upper + drop_duplicates(keep="first")` yields the same frame bytes given the same input order, which is guaranteed because `pd.read_parquet(f)` on a single file returns rows in file row order (not pyarrow dataset scan order).
- `hash_series` is MD5 over a concatenation of canonicalised string parts -> pure function, no non-determinism.
- `pa.Table.from_pandas(df, preserve_index=False) + pq.write_table(..., compression="snappy")` yields byte-stable output for a given logical frame; the only time-varying parquet-level field is `file_metadata.created_by` (library version string, not clock-sensitive).
- **Caveat:** `load_dt = datetime.now(timezone.utc)` at `build_raw_vault` entry is NOT frozen per ym. Two independent runs will stamp different `load_dt` values. This is a pre-existing property of `build_raw_vault` since WI-1; it is NOT a T-08 regression and was explicitly out of scope for this task. If `plan.md` sec 10 "byte-identical rerun" is to be literally enforced, the `load_dt` default should be threaded as a CLI/arg in a follow-up task (NOT T-11 scope; file as a tracked observation).

### A.2 `_DONE` sentinel + resume gate

**PASS.**

- Write order inside the per-ym loop is: `vault_io.write_partition` (atomic `tmp + os.replace` onto `part.parquet`) THEN `_write_done` (writes `_DONE` plaintext). A torn write cannot leave `_DONE` present without a committed `part.parquet` -> `_partition_done` returns True only when both exist, which is the correct safety direction.
- The resume gate (`skip_mask = [_partition_done(t, ym, out_dir) for t in PARTITIONED_TABLES]`) is per-table within the ym, and `fl` is only read when at least one table still needs processing. Good: on a resume where ALL four tables already landed, the bronze parquet is never re-read.
- Smoke evidence (RUN 2): `st_mtime_ns` unchanged on every `ym=*/part.parquet` across runs 1 and 2, confirming the resume gate. Cited in T-08 AC-4 evidence.

### A.3 Watermark advance idempotency

**PASS.** `watermark.set` is `INSERT ... ON CONFLICT (layer, table_name) DO UPDATE` (reviewed in `etl4/vault/watermark.py`). Re-upsert with the same `(ym, row_count)` is a no-op semantically, only `updated_at` moves. Acceptable per architecture sec 2.1.

### A.4 Residual risk flagged (NOT blocking)

On a crash AFTER all four `part.parquet` + `_DONE` land but BEFORE `conn.commit()` on the watermark upsert, the next run will:

1. See `skip_mask` all True for this ym and leave `fl = None`.
2. Still re-invoke `_compact_small_hub` with an empty `new_frames` list for the three small hubs; this re-reads + re-dedups + atomically rewrites the existing monolith (same content, different mtime). Idempotent for correctness; wastes one monolith rewrite.
3. Advance the watermark correctly.

Not a defect. Documented here so T-09's `compact.py` knows the `new_frames = []` branch MUST remain a legal no-op path.

---

## B. Contract conformance

### B.1 Per-ym write path matches architecture sec 2.4

**PASS.** The four partitioned tables (`hub_flight`, `link_flight_aircraft`, `link_flight_route`, `sat_flight_summary`) land under `ym=YYYY-MM/part.parquet` via `vault_io.write_partition`, which builds the exact path `<out_dir>/<name>/ym=<ym>/part.parquet`. Verified against architecture sec 2.4 bullet list.

### B.2 Small-hub compaction preserves `hub_aircraft` global uniqueness

**PASS.**

- `_compact_small_hub` reads the existing monolith into `frames[0]`, appends new per-ym frames, concats, dedups via `_dedup_safely` on `_FINAL_DEDUP[name]` (`hub_aircraft` -> `["hk_aircraft"]`). `keep="first"` in `_dedup_safely` means existing-monolith rows win over new-ym rows, which preserves the first-seen `rec_src` tag — matching the original `build_hub_aircraft` multi-source precedence.
- States extras are appended LAST in the `new_frames` list on the final ym (via `_states_extras_frame`), so flightlist rows win over state-only icao24 observations for the same business key. Matches the architecture "flightlist > state" rec_src precedence from WI-1.
- 2 GiB pyarrow dedup ceiling is correctly handled via `_dedup_safely` (string -> object -> dedup -> string round-trip). No change to this logic from the WI-1 baseline; T-08 preserves it.

### B.3 `ym` derivation matches `vault_io.ym_from_filename`

**PASS.** T-08 calls `vault_io.ym_from_filename(f.name)` — single source of truth, matching architecture sec 2.5 truth table ("flightlist bronze | ym_from_filename(path)"). No parallel ym helper is introduced in `raw_vault.py`.

### B.4 In-file `ym` column

**PASS.** `frame["ym"] = pd.Series([ym] * len(frame), dtype="string")` before `write_partition`. Matches architecture finding #4 (ym stays in file body so `pd.read_parquet(individual_file)` under CLAUDE.md sec 7 continues to work). Smoke asserts this round-trip.

---

## C. Same-txn watermark

**PASS.** The per-ym watermark advance is:

```python
with psycopg.connect(dsn) as conn:
    for t in PARTITIONED_TABLES:
        rc = vault_io.row_count(t, ym_filter=ym, root=out_dir)
        watermark.set("vault", t, ym, rc, conn)
    conn.commit()
```

All four `watermark.set` upserts share a single psycopg connection and a single explicit `conn.commit()`. Any exception inside the loop (or after it, before commit) exits the `with psycopg.connect(...)` context manager without committing -> psycopg's default implicit rollback drops all four upserts atomically. Verified against `watermark.py::set` which explicitly does NOT commit.

**Note on the read-side gate:** `_chunked_flightlist_vault` queries only `watermark.get("vault", "hub_flight")` for the skip filter. The developer's deviation 2 flagged this asymmetry. It is **safe** because the write path advances all four in one txn: either all four are at the same ym or none moved. hub_flight as the gate key is sufficient as long as the write path stays atomic — which it does.

---

## D. T-08 flagged deviations — severity triage

| # | Flagged deviation | Severity | Action |
|---|---|---|---|
| 1 | Smoke used tempfile fixture of head(5000) from real monthlies instead of `data/bronze/synthetic` | **LOW / RESOLVED** | The task literal was structurally impossible — `data/bronze/synthetic` is state-vector shaped (`date=/hour=`, no callsign/day columns) and filenames do not match `ym_from_filename`'s `flightlist_YYYYMMDD_YYYYMMDD` regex. The developer's substitution is a strictly stronger fixture. No action required. |
| 2 | Watermark advanced for all 4 partitioned tables; read gate keys on hub_flight only | **NONE / CORRECT** | Architecture sec 5 T-02, sec 6 seed_watermarks, and sec 7 rewind all key on per-table watermark rows. Writing only hub_flight would stale-skew the other three and break rewind per-table rollbacks. The single-table read gate is safe because the write is one-txn atomic. This is a design-aligned expansion of the task brief, not a deviation. |
| 3 | R13 `states_hub_input` unfiltered; deferred | **LOW / DEFER to T-10** | R13 severity in architecture sec 9.2 table is LOW. Current cost: `build_raw_vault` still reads the icao24 projection of every state parquet unfiltered. Post-T-10 the per-ym Spark path can emit an incremental `states_hub_input` as a byproduct (or T-10 can add a watermark-aware filter in the pandas path). This review pins R13 explicitly to T-10. If T-10 scope grows, split a dedicated R13 task. |
| 4 | `sat_state_observation` monolith path unchanged | **NONE / T-10 SCOPE** | Architecture sec 5 T-10 is the explicit scope owner for `sat_state_observation` per-ym partitioning in both the pandas and Spark branches. T-08's responsibility is flightlist-derived tables + small hubs. No scope leakage. |

---

## E. Inline `_compact_small_hub` stub — shape T-09 must honour

The stub signature is:

```python
_compact_small_hub(name: str, new_frames: list[pd.DataFrame], out_dir: Path) -> int
```

**Contract T-09 (`etl4/vault/compact.py`) MUST honour:**

1. **Signature compatibility.** Keep the three positional args or expose a wrapper under the same name that preserves them; `_chunked_flightlist_vault` line 480 is the only call site.
2. **Return int row count** of the post-compaction monolith (not the delta). Used in the stdout log line `{t} compacted: {n:,}`.
3. **Existing-monolith precedence.** Existing monolith rows MUST be unioned FIRST in the dedup input so `keep="first"` preserves the earliest-seen `rec_src` tag (multi-source contract from WI-1 `build_hub_aircraft`).
4. **`new_frames = []` is a legal input** — must be a correct no-op when the per-ym loop re-enters on resume with all partition sentinels present (see A.4 above). Current stub satisfies this by concat-of-only-existing-frame + rewrite.
5. **Atomic write via tmp + os.replace.** Current stub uses `_atomic_write_monolith`; T-09 must preserve atomicity (tmp file beside the target on the same filesystem, then `os.replace`).
6. **Business-key dedup** on `_FINAL_DEDUP[name]` (`hub_aircraft -> hk_aircraft`, `hub_airport -> hk_airport`, `sat_aircraft_identity -> hk_aircraft`). Keys are stable; do NOT redefine.
7. **pyarrow 2 GiB ceiling** — `_dedup_safely`'s string -> object -> string round-trip is load-bearing on real-scale inputs. Reuse or re-implement equivalently in `compact.py`. Do NOT regress to a straight pandas `drop_duplicates` on the pyarrow-backed string column.
8. **`icao24` dtype** stays pandas `"string"` throughout (CLAUDE.md sec 2.6). Dedup round-trip is the only place it may transit `object`; the final write must be `"string"`.
9. **Windows cp1252-safe stdout** — any new print statements must be ASCII (no arrows, no em-dashes).
10. **Determinism.** Same inputs -> byte-identical monolith (modulo the pre-existing `load_dt` non-determinism).

Call-site change expected in T-09: swap the inline function body for an import and a call. The call signature in `_chunked_flightlist_vault` does NOT need to change. Marker comment at line 341 already names T-09 as the owner.

---

## Blockers for downstream tasks

| Task | Blocked? | Notes |
|---|---|---|
| T-09 (`compact.py`) | NO | Contract pinned in E above. Can proceed. |
| T-10 (`raw_vault_spark.py` per-ym `sat_state_observation`) | NO | Flagged deviation 3 (R13 `states_hub_input`) pinned to T-10 as follow-up. |
| T-14 (`marts.py` partition-aware reads) | NO | T-08 partitions land under the architecture-mandated paths; `vault_io.read_table(ym_filter=...)` consumes them cleanly (verified by smoke RUN 2 round-trip at line 224 of `t08_vault_smoke.py`). |

---

## Open observations (non-blocking, tracked for later)

1. **Per-run `load_dt`.** `datetime.now(timezone.utc)` in `build_raw_vault` prevents byte-identical reruns across clock boundaries. Pre-existing; file as improvement if T-25 determinism test needs strict byte-equality.
2. **State-extras folded only on final ym of the current run.** Correct for state coverage, but if a later ym arrives with new state data for icao24 already finalized earlier, the subsequent run will re-fold states extras into hub_aircraft at the NEW final ym — still idempotent. Noted.
3. **Small-hub recompaction cost.** O(N months × monolith size) reads/writes per run, because each ym iteration re-reads the monolith. Fine at current scale (hub_aircraft ~10^5 rows). Candidate micro-optimisation for `compact.py`: accumulate per-run frames in memory and compact once at the end.

---

## Evidence summary

- Read: `etl4/vault/raw_vault.py` lines 1-590 (full file), `etl4/vault/vault_io.py` lines 1-340, `etl4/vault/watermark.py` lines 1-92, `etl4/vault/schema.py::hash_series` lines 32-53, `multi-agent-engineering/projects/PROJ-20261118-impr01-partition-contract/artifacts/t08_vault_smoke.py` lines 1-250, `multi-agent-engineering/projects/PROJ-20261118-impr01-partition-contract/architecture.md` sec 2.4 / 2.5 / 5.T-08 / 9.R13, `multi-agent-engineering/projects/PROJ-20261118-impr01-partition-contract/tasks/T-08.json`.
- Commit inspected: `a008205` (T-08 + T-20).
- Smoke artefact executed (prior to this review) in T-08 handoff: `VAULT_SMOKE OK`; mtime-equality on resume; watermark-row evidence per partitioned table.
- `pytest etl4/tests/ -q`: 92 passed per commit message (per T-08 handoff; not re-run under this review since review is static).

---

## Verdict

**APPROVED.** The T-08 rewrite is correct, deterministic within its scope, honours the partition contract, keeps the same-txn watermark invariant, and leaves a clean replace point for T-09. The four flagged deviations are either stronger-than-literal (1, 2) or correctly deferred to T-10 (3, 4). Unblocks T-09, T-10, T-14 and the PR2 vault rewrite chain.
