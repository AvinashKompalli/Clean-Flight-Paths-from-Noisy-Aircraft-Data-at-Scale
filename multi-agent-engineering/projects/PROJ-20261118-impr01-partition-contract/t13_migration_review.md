# T-13 Migration Review - etl4/scripts/migrate_partition_ym.py

**Project:** PROJ-20261118-impr01-partition-contract
**Reviewer:** data-engineer
**Subject:** T-12 commit 2145dbf (feature/pipeline-3-avinash), 691-line one-shot migration script
**Date:** 2026-11-20
**Status:** APPROVED with binding decisions on the 3 flagged schema gaps + architecture section 6 patch below
**Depends-on reviews (prior PASS):** T-11 (T-08 vault), T-16 (T-14/T-15 marts), T-19 (T-17/T-18 SQL)

---

## Executive summary

The migration script implementation is CORRECT for the 4 tables it will migrate from monolith (`hub_flight`, `sat_state_observation`, `fact_flight`, `fact_trajectory_enriched`). For the 3 tables it flagged as `schema_mismatch` the gaps are REAL and the dev-backend's refusal to silently migrate is the right call. Binding decisions below pick FILE-MONTH as the single authoritative key for all flightlist-derived vault tables, resolved via BRONZE REBUILD (Option C) for the 3 flagged tables -- which aligns migration output byte-for-byte with the committed T-08 writer (`raw_vault.py` L543) and avoids split-brain.

**Dispatch/reality conflict surfaced**: the T-13 dispatch asserts "Option A (firstseen) matches T-08". Primary-source evidence (`etl4/vault/raw_vault.py` line 543: `frame["ym"] = pd.Series([ym] * len(frame), dtype="string")` where `ym = ym_from_filename(f.name)`) shows T-08 uses FILE-MONTH for every partitioned flightlist-derived table. T-11 reviewed and APPROVED this behaviour. The dispatch's framing is inverted; decisions below follow the primary source.

**Second dispatch/reality conflict**: the dispatch claims "Option B breaks the T-15 ADR". The 2026-11-20 T-15 ADR (etl4/docs/decisions.md, final entry) explicitly states the per-ym read scope means both cross-month rows never share a frame inside a single `build_fact_flight_for_ym` invocation, and the reader rule therefore only closes R1 when both rows co-occur (multi-ym reads, future global reducer); operationally R1 is closed by sink-side UPSERT + the rewind M+/-1 contract, NOT by partition-key choice. Both Option A and Option B are equally compatible with T-15; Option B is further compatible with the committed T-08 writer.

---

## 1. Per-gap binding decisions

### Gap 1 - link_flight_aircraft / link_flight_route lack `day` column

**Primary source**: `etl4/vault/raw_vault.py` lines 158-199. `build_link_flight_aircraft` output schema is `[hk_flight_aircraft, hk_flight, hk_aircraft, load_dt, rec_src]`; `build_link_flight_route` output schema is `[hk_flight_route, hk_flight, hk_airport_origin, hk_airport_destination, load_dt, rec_src]`. Neither emits `day`. The monolith is a straight concat of these outputs, so it cannot carry `day` either. `schema_mismatch` is correct, not a bug.

**Downstream dependency on `day`**: none. `marts.build_fact_flight_for_ym` joins link tables to `hub_flight` on `hk_flight` (not `day`) and groups by `hk_flight`. Neither mart nor sink nor matrix reads `day` off a link table. Adding `day` to link output solely to carry ym would be dead weight everywhere except migration.

**Binding decision: OPTION C - rebuild from flightlist bronze via T-08.**

Rationale:
1. The committed T-08 writer uses FILE-MONTH (`ym_from_filename(f.name)`) for every flightlist-derived partitioned table. Any migration path that produces a different key creates split-brain under rewind.
2. The link tables together are ~165M rows (approx 2x hub_flight, which was 92.9M). T-08's per-ym loop over 48 bronze files is designed exactly for this. Each file is one month; cost per month is minutes.
3. Option A (extend builders to emit `day` + regenerate monolith) is more code, more expensive, and introduces a dead column. Rejected.
4. Option B (in-memory join against hub_flight) consumes 3.5 GB RSS one-shot, adds a code path that exists only for the migration, and if hub_flight and link_* diverge in future months the join silently loses rows. Rejected.
5. Option C deletes the two link monoliths, then T-08's `_partition_done` machinery (which already checks the `_DONE` sentinel per partition) rebuilds only the link partitions per month -- hub_flight / sat_flight_summary / sat_state_observation partitions that the migration committed get `_partition_done=True` from their sentinel and are skipped.

**Operator action**: after confirming the 4 migratable tables pass their row-count gate (step 4 of §3 below), delete `data/silver/vault/link_flight_aircraft/part.parquet` and `data/silver/vault/link_flight_route/part.parquet`, clear the watermark for these two tables only (`DELETE FROM pipeline_watermarks WHERE layer='vault' AND table_name IN ('link_flight_aircraft', 'link_flight_route')`), then run the standard incremental vault pass. T-08 will iterate every bronze month, skip the already-partitioned tables via `_partition_done`, and write link partitions keyed by `ym_from_filename` -- byte-identical to how they would have been written on a fresh pipeline.

### Gap 2 - sat_flight_summary.firstseen key vs T-08 file-month key

**Primary source**: `etl4/vault/raw_vault.py` line 543 writes `frame["ym"] = pd.Series([ym] * len(frame), dtype="string")` where `ym = ym_from_filename(f.name)`. This is unambiguously FILE-MONTH for sat_flight_summary (and for hub_flight, link_*). T-11 approved this behaviour on 2026-11-19.

Dry-run empirical: 12,685 of 4,600,646 rows have `firstseen` in a different calendar month than the bronze file's month (6,152 in the June file with firstseen=May; 6,533 in the December file with firstseen=November).

**T-15 ADR reconciliation**: the 2026-11-20 T-15 ADR says the reader-side rule in `_latest_sfs` closes R1 only when both cross-month rows co-occur in a single frame (multi-ym read) and that operationally R1 is closed by `postgres_sink` `ON CONFLICT (hk_flight) DO UPDATE SET lastseen=EXCLUDED.lastseen` + the rewind M+/-1 contract. The key choice at the partitioning layer does NOT drive R1 closure either way. The dispatch's "A matches T-15" is a misreading.

**Binding decision: OPTION B - FILE-MONTH key (via Option C bronze rebuild).**

Rationale:
1. Matches the committed T-08 writer exactly. Zero split-brain risk under rewind.
2. Zero code change in T-08 (which has already shipped and been reviewed).
3. Zero code change in the migration script -- it falls through the `unsupported_reason` gate and sat_flight_summary never attempts a firstseen-based migration.
4. sat_flight_summary monolith is 4,600,646 rows across 2 months. Rebuilding from bronze via T-08 is < 1 minute total.
5. Option A (migrate via firstseen) would partition 6,152 May-firstseen rows into `ym=2022-05` and 6,533 Nov-firstseen rows into `ym=2022-11` even though T-08's ongoing writer places them at `ym=2022-06` and `ym=2022-12`. Any subsequent rewind+rebuild of 2022-06 regenerates those 6,152 rows at `ym=2022-06` -- now the same 6,152 rows exist in two partitions. The sink UPSERT masks this at Postgres but the parquet tree carries 6,152 dead rows at `ym=2022-05`. Rejected.

Option A would also require either (a) coercing T-08 to use firstseen (reopens T-08, invalidates T-11), or (b) accepting permanent key-disagreement between migration and ongoing writes. Neither is acceptable.

**Operator action**: as for Gap 1 but on `sat_flight_summary`. Delete `data/silver/vault/sat_flight_summary/part.parquet`, clear its watermark row, run the incremental vault pass. T-08 writes 2 partitions (`ym=2022-06` with 2,454,119 rows at file-month; `ym=2022-12` with 2,146,527 rows at file-month).

### Gap 3 - In-file `ym` column + `_DONE` sentinel on migration output

**Primary source**:
- `etl4/vault/raw_vault.py` line 543: `frame["ym"] = pd.Series([ym] * len(frame), dtype="string")` -- T-08's writer appends `ym` inside the file body.
- `etl4/vault/raw_vault.py` line 448-450 `_write_done`: drops an `_DONE` file beside `part.parquet` holding the load_dt.
- `etl4/vault/raw_vault.py` line 529 `_partition_done`: the resume gate checks for both `part.parquet` and `_DONE`.
- Architecture section 6.2 (committed): "`ym` is retained in-file; readers that `pd.read_parquet(f)` individual files via CLAUDE.md section 7 do not lose the partition column."

The in-file `ym` column is already in architecture section 6.2 (writer-reader contract) -- the dispatch's "not in architecture" framing is partially inaccurate: the writer contract specifies it, only the section-6 code template is missing the explicit mention.

The `_DONE` sentinel is NOT in architecture section 6. Without it, the next T-08 run with a cleared watermark would see `part.parquet` present but no `_DONE` and interpret the partition as "legacy monolith residue" or at minimum re-enter the rebuild branch. Advisor was right to flag this during T-12 implementation and dev-backend was right to add it.

**Binding decision: CONFIRM both additions are correct and binding. Architecture section 6 is amended (patch in §4 below) to spell out both requirements.**

**Operator-facing note**: the sentinel content matches `raw_vault._write_done` format (`load_dt=<iso>\n`); the migration script writes an extra `source=migrate_partition_ym\n` line which is informational and does not break `_partition_done`'s check (which only tests for file existence, not content).

---

## 2. Operator runbook (revised; supersedes T-12.json operator_runbook on conflict)

### Preconditions
1. PR2 merged -- vault_io.read_table transparent partition/monolith fallback is live.
2. PR3 merged -- marts.py reads vault via vault_io.read_table.
3. `pipeline_watermarks` table exists (T-01 DDL applied).
4. Disk headroom >= 2 * sum(monolith size) for the 4 migratable tables; the migration doubles that footprint until the operator deletes monoliths.
5. **Pre-rewind lock** -- NO rewind of any ym may happen between steps 1 and 8 below. Rewind deletes partitions the migration wrote; the T-08 bronze-rebuild of Gap-1/Gap-2 tables depends on those partitions' `_DONE` sentinels to skip the already-migrated tables.

### Step-by-step

**1. Dry-run everything**
```powershell
python -m etl4.scripts.migrate_partition_ym --dry-run
```
Expect: exit 0. 4 tables plan; 3 tables `schema_mismatch` WARN. SQL block printed. Zero filesystem change (`Get-ChildItem data -Recurse -Filter '*.tmp' -ErrorAction SilentlyContinue` and `Get-ChildItem data -Recurse -Filter 'ym=*' -ErrorAction SilentlyContinue` both empty for currently-unmigrated tables).

**2. Real-run the supported subset**
```powershell
python -m etl4.scripts.migrate_partition_ym --tables hub_flight,sat_state_observation,fact_flight,fact_trajectory_enriched
```
Expect: exit 0. Four tables report `migrated`. `pipeline_watermarks` rows upserted for each. SQL block printed to stdout.

**3. Apply the Postgres-side migration**
```powershell
python -m etl4.scripts.migrate_partition_ym --dry-run --tables fact_trajectory_enriched 2>$null `
  | Select-String -Pattern '---- Postgres' -Context 0,20 `
  | psql $env:POSTGRES_DSN
```
Alternatively copy the `POSTGRES_SQL_BLOCK` output and run via `psql -f`. Expect the `ym` column added, backfilled from `to_char(to_timestamp(time), 'YYYY-MM')`, `NOT NULL` enforced, index `idx_fact_traj_ym` created.

**4. Row-count equality verification (HARD GATE for monolith delete)**
For each of the four migrated tables, assert:
```
vault_io.row_count(name) == pre_migration_monolith_footer_count
```
The migration script's stdout already prints `monolith=N written=N` on each; the operator reconciles that `monolith == written`. If any row count disagrees, STOP; do not proceed to step 5.

**5. Delete monoliths for the 4 migrated tables ONLY**
```powershell
Remove-Item data/silver/vault/hub_flight/part.parquet
Remove-Item data/silver/vault/sat_state_observation/part.parquet
Remove-Item data/gold/marts/fact_flight/part.parquet
Remove-Item data/gold/marts/fact_trajectory_enriched/part.parquet
```
DO NOT delete:
- `data/silver/vault/link_flight_aircraft/part.parquet`
- `data/silver/vault/link_flight_route/part.parquet`
- `data/silver/vault/sat_flight_summary/part.parquet`

**6. Rebuild Gap-1 and Gap-2 tables from bronze (binding decision)**
Clear the three watermarks in a single transaction:
```sql
BEGIN;
DELETE FROM pipeline_watermarks
 WHERE layer = 'vault'
   AND table_name IN ('link_flight_aircraft', 'link_flight_route', 'sat_flight_summary');
COMMIT;
```

Then delete the three monoliths:
```powershell
Remove-Item data/silver/vault/link_flight_aircraft/part.parquet
Remove-Item data/silver/vault/link_flight_route/part.parquet
Remove-Item data/silver/vault/sat_flight_summary/part.parquet
```

Then run the incremental vault pass:
```powershell
python -m etl4.vault.raw_vault
# or whatever entrypoint build_raw_vault is wired behind; see run_e2e.py
```

T-08 iterates bronze flightlists; for each month, `_partition_done` reports True for `hub_flight` (migration dropped its `_DONE` sentinel) and False for the three cleared tables; those three get built and watermarked. Expected per-month runtime is minutes, not hours; total rows rewritten = original monolith rows.

**7. Verify Gap-1/Gap-2 row counts**
```python
# or equivalent psql
python -c "from etl4.vault import vault_io; [print(t, vault_io.row_count(t)) for t in ('link_flight_aircraft','link_flight_route','sat_flight_summary')]"
```
The totals must match the pre-migration monolith footer counts of the respective tables. Dev-backend's dry-run reported these as:
- `link_flight_aircraft`: (pre-migration monolith footer; verify)
- `link_flight_route`: (pre-migration monolith footer; verify)
- `sat_flight_summary`: 2,454,119 + 2,146,527 = 4,600,646

**8. Release the rewind lock**
Document the migration completion in `etl4/docs/decisions.md` (follow-up ADR by orchestrator; see §5 below). After this point, `python -m etl4.orchestration.rewind` is safe to run again.

### Exit conditions (for monolith delete and for rewind release)

| Phase | Precondition |
|---|---|
| Step 5 (delete 4 monoliths) | Every `migrated` table satisfies `vault_io.row_count == pre-migration footer` in step 4. |
| Step 6 (rebuild 3 tables) | Step 5 complete; watermark rows for the 3 cleared. |
| Step 7 (verify) | Step 6 completed without error. |
| Step 8 (release rewind lock) | Step 7 row-counts match pre-migration footers. |

---

## 3. Exit-code policy review (as committed in migrate_partition_ym.py main)

| Scenario | Dev-backend policy | Review verdict |
|---|---|---|
| `--dry-run` + any `schema_mismatch` | WARN tag in stdout, exit 0 | ACCEPT. Dry-run is an operator surface; operator must see the gap before committing to a run. |
| `--dry-run` + any `row_count_mismatch` (NA in ym column) | exit 1 | ACCEPT. A paper data-loss is still data-loss. |
| Real run + any `schema_mismatch` | FAIL tag, exit 1, `seed_watermarks` SKIPPED | ACCEPT. The advisor-caught risk of a mixed migrate-4-skip-3 half-baked state is real. Operator must opt in via `--tables`. |
| Real run + any `row_count_mismatch` | exit 1, writer tmp files cleaned, monolith untouched | ACCEPT. Streaming writer aborts cleanly; no partial partitions committed. |

Exit code 2 reserved for argparse errors. All above policy is correct and pins down the architecture section 6 gap in "migration is gated on every reader being converted first" without being prescriptive beyond the operator's actual tool surface.

---

## 4. Patch text for architecture.md section 6 (orchestrator applies to architecture.md)

Replace the architecture.md section 6 `PARTITIONED_TABLES` dict in the migration-script code block with:

```python
PARTITIONED_TABLES = {
    # key_source is the row-level column used by the migration helper.
    # Four flightlist-derived vault tables are NOT in this dict:
    #   - hub_flight (migrated via day -> ym_from_date; see note A below)
    #   - link_flight_aircraft, link_flight_route (no day column; rebuild from bronze via T-08 after monolith delete)
    #   - sat_flight_summary (firstseen crosses file-month; rebuild from bronze via T-08 after monolith delete)
    # These four are instead reconciled with T-08's FILE-MONTH key. Migration of hub_flight
    # is a streaming rechunk because its day-month equals file-month for the data as of
    # 2026-11-20; a future violation would be caught by a cross-check in migrate_one.
    #
    # The remaining three are state/trajectory-derived (no file-month concept, time epoch
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
```

Add immediately after the writer-reader contract subsection:

> **6.3. In-file `ym` column + `_DONE` sentinel on migration output (BINDING)**
>
> Every migrated partition file carries `ym` as an in-row column of dtype `string`. This mirrors `etl4/vault/raw_vault.py` line 543 (T-08 writer convention). Without the in-file column, concurrent reads of migrated partitions + T-08-written partitions through `vault_io.read_table` would yield `NaN` for `ym` on migrated rows (Spark-shard broadcast join risk and `fact_trajectory_enriched.ym` backfill drift).
>
> Every migrated partition directory carries an `_DONE` sentinel file matching the `raw_vault._write_done` format: `load_dt=<ISO datetime>\n`. The migration may append `source=migrate_partition_ym\n` for provenance. `_partition_done` (T-08 line 529) tests existence only, so this is byte-safe. Without the sentinel, a subsequent T-08 run with a cleared watermark would rebuild over the migrated partition and double-write the month.

Add immediately after section 6.1:

> **6.4. Three flightlist-derived vault tables are rebuilt from bronze, not stream-rechunked (BINDING, T-13 decision 2026-11-20)**
>
> `link_flight_aircraft`, `link_flight_route`, and `sat_flight_summary` are not stream-migrated from their monoliths because:
> - Link monoliths have no row-level month column (builders at `raw_vault.py` L158-199 drop `day`).
> - `sat_flight_summary` monolith's `firstseen` key disagrees with T-08's FILE-MONTH key for 12,685 cross-month rows; any firstseen-based migration would create split-brain under rewind.
>
> Operator procedure: migrate the other 4 tables first; row-count-verify; delete those 4 monoliths; clear the 3 cleared-table watermarks; delete the 3 remaining monoliths; run `python -m etl4.vault.raw_vault` (or equivalent entrypoint) to rebuild the 3 tables per-month via T-08's `_partition_done`-gated loop. The migration is wall-cost dominated by the first step (sat_state_observation rechunk); the bronze rebuild of the 3 tables is minutes.
>
> Byte-equivalence with T-08: all four flightlist-derived vault tables (hub_flight, link_*, sat_flight_summary) land on partition keys consistent with T-08's committed `frame["ym"] = ym_from_filename(f.name)` writer.

Add to §9.1 risks table after R4:

> | R1b | hub_flight has a row with `day` in month M and lives in bronze file for month M+1 (currently 0 rows at scale; future risk) | LOW | Cross-check inside `migrate_one` for `hub_flight`: compare `day`-derived ym against bronze source filename once the migration is invoked; abort on mismatch | T-12 defensive check (follow-up) |

---

## 5. Follow-up ADR text for etl4/docs/decisions.md (orchestrator appends; append-only log)

### 2026-11-20 - IMPR-01 T-13: FILE-MONTH as the single ym key for flightlist-derived vault tables

**Context**: ADR 2026-11-13 and architecture.md section 2.5 originally specified `ym_from_date(df.day)` for `hub_flight`, `link_*`, and `sat_flight_summary`. The T-08 implementation of `_chunked_flightlist_vault` (commit a008205, T-11 APPROVED 2026-11-19) writes `frame["ym"] = pd.Series([ym] * len(frame), dtype="string")` where `ym = ym_from_filename(f.name)`, i.e. FILE-MONTH. For the production data as of 2026-11-20 these two keys agree on 99.72% of `sat_flight_summary` rows and 100% of link-table rows (which carry no `day`), but the migration script surfaced 12,685 cross-month `firstseen` rows where the keys disagree.

**Decision**:
1. FILE-MONTH (via `ym_from_filename(f.name)`) is the AUTHORITATIVE partition key for every flightlist-derived vault table: `hub_flight`, `link_flight_aircraft`, `link_flight_route`, `sat_flight_summary`.
2. Architecture.md section 2.5's "`ym_from_date(df.day)`" row is reinterpreted as "best-effort row-level key that MUST agree with file-month". A future violation (a bronze file containing rows with `day` in a neighbouring month) is a bronze upstream bug that must be fixed before T-08 writes the file; not a partitioning decision.
3. The one-shot migration script (etl4/scripts/migrate_partition_ym.py) rebuilds `link_*` and `sat_flight_summary` from bronze via T-08 rather than stream-rechunking their monoliths. `hub_flight` is still stream-rechunked because its `day` column IS present in its monolith and has 100% row-level agreement with file-month at current data (verified by T-12 dry-run exit 0).

**Consequences**:
- Migration output byte-matches T-08's ongoing writes; rewind M followed by rebuild M produces the same partition bytes as the original migration.
- `_latest_sfs` cross-month reader rule (ADR 2026-11-20) continues to close R1 via sink UPSERT + rewind M+/-1 contract; partition-key choice is orthogonal.
- No code change in T-08 or in any committed reader; only the migration script's TABLE_SPECS and the operator runbook change.

**Alternatives considered**:
- Firstseen-based migration key for `sat_flight_summary` (rejected: creates permanent split-brain with T-08's file-month on 12,685 rows under rewind).
- Add `day` column to link-table builders + regenerate monolith (rejected: dead column everywhere except migration; more code than bronze rebuild).
- In-memory join of link monoliths against hub_flight to backfill `day` (rejected: 3.5 GB RSS, custom one-shot code path).

**Risks**:
- A future bronze file with cross-month `day` rows (today's count: 0 on hub_flight, 12,685 on sat_flight_summary but the firstseen-vs-day distinction removes the issue) would be hidden by file-month partitioning. Mitigation: documented as R1b in architecture.md section 9.1.

**Links**: T-12 commit 2145dbf; T-13 review `multi-agent-engineering/projects/PROJ-20261118-impr01-partition-contract/t13_migration_review.md`; T-11 review approval; T-15 ADR (etl4/docs/decisions.md 2026-11-20 cross-month flight reader rule); architecture.md section 6.3 + 6.4 (patch in §4 of t13_migration_review.md).

---

## 6. Risk acceptance

| Flagged risk | Severity | Verdict |
|---|---|---|
| `seed_watermarks` not covered by offline smoke (only inspection against t10_sat_smoke) | MEDIUM | ACCEPT. Operator always runs against live Postgres; the migration script prints an informative skip message if Postgres is unreachable. A follow-up smoke adding a live-DB case is a nice-to-have but not a blocker. Covered in operator runbook step 2 which explicitly runs with Postgres up. |
| Monolith kept post-migration doubles disk until step 5 | MEDIUM | ACCEPT. Documented in operator runbook preconditions #4 and step 5. Operator has agency to delete monoliths after row-count gate passes; running the delete pre-gate is explicitly forbidden. |
| Pre-rewind lock between steps 1 and 8 | MEDIUM | ACCEPT. Documented in operator runbook preconditions #5. Rewind.py already requires `--yes` for destructive ops; the lock is operator-enforced, not tool-enforced. A follow-up could add a "migration-in-progress" sentinel the rewind CLI refuses to pass through, but this is scope-expansion beyond T-13. |
| Orphan `.part.parquet.<pid>.tmp` files from a killed process | LOW | ACCEPT. The `.tmp` naming is invisible to `vault_io` globs and `reconcile.rglob("*.parquet")`. Dev-backend provided a one-liner: `Get-ChildItem data -Recurse -Filter '.part.parquet.*.tmp' | Remove-Item`. |
| hub_flight day-vs-file-month agreement (today 100%, future unknown) | LOW | ACCEPT with follow-up (R1b in §4 patch). A future violation is a bronze upstream bug and would surface as a `vault_io.row_count` row-count mismatch at step 4. The defensive check in `migrate_one` is a nice-to-have follow-up, not a blocker. |

---

## 7. Final verdict

**STATUS: APPROVED**

Preconditions for operator execution:
- Architecture.md section 6 patched per §4 above (orchestrator applies).
- ADR entry per §5 appended to `etl4/docs/decisions.md` (orchestrator appends).
- Operator runbook per §2 is binding; T-12.json operator_runbook section is superseded on conflict.

T-12 status flips to APPROVED with the operator runbook amendment above.

No code changes required in `etl4/scripts/migrate_partition_ym.py`. The script as-shipped correctly:
1. Migrates the 4 stream-rechunkable tables.
2. Refuses to silently migrate the 3 flagged tables.
3. Preserves monoliths pending operator verification.
4. Writes in-file `ym` + `_DONE` sentinel for schema compatibility with T-08.
5. Advances `pipeline_watermarks` in one commit per run.
6. Exits with the correct semantics for both dry-run and real-run across all tested cases.

The decision to resolve Gap 1 and Gap 2 via bronze rebuild (rather than script extension) is a runbook-layer decision, not a code-layer decision. The script's `TABLE_SPECS` dict could optionally be pruned to only the 4 migratable tables in a follow-up cleanup PR; today, keeping the 3 flagged entries with their `unsupported_reason` is actively useful because it makes the operator surface self-documenting.

---

*End of t13_migration_review.md.*
