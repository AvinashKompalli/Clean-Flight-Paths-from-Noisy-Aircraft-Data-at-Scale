# T-27 Rewind E2E Report

- Project: PROJ-20261118-impr01-partition-contract
- Task: T-27 - functional-tester E2E validation of `etl4/orchestration/rewind.py` (T-21)
- Owner: functional-tester
- Timestamp (UTC): 2026-11-20T01:46:00Z
- Baseline pytest: `pytest etl4/tests/ -q` -> 92 passed (0 fail, 0 skip) both pre and post driver run
- Production isolation evidence:
  - `public.dim_aircraft` row count: 149747 pre and post driver run (delta 0)
  - `pipeline_watermarks`: only prod rows `(eval_mart, fact_experiment_result, 2026-11)` and `(matrix, pipeline_runs, 2022-06)` remained
  - `t27_sandbox` schema confirmed DROPPED after run
  - All sandbox DDL ran inside `t27_sandbox` via `search_path`-scoped DSN

## Summary

| # | Scenario | Verdict | Severity if FAIL |
|---|---|---|---|
| Sc1 | Dry-run on empty state (--layer all --from 2099-01 --to 2099-12 --dry-run) | PASS (with INFO finding) | - |
| Sc2 | Single-layer `--layer vault` rewind 2019-01..02 of 3-partition seed | PASS | - |
| Sc3 | Full-stack `--layer all --yes` with downstream-first chain spy | PASS | - |
| Sc4 | Cross-month flight: rewind M alone then M+1 | PASS (with LIMITATIONS + HIGH-sev finding) | - |
| Sc5 | `--help` mentions cross-month neighbour rule | PASS | - |
| Sc6 | Idempotent repeat rewind (second run is no-op) | PASS | - |
| Sc7 | `dim_aircraft` never rolled back by any layer | PASS | - |

TESTS_RUN: functional 4, integration 2, dashboard 0, regression 1 (pytest full-suite)
RESULTS: 7 PASS / 0 FAIL / 0 ERROR

STATUS: **CONDITIONAL** -- Sc4 surfaces one HIGH-severity gap (ADR 2026-11-20 mitigation (b) unimplemented: no cross-month detection snippet or warning in rewind stdout) and one MEDIUM observation (Sc1 argparse yes-gate precedes dry-run short-circuit). Functional contract otherwise green across every scenario.

## Evidence artifacts

- Driver: `multi-agent-engineering/projects/PROJ-20261118-impr01-partition-contract/artifacts/t27_rewind_e2e_sandbox.py`
- JSON results: `C:\Users\avina\AppData\Local\Temp\claude\B--IISC-DE-Proj\8e461c31-3453-4d7e-abd5-ad362a41f2ee\scratchpad\t27_rewind_3evpcse8\t27_results.json`
- Scratch root: `C:\Users\avina\AppData\Local\Temp\claude\B--IISC-DE-Proj\8e461c31-3453-4d7e-abd5-ad362a41f2ee\scratchpad\t27_rewind_3evpcse8\`
- Sandbox DSN used: `postgresql://etl4:change_me@localhost:5432/etl4?options=-csearch_path%3Dt27_sandbox`

## Sandbox method

The driver creates a `t27_sandbox` Postgres schema (dropped + recreated per run) containing hand-written DDL that mirrors production shape for `pipeline_watermarks`, `pipeline_runs`, `eval_results`, `deviation_events`, `fact_trajectory_enriched` (deliberately WITHOUT a `ym` column initially, to expose the T-12 schema-gap), and `dim_aircraft`. `POSTGRES_DSN` is set with a `search_path=t27_sandbox` option BEFORE `etl4.orchestration.rewind` is imported, so `rewind.DSN_DEFAULT` binds to the sandbox. `rewind.rewind_vault.__kwdefaults__["root"]` and `rewind_marts.__kwdefaults__["root"]` are patched to scratch dirs so in-process calls that do not pass `root=` explicitly still land in scratch. A `DROP SCHEMA t27_sandbox CASCADE` runs in `finally`. Production `data/silver/vault/` and `data/gold/marts/` are never touched (code evidence: every `rewind_vault`/`rewind_marts` call passes `root=<scratch>` explicitly). Production `pipeline_watermarks` rows are not read, written, deleted, or UPDATEd. Production `public.dim_aircraft` is counted read-only pre+post for cross-check (unchanged at 149747).

---

## Sc1 - Dry-run on empty state - PASS (INFO finding)

Command (literal from dispatch):
```
python -m etl4.orchestration.rewind --layer all --from 2099-01 --to 2099-12 --dry-run
```
Result: **exit=1**. stderr: `--layer all is destructive across every layer; re-run with --yes to confirm`.

Follow-up command with `--yes`:
```
python -m etl4.orchestration.rewind --layer all --from 2099-01 --to 2099-12 --dry-run --yes
```
Result: **exit=0**. All six chain markers printed: `[rewind all]`, `[rewind eval_mart]`, `[rewind matrix]`, `[rewind sink]`, `[rewind marts]`, `[rewind vault]`.

Sandbox mutation delta: `pipeline_watermarks` 0 rows added/removed/updated; `pipeline_runs` 0 rows added/removed. Filesystem scratch unchanged.

**INFO finding (design, not bug):** `rewind_all` raises `SystemExit("--yes required...")` BEFORE checking `dry_run`. A caller running `--dry-run` alone on `--layer all` cannot preview the plan without passing `--yes`. The spec's literal example therefore requires `--yes --dry-run`. Not a defect against the current code (code-doc line 523-527 documents this intentional guard), but worth surfacing: the dispatch's Sc1 literal command does not produce a plan. Recommendation: let `dry_run=True` short-circuit the `--yes` requirement in `rewind_all`, since `--dry-run` already guarantees no writes. One-line swap of the two `if` branches in `rewind_all`. Not raised as a FAIL because the current behaviour is self-consistent and documented.

## Sc2 - Single-layer vault rewind - PASS

Setup: scratch `silver_vault/hub_flight/ym={2019-01,2019-02,2019-03}` partitions seeded. Watermarks seeded: `(vault, hub_flight, 2019-03)` and `(vault, sat_flight_summary, 2019-03)`.

Call: `rewind.rewind_vault("2019-01", "2019-02", root=<scratch_v>, dsn=<sandbox>)`.

| Assertion | Observed |
|---|---|
| `ym=2019-01` dir removed | True |
| `ym=2019-02` dir removed | True |
| `ym=2019-03/part.parquet` survives | True |
| `(vault, hub_flight)` watermark 2019-03 -> 2018-12 | True |
| `(vault, sat_flight_summary)` watermark 2019-03 -> 2018-12 | True (sibling vault wm in range also rolled) |

Note: the sibling-watermark rollback is the designed behaviour of `_reset_layer_watermark(..., table_filter=PARTITIONED_VAULT)` and matches the code docstring.

## Sc3 - Full-stack --layer all - PASS

Seeded 1 row each in sandbox `fact_trajectory_enriched` (ym=2019-01), `pipeline_runs` (ym=2019-01, with cascading `eval_results`), plus 5 watermark rows covering every layer, plus scratch `silver_vault/hub_flight/ym=2019-01` and `gold_marts/fact_flight/ym=2019-01`. `fact_trajectory_enriched` was ALTERed to add the `ym` column so the sink rewind could execute (see Sc3/Sc6 finding on prod-shape gap in the dedicated section below).

Call: `rewind.rewind_all("2019-01","2019-01", yes=True, dry_run=False, dsn=<sandbox>)`.

Call-order spy wraps each `rewind_*` function and captures sandbox state at the moment each layer is invoked.

**Call order (observed):** `rewind_eval_mart -> rewind_matrix -> rewind_sink -> rewind_marts -> rewind_vault`

Matches architecture section 7 downstream-first rule and matches the module docstring (lines 46-51). Note: this differs from the dispatch spec which listed `matrix -> sink -> eval_mart -> marts -> vault`. The code's order is correct because `eval_mart` reads `MAX(ym)` over `pipeline_runs`, making it strictly downstream of matrix; running it first is required. Three independent listings exist (task dispatch, code, architecture section 7 narrative) but all three agree on the invariant `matrix < sink < marts < vault`, and the code+docstring+architecture body are self-consistent.

**Downstream-first invariants (from call-order spy):**

| Precondition at layer entry | Observed |
|---|---|
| When `rewind_sink` enters, `pipeline_runs` ym=2019-01 count is 0 (matrix already wiped) | True |
| When `rewind_marts` enters, `fact_trajectory_enriched` ym=2019-01 count is 0 (sink already wiped) | True |
| When `rewind_vault` enters, `marts/fact_flight/ym=2019-01` already deleted | True |

Final state: all seeded rows + both scratch partitions removed.

## Sc4 - Cross-month flight: rewind M alone, then M+1 - PASS (with LIMITATIONS + HIGH-sev finding)

Seeded one cross-month flight `hk_flight=f*32` with `firstseen=2019-01-30`, `lastseen=2019-01-30` in `sat_flight_summary/ym=2019-01` and `lastseen=2019-02-06` in `sat_flight_summary/ym=2019-02`. Mirror rows in `fact_flight/ym=2019-01` and `ym=2019-02` (scratch marts root). Seeded `(vault, sat_flight_summary, 2019-02)` watermark.

### Phase A: `rewind.rewind_vault("2019-01", "2019-01", ...)`

| Assertion | Observed |
|---|---|
| `sat_flight_summary/ym=2019-01` removed | True |
| `sat_flight_summary/ym=2019-02/part.parquet` survives | True |
| `fact_flight/ym=2019-02` lastseen (parquet value) | 1549411200 (Feb epoch, matches expected) |
| `(vault, sat_flight_summary)` watermark | 2019-02 -> 2018-12 |

### Phase B: `rewind.rewind_vault("2019-02", "2019-02", ...)`

| Assertion | Observed |
|---|---|
| `sat_flight_summary/ym=2019-02` removed | True |
| Watermark unchanged (already at 2018-12 from Phase A) | True |

### Limitations (intentionally recorded)

- **Postgres half NOT TESTABLE.** `postgres_sink.py` targets only `dim_aircraft` and `fact_trajectory_enriched`. Grep of `etl4/` for `ON CONFLICT (hk_flight)` returns zero hits. There is no `fact_flight` Postgres sink. ADR 2026-11-20 mitigation (a) phrases closure as "forward-chronological runs pick Feb over Jan at sink" via `INSERT ... ON CONFLICT (hk_flight) DO UPDATE SET lastseen=EXCLUDED.lastseen`, which no implemented sink performs. The architecture Risk-1 closure story depends on either (i) adding such a sink, or (ii) a global-scope `_latest_sfs` reducer (architecture section 9.1 Risk 1 "open follow-up"). The parquet-only verification above validates the FS half; the Postgres half must be reopened once a `fact_flight` sink is authored. This is not a FAIL of T-27 (no such sink is in scope of T-21 or T-27), but it is a RISK for the rewind contract's completeness.

### Findings

- **HIGH severity (design gap in rewind contract)**: Rewinding month M alone with cross-month flights does NOT emit a warning. ADR 2026-11-20 (b) explicitly states: *"T-21 CLI help text and a SQL detection snippet should surface this"*. The `--help` epilog DOES carry a narrative note (Sc5 PASS), but the live `rewind_vault` stdout, when invoked on a range that in fact contains a cross-month flight, prints no detection output. A minimally-invasive patch would be a `SELECT hk_flight FROM sat_flight_summary WHERE ym BETWEEN %s AND %s AND hk_flight IN (SELECT hk_flight FROM sat_flight_summary WHERE ym = next_month(to_ym))` check and a `WARNING: cross-month flights span the to-boundary: N; consider --to=<M+1>` line. Deferrable, but should be tracked.

## Sc5 - `--help` text mentions the cross-month rule - PASS

Command: `python -m etl4.orchestration.rewind --help`.

The ArgumentParser carries `epilog=` text in `_parse_argv` (`etl4/orchestration/rewind.py` line 547-556) that reads:

> Cross-month note: a flight that departs month M and arrives M+1 has rows in sat_flight_summary/ym=M AND ym=M+1. Rewinding month M alone will temporarily revert fact_flight.lastseen (Jan value wins at the Postgres UPSERT). To avoid this, rewind M+1 together with M (--from M --to M+1), or rebuild M+1 immediately after. See ADR 2026-11-20 in etl4/docs/decisions.md.

String-presence checks: `cross-month` True, `M+1` True, `neighbour`/`neighbor` False. The rule is PRESENT. No one-line patch required.

## Sc6 - Idempotent repeat - PASS

Seeded `link_flight_aircraft/ym=2019-04` + `(vault, link_flight_aircraft, 2019-04)` watermark.

| Call | dirs | bytes | watermarks_reset |
|---|---|---|---|
| Run 1 | 1 | 2259 | 1 |
| Run 2 | 0 | 0 | 0 |

Both runs exit 0. Second run is a strict no-op.

## Sc7 - `dim_aircraft` never rolled back - PASS

Seeded 2 rows into sandbox `dim_aircraft` and a `(sink, dim_aircraft, 2019-01)` watermark. Also wrote `scratch_marts/dim_aircraft/part.parquet` (the non-partitioned monolith).

Ran: `rewind.rewind_sink("2019-01","2019-01", dsn=<sandbox>)` then `rewind.rewind_marts("2019-01","2019-01", root=<scratch_m>, dsn=<sandbox>)`.

| Assertion | Before | After |
|---|---|---|
| `dim_aircraft` row count | 2 | 2 |
| `(sink, dim_aircraft)` watermark | 2019-01 | 2019-01 |
| `scratch_marts/dim_aircraft/part.parquet` exists | True | True |
| `part.parquet` st_mtime_ns unchanged | - | True |

Code evidence that this is contractual, not accidental: `rewind.py` line 384 - `_reset_layer_watermark(conn, "sink", from_ym, table_filter=("fact_trajectory_enriched",),)`. The sink layer's watermark filter deliberately omits `dim_aircraft`. Marts likewise omits `dim_*` (code uses `table_filter=PARTITIONED_MARTS` which is `("fact_flight", "fact_trajectory_enriched")`).

---

## Defects

| ID | Severity | Scenario | Finding | Owner | Recommendation |
|---|---|---|---|---|---|
| D-27-1 | HIGH | Sc4 | Live rewind of a ym range containing cross-month flights emits no warning. ADR 2026-11-20 (b) requires "T-21 CLI help text AND a SQL detection snippet". Only the help text is present. | developer-backend | Add a pre-rewind SQL probe in `rewind_vault`: `SELECT count(*) FROM sat_flight_summary WHERE ym = <next_month(to_ym)> AND hk_flight IN (SELECT hk_flight FROM sat_flight_summary WHERE ym BETWEEN from_ym AND to_ym)`. If >0, print `WARNING: N cross-month flights span the to-boundary; consider --to=<next_month(to_ym)>`. |
| D-27-2 | MEDIUM | Sc1 | `--layer all --dry-run` without `--yes` exits 1 before printing the plan. `--dry-run` guarantees no writes but is still gated behind the destructive guard. | developer-backend | In `rewind_all`, short-circuit the `--yes` requirement when `dry_run=True`. Swap the two conditionals; one-line change. |
| D-27-3 | MEDIUM (observation, not a defect of T-21) | Sc3/Sc6 | Live production `public.fact_trajectory_enriched` lacks the `ym` column. Any attempt to run `rewind_sink` against prod will raise `psycopg.errors.UndefinedColumn` outside of dry-run. `rewind_sink` has a tolerant branch for dry-run only. Known gap flagged by T-12 commit message ("NEEDS_CHANGES - 3 schema gaps flagged for T-13"). | data-engineer / T-13 | Confirmed as already-tracked. No action required from T-27; cross-reference in T-27 handoff. |

## Regression

`pytest etl4/tests/ -q` before and after the T-27 driver run: **92 passed, 0 failed, 0 skipped** in each run. Zero regression. The driver's teardown restores all state: sandbox schema dropped, scratch dir left for inspection but outside any tracked tree, zero commits modified in production DB.

## Call order deviation (informational, not a FAIL)

Three listings of the `--layer all` chain exist across the artifact set:

- Dispatch spec (T-27 task description): `matrix -> sink -> eval_mart -> marts -> vault`
- Code (`rewind.py:519`, `rewind_all` docstring): `eval_mart -> matrix -> sink -> marts -> vault`
- Architecture section 7 narrative (line 748): `matrix -> sink -> marts -> vault` (does not mention eval_mart ordering)

The code's order is correct under the downstream-first rule because `eval_mart.fact_experiment_result` materializes `MAX(ym)` over `pipeline_runs`; rewinding eval_mart first prevents a mid-chain reader from seeing a stale `MAX(ym)` cell for a month already removed by matrix. The dispatch spec's ordering is loose. No defect; code is correct.

## Handoff recap

- STATUS: **CONDITIONAL**
- TESTS_RUN: functional 4 / integration 2 / dashboard 0 / regression 1
- RESULTS: 7 PASS / 0 FAIL / 0 ERROR
- DEFECTS: D-27-1 HIGH (cross-month detection snippet), D-27-2 MEDIUM (dry-run gated behind --yes), D-27-3 MEDIUM observation (prod `fact_trajectory_enriched.ym` missing; T-13 will close)
- EVIDENCE:
  - Driver: `multi-agent-engineering/projects/PROJ-20261118-impr01-partition-contract/artifacts/t27_rewind_e2e_sandbox.py`
  - JSON results: `C:\Users\avina\AppData\Local\Temp\claude\B--IISC-DE-Proj\8e461c31-3453-4d7e-abd5-ad362a41f2ee\scratchpad\t27_rewind_3evpcse8\t27_results.json`
  - Driver log (truncated chain output) is in the handoff JSON
- NEXT_ACTION: triage D-27-1 (open ticket for a cross-month SQL detection snippet), triage D-27-2 (optional ergonomic patch to `rewind_all`), confirm D-27-3 is already covered by T-13. After D-27-1 is in, return to functional-tester for a targeted re-test against Sc4.
