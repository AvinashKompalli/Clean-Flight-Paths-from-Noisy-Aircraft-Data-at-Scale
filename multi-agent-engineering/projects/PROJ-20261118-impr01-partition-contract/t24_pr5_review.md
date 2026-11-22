# T-24 - Data-engineer review of PR5 bundle (T-20 + T-21 + T-22 + T-23)

**Reviewer:** data-engineer
**Date (UTC):** 2026-11-20
**Branch HEAD:** `feature/pipeline-3-avinash` @ `2145dbf`
**Commits in scope:**
- `a008205` - T-20 run_matrix.py --ym + SYN-002A sentinel (co-landed with T-08)
- `d90f60b` - T-21 rewind.py CLI (565 lines)
- `dc93e28` - T-22 run_e2e --ym + filesystem-watermark drift check
- `2129532` - T-23 idempotent stage_eval_mart + v_eval_mart_ym DDL

**Verdict:** **APPROVED** with 2 LOW-severity follow-up items (both pre-logged as IMPR items; neither blocks branch-to-main merge).

---

## A. `--ym` threading coherence across run_matrix + run_e2e

**Verdict: PASS.**

- `etl4/bench/run_matrix.py` validation (lines 139-148): `--source` in `{s3,zenodo,rest}` demands `--ym`, enforces `_YM_CALENDAR_RE = r"^[0-9]{4}-(0[1-9]|1[0-2])$"`. Synthetic path auto-defaults `--ym = SYN-002A` when `--ym` absent.
- `etl4/run_e2e.py` argparse validation (lines 407-423) reproduces the same contract **before** any DB/FS touch; it additionally gates `stage_sink` on a calendar YYYY-MM because `postgres_sink` rejects `SYN-*` under the DDL CHECK.
- `stage_matrix` (lines 202-213) threads `--ym`, `--source`, `--methods`, `--workers`, `--bronze` to `run_matrix.py`; `stage_sink` (line 195) threads `--ym` to `postgres_sink`. Both regexes match character-for-character.
- SYN-002A sentinel consistency verified at 4 sites: run_matrix `SYNTHETIC_YM_DEFAULT`, create_tables.sql backfill literal (line 162), stage_eval_mart max_ym filter (lines 335-339), and run_e2e `_YM_CALENDAR_RE`. All four reject the sentinel from the `pipeline_watermarks.last_ym` CHECK path while allowing it on `pipeline_runs.ym` (which has no CHECK).

## B. Rewind safety (T-21)

**Verdict: PASS with 1 LOW follow-up.**

- Chain order for `--layer all` is `eval_mart -> matrix -> sink -> marts -> vault` (rewind.py L531). Downstream-first matches architecture section 7 and overrides the dispatch's forward-order listing; rationale is correct (upstream rebuilds never leave orphans).
- `--yes` gate on `--layer all` enforced (L523-527).
- Filesystem delete runs BEFORE Postgres txn in `rewind_vault` / `rewind_marts` (L246-253, L301-308). A crash between leaves the watermark AHEAD of disk; T-22 drift check detects this. Reverse order (watermark first) would leave builder silently skipping real missing partitions - rightly avoided.
- `UPDATE ... WHERE last_ym >= from_ym` never forward-rolls (L194, L204). Re-running rewind is idempotent.
- `rewind_eval_mart` correctly does NOT delete `fact_experiment_result` rows (latest-per-cell shape); resettlement occurs on the next `stage_eval_mart` run after matrix rewind.
- **LOW follow-up: cross-month flight neighbour rule missing from CLI help text.** ADR 2026-11-20 (tail of decisions.md, under "Consequences" + "Open follow-up") states "rewind of month M MUST rewind M+1 too when either month contains a cross-month flight; T-21 CLI help text and a SQL detection snippet should surface this." `grep -n "cross-month|cross_month|neighbour|neighbor" etl4/orchestration/rewind.py` returns zero matches. ADR itself tags this as deferred and not required for current scale; defer as new IMPR item or accept a 1-line help-text patch pre-merge at the orchestrator's discretion. **Does NOT block merge**; the hazard is contained by (a) postgres_sink `ON CONFLICT (hk_flight) DO UPDATE SET lastseen=EXCLUDED.lastseen` forward-chronological rebuild, and (b) chronological-run convention.

## C. Drift-check correctness (T-22)

**Verdict: PASS with 1 LOW follow-up (known).**

- Three cases covered (run_e2e.py L96-130):
  1. fs dirs present, no watermark row -> SystemExit with rewind hint (L104-109).
  2. watermark row present, zero fs dirs -> SystemExit with `--layer vault` hint carrying the exact ym range (L112-119).
  3. Contiguous-range hole or extras past last_ym -> SystemExit enumerating missing/extra (L121-130).
- `_YM_CALENDAR_RE` filter on `fs_yms_all` correctly excludes any SYN-* dir that could land on disk under a future contract (future-proof).
- Pre-flight ordering (after infra+schema, before every other stage): correct - avoids UndefinedTable crash on fresh DB. `--skip-drift-check` escape hatch available.
- Broad `except Exception` on L133-142 wraps real failures (UndefinedTable, OperationalError) into actionable SystemExit messages. Acceptable; T-22 handoff documents the surface.
- No "fast-forward watermark" or "rebuild from fs" modes exist - the dispatch question wording is misleading. Implementation is ALWAYS abort-and-suggest-rewind, which matches architecture R4 (check only, no auto-repair). Confirmed with architect intent in handoffs.
- **LOW follow-up: `sat_state_observation` is excluded from `VAULT_PARTITIONED_TABLES` tuple (run_e2e.py L53-54).** vault_io.PARTITIONED_TABLES includes it; the drift check omits it per dispatch spec. Drift on the 333M-row table would go undetected until the next vault stage runs. Not a blocker: vault runs validate on open; row-count reconciliation (`qa/reconcile.py`) still catches post-build inconsistency. Log as IMPR item to extend the drift check to sat_state_observation when the 20 GB table is rolled under the partition contract.

## D. eval_mart ym threading (T-23)

**Verdict: PASS.**

- `fact_experiment_result` grows from 18 to 19 cols (adds `last_ym TEXT`). Decisions.md tail notes the 18-col ADR amendment follow-up; T-23 handoff flags that it did not edit decisions.md (out of task scope). **No blocker**: an ADR amendment is already queued in architecture.md section 9.3 bullet 4.
- `v_eval_mart_ym` DDL present in both run_e2e.py (L297-317) and create_tables.sql (L207-232). Definitions byte-match so `CREATE OR REPLACE VIEW` from either side never trips "cannot change name/type of view column". The create_tables.sql copy is guarded by a DO block on `information_schema.columns` so applying on a fresh DB where stage_eval_mart has not yet run does not raise.
- Watermark set in same psycopg transaction as DDL + COPY + view-recreate (L344-361). On crash, both roll back. Confirmed PG DDL-is-transactional; readers see either the old mart or the new one.
- max_ym calendar filter (L334-339): `[y for y in df.last_ym.dropna().astype(str) if _YM_CALENDAR_RE.match(y)]` correctly excludes SYN-002A / SYN-TEST before `watermark.set`, matching the CHECK regex.
- **Dashboard-schema audit for breakage:**
  - `etl4/dashboard/06_de_pipeline.json`: queries rmse_m + wall_secs + method + noise by name. Non-breaking.
  - `etl4/dashboard/07_analyst_view.json`: queries noise + method + continuity + n_flights by name. Non-breaking.
  - `etl4/dashboard/08_accuracy_cost.json`: queries method + noise + seg_f1 + continuity + rmse_m + wall_secs by name. Non-breaking.
  - `grep SELECT \* FROM fact_experiment_result etl4/dashboard/` returns zero matches. All three dashboards tolerate the 19th column.
  - Views `v_pareto_frontier` and `v_eval_mart_ym` recreated on every stage_eval_mart invocation; not frozen against the 18-col shape.

## E. Transactional safety across the bundle

**Verdict: PASS.**

- **T-20 run_matrix:** `_record()` calls `write_run()` per cell inside its own txn; `_advance_matrix_watermark()` runs AFTER the full matrix loop in a separate short-lived connection. Can a crash between cell N committed and watermark advance leave disagreement? Yes - but the rule is monotone: watermark advance means "all cells for ym=X committed". Watermark behind = next re-run is per-cell DO NOTHING + eventual advance. Idempotent and recoverable. Acceptable design choice.
- **T-21 rewind:** Filesystem delete BEFORE Postgres txn. Crash between leaves watermark ahead of disk; T-22 catches it; operator re-runs rewind. Documented and safe.
- **T-22 drift:** Read-only; no transactional concern.
- **T-23 stage_eval_mart:** Single psycopg transaction wraps DROP/CREATE/COPY/view-recreate/watermark.set. On crash, all roll back together; mart stays on prior committed snapshot. No stale-view window.
- **No residual gap** where `pipeline_runs` is committed without a matching `pipeline_watermarks` entry WITHIN the same ym: the watermark semantics are "represents ym with ALL cells committed"; partial state is tolerated, idempotent, and self-healing on re-run.

## F. Test coverage

**Verdict: PASS.** `python -m pytest etl4/tests/ -q` at branch HEAD `2145dbf` reports **92 passed in 9.42s** (OpenSky env). Zero regressions from the four PR5 commits. T-27 (functional rewind E2E) is scheduled separately per architecture section 5.

---

## Severity triage

| ID | Finding | Section | Severity | Action |
|---|---|---|---|---|
| F1 | T-21 CLI help text does not mention the cross-month flight neighbour rewind rule (ADR 2026-11-20 follow-up) | B | LOW | Defer as IMPR item or accept a 1-line patch pre-merge |
| F2 | Drift check `VAULT_PARTITIONED_TABLES` omits `sat_state_observation` | C | LOW | Defer as IMPR item; extend drift check when partitioning is rolled to sat_state_observation at scale |
| F3 | `fact_experiment_result` ADR amendment (18 -> 19 cols) not yet written to decisions.md | D | INFO | Orchestrator homework; pre-logged in architecture.md section 9.3 bullet 4 |

Zero HIGH-severity findings. Zero MEDIUM-severity findings. Zero blockers.

## Commands run

- `python -c "import ast; ast.parse(open('etl4/orchestration/rewind.py').read()); ast.parse(open('etl4/run_e2e.py').read()); ast.parse(open('etl4/bench/run_matrix.py').read()); print('SYNTAX_OK')"` -> `SYNTAX_OK` (exit 0)
- `python -m pytest etl4/tests/ -q` -> `92 passed in 9.42s` (exit 0)
- `grep -n "cross-month|cross_month|neighbour|neighbor" etl4/orchestration/rewind.py` -> zero matches
- `grep "SELECT \* FROM fact_experiment_result" etl4/dashboard/*.json` -> zero matches
- `git log --stat 2129532 dc93e28 d90f60b a008205 2145dbf` - commit payloads inspected

## Blockers for branch-to-main merge

**None.**

## STATUS

**APPROVED** - PR5 bundle (T-20 + T-21 + T-22 + T-23) is coherent, transactionally safe, and non-breaking to downstream dashboards. Two LOW follow-up IMPR items noted; neither blocks the merge.

---

*End of t24_pr5_review.md*
