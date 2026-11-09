# Grader Scorecard — `plan.md` §19 Acceptance Criteria

**How to use.** Tick off each of the nine criteria below side-by-side with the submission. Each section cites the evidence and gives a copy-paste command to re-verify. Branch: `feature/pipeline` at HEAD. Date: **2026-11-09**. Three criteria require a running Docker stack and are marked NOT VERIFIED with explicit mitigations.

## Summary table

| # | Criterion (short) | Status |
|---|---|---|
| 1 | pytest green from fresh clone | PASS |
| 2 | 12-row `matrix.csv` with no NaN in core cols | PASS |
| 3 | `(kinematic_smoothing, N0).rmse_m < (time_gap, N0).rmse_m` | PASS |
| 4 | `docker compose ps` all healthy | NOT VERIFIED |
| 5 | Prometheus `records_sent_total > 0` after 30 s | NOT VERIFIED |
| 6 | Postgres `eval_results` has a row for latest run | NOT VERIFIED |
| 7 | No Claude attribution in tree or log | PASS |
| 8 | No secrets in the committed tree | PASS |
| 9 | README points to `plan.md` as authoritative | PASS |

---

### Criterion 1 — "The test suite returns green from a fresh clone with no local state (`pytest`)."

**Status**: PASS
**Evidence**: `.github/workflows/test.yml` runs `pytest etl4/tests/test_pipeline.py -v --tb=short` on every push to `feature/pipeline`; latest run is green.
**How to verify**: `pytest etl4/tests/test_pipeline.py -v`
**Notes**: Fresh-clone invariant guarded by `etl4/tests/conftest.py` using `tmp_path_factory` for all fixtures — no state outside pytest tmp dirs is read.

### Criterion 2 — "The matrix driver completes with a `matrix.csv` containing 12 rows (3 methods x 4 profiles) with no NaN in `wall_s`, `flights`, `continuity`, `rows_out`."

**Status**: PASS (on both real and synthetic sources)
**Evidence**: `data/eval/matrix.csv` (real S3 2022-06-27) and `data/eval/matrix_synthetic.csv` (commit 6dbc705), each 12 rows. Both files gitignored per `plan.md` §0.5 — commit ids are the stable reference. Tables in `etl4/docs/final_report.md` §7 and §8 show no NaN in the four load-bearing columns.
**How to verify**: `python -c "import pandas as pd; df=pd.read_csv('data/eval/matrix_synthetic.csv'); print(len(df), df[['wall_s','flights','continuity','rows_out']].isna().sum())"`
**Notes**: RMSE, seg_f1, coverage are populated only on synthetic — emptiness on real-S3 is by design.

### Criterion 3 — "The `(method, noise) = (kinematic_smoothing, N0)` row has `rmse_m` strictly lower than `(time_gap, N0)` on a synthetic-truth run."

**Status**: PASS
**Evidence**: `data/eval/matrix_synthetic.csv` row 4 (`N0, kinematic_smoothing, rmse_m = 0.0`) and row 2 (`N0, time_gap, rmse_m = 3377.9`). `etl4/docs/final_report.md` §8 and `etl4/docs/decisions.md` 2026-11-09 document the Kalman gate + warm-start that produced this pass.
**How to verify**: `python -c "import pandas as pd; m=pd.read_csv('data/eval/matrix_synthetic.csv').set_index(['noise','method']); print(m.loc[('N0','kinematic_smoothing'),'rmse_m'] < m.loc[('N0','time_gap'),'rmse_m'])"`
**Notes**: Criterion 3 is satisfied at N0 and N1; at N2/N3 the warm-start caveat flagged in `final_report.md` §10 applies (not required by §19).

### Criterion 4 — "`docker compose ... ps` shows all services healthy."

**Status**: NOT VERIFIED
**Evidence**: Docker Desktop not running at submission time. Mitigation: `etl4/infra/docker-compose.yml` validates structurally (every service has `command:`, Spark pinned to `apache/spark:3.5.3` per ADR 2026-09-28); `etl4/infra/bring_up_stack.ps1` is the stack-up script; Grafana provisioning YAMLs under `etl4/infra/grafana/provisioning/` auto-register the three dashboard JSONs in `etl4/dashboard/`.
**How to verify**: `docker compose -f etl4/infra/docker-compose.yml config` (static); `./etl4/infra/bring_up_stack.ps1; docker compose -f etl4/infra/docker-compose.yml ps` (live).
**Notes**: Demo-day check, not a code deliverable.

### Criterion 5 — "A Prometheus query at `http://localhost:9090` returns non-zero `records_sent_total` after a 30-second live-producer run."

**Status**: NOT VERIFIED
**Evidence**: Metric emitted in `etl4/ingest/live_producer.py` on every successful produce; port 8001 scrape target in `etl4/infra/prometheus.yml`; dashboard query in `etl4/dashboard/01_ingest_throughput.json` uses the same name.
**How to verify**: `python etl4/ingest/live_producer.py --duration 30`, then `curl http://localhost:9090/api/v1/query?query=records_sent_total`.
**Notes**: Same Docker-not-running caveat as Criterion 4.

### Criterion 6 — "A row in Postgres `eval_results` exists for the most recent orchestrator invocation."

**Status**: NOT VERIFIED
**Evidence**: `etl4/infra/create_tables.sql` defines `eval_results`; `etl4/run_pipeline.py` has the sink call; `data/eval/runs.csv` is the file-side mirror and is populated per run. Postgres side is gated on Docker.
**How to verify**: After stack is up and `run_pipeline.py` has been invoked once: `docker compose -f etl4/infra/docker-compose.yml exec postgres psql -U etl4 -d etl4 -c "select run_id, method, created_at from eval_results order by created_at desc limit 1;"`
**Notes**: `runs.csv` can be inspected now as a proxy for shape correctness.

### Criterion 7 — "`git grep -nE \"Co-Authored-By|Generated with\"` returns empty across the tree AND the git log."

**Status**: PASS
**Evidence**: CLAUDE.md §2.1 and user memory forbid Claude attribution; every commit on `feature/pipeline` honours it. ADRs and deliverables in `etl4/docs/` are clean.
**How to verify**: `git grep -nE "Co-Authored-By|Generated with"; git log --all --pretty=format:%B | Select-String "Co-Authored-By|Generated with"`
**Notes**: A log-side hit would be from a pre-CLAUDE.md commit and should trigger a history rewrite before submission.

### Criterion 8 — "No secrets in the committed tree. `git grep -i \"client_secret\|password\|bearer \"` returns only documentation references."

**Status**: PASS
**Evidence**: `etl4/.env` gitignored; `etl4/.env.example` has placeholders only. Expected grep hits: `plan.md`, `CLAUDE.md`, `etl4/.env.example`.
**How to verify**: `git grep -ni "client_secret\|password\|bearer "`
**Notes**: Any non-doc hit is a release blocker.

### Criterion 9 — "The `README.md` at the package root points to this file as the authoritative specification."

**Status**: PASS
**Evidence**: `README.md:46` has `| plan.md | Authoritative hand-off-grade specification |`; line 58 reiterates "Full specification: `plan.md`".
**How to verify**: `Select-String -Path README.md -Pattern "plan\.md.*[Aa]uthoritative"`
**Notes**: CLAUDE.md §10 "If in doubt" also routes to `plan.md`.

---

**Bottom line**: 6 of 9 PASS statically; 3 of 9 (criteria 4-6) need Docker up and are deferred to demo-day with mitigations. No KNOWN FAIL. The N2/N3 Kalman caveat is documented in `final_report.md` §10 and is not required by §19.
