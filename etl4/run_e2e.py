"""End-to-end pipeline runner.

Chains every stage from fresh Zenodo ingest to the final `fact_experiment_result`
mart in Postgres, so one command reproduces the whole showcase.

Stages, in order:
   1. infra        docker compose up (postgres, kafka, grafana, prometheus, ...)
   2. schema       create_tables.sql
   3. ingest       Zenodo pull via orchestration/ingest_runner  (telemetry -> ingest_runs)
   4. bronze       bronze_flightlists conversion
   5. vault        raw Data Vault 2.0 build
   6. marts        dim_aircraft, dim_airport, fact_flight, fact_trajectory_enriched
   7. qa           Great Expectations suites against vault/marts  (non-zero exit on failure)
   8. sink         COPY marts into Postgres (per-ym, via --ym propagation)
   9. reconcile    row-count reconciliation bronze -> vault -> marts  (gates matrix)
  10. matrix       3-method x N-noise reconstruction matrix  (telemetry -> pipeline_runs + eval_results)
  11. eval_mart    fact_experiment_result mart (pivot of pipeline_runs x eval_results)

Usage:
  python etl4/run_e2e.py --ym 2022-06                  # everything for one month
  python etl4/run_e2e.py --skip infra schema --ym 2022-06
  python etl4/run_e2e.py --source synthetic --only matrix --methods time_gap
  python etl4/run_e2e.py --source s3 --only sink --ym 2022-06
  python etl4/run_e2e.py --source live --live-limit 2000 --only ingest  # legacy live REST stream

--ym is required when --source is one of s3|zenodo|rest; synthetic defaults
to SYN-002A per architecture.md section 2.3.
"""
from __future__ import annotations

import argparse
import io
import re
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PY = sys.executable

sys.path.insert(0, str(ROOT))

from etl4.vault import vault_io, watermark

STAGES = ["infra", "schema", "ingest", "bronze", "vault",
          "marts", "qa", "sink", "reconcile", "matrix", "eval_mart"]

REAL_SOURCES = ("s3", "zenodo", "rest")
MATRIX_SOURCES = ("s3", "zenodo", "rest", "synthetic")
INGEST_SOURCES = ("zenodo", "live")

VAULT_PARTITIONED_TABLES = ("hub_flight", "link_flight_aircraft",
                            "link_flight_route", "sat_flight_summary")

_YM_CALENDAR_RE = re.compile(r"^[0-9]{4}-(0[1-9]|1[0-2])$")


def _months_between(lo: str, hi: str) -> set:
    """Set of 'YYYY-MM' strings from lo to hi inclusive (calendar months).

    Local to this module so the pre-flight check does not take a dependency
    on etl4.orchestration.rewind (parallel sibling task T-21).
    """
    y0, m0 = int(lo[:4]), int(lo[5:7])
    y1, m1 = int(hi[:4]), int(hi[5:7])
    out = set()
    y, m = y0, m0
    while (y, m) <= (y1, m1):
        out.add("%04d-%02d" % (y, m))
        m += 1
        if m == 13:
            m = 1
            y += 1
    return out


def check_watermark_vs_filesystem(root: Path = vault_io.SILVER_VAULT_ROOT) -> None:
    """Abort on watermark-vs-filesystem drift for the 4 partitioned vault tables.

    Flags three states:
      1. Watermark row present but filesystem is empty -> sink/matrix would
         re-skip already-committed months on disk that got scrubbed.
      2. Filesystem has ym dirs but no watermark row -> next vault run would
         bootstrap and reprocess every month.
      3. Watermark present; filesystem has a non-contiguous range or extra
         dirs past last_ym -> ADR 2026-11-13 Risk 16 (holes).

    Calendar yms only (watermark DDL CHECK rejects SYN-*). Raises SystemExit
    with a rewind hint when drift is detected.

    Does NOT swallow OperationalError: Postgres being down pre-stage_infra
    is a different failure mode. Caller orders the call after infra+schema
    when those stages are in the run set.
    """
    try:
        for table in VAULT_PARTITIONED_TABLES:
            fs_yms_all = set(vault_io.list_ym(table, root=root))
            fs_yms = {y for y in fs_yms_all if _YM_CALENDAR_RE.match(y)}
            db_last = watermark.get("vault", table)

            if db_last is None:
                if fs_yms:
                    raise SystemExit(
                        "[run_e2e] watermark drift: table=%s has %d ym dirs "
                        "on disk but no watermark row; run "
                        "`python -m etl4.orchestration.rewind` to reconcile"
                        % (table, len(fs_yms))
                    )
                continue

            if not fs_yms:
                raise SystemExit(
                    "[run_e2e] watermark drift: table=%s has watermark "
                    "last_ym=%s but zero ym dirs on disk; run "
                    "`python -m etl4.orchestration.rewind --layer vault "
                    "--from %s --to %s --yes` to reconcile"
                    % (table, db_last, db_last, db_last)
                )

            expected = _months_between(min(fs_yms), db_last)
            missing = expected - fs_yms
            extra = fs_yms - expected
            if missing or extra:
                raise SystemExit(
                    "[run_e2e] watermark drift on %s: "
                    "missing=%s extra=%s; run "
                    "`python -m etl4.orchestration.rewind` to reconcile"
                    % (table, sorted(missing)[:5], sorted(extra)[:5])
                )
    except SystemExit:
        raise
    except Exception as e:
        # psycopg.errors.UndefinedTable before stage_schema has run, or
        # OperationalError before stage_infra. Caller is expected to order
        # this check after those stages; surface the real error so the
        # operator sees the failure mode and does not get a silent pass.
        raise SystemExit(
            "[run_e2e] pre-flight drift check failed: %s: %s; "
            "ensure infra+schema ran first, or pass --skip-drift-check to bypass"
            % (type(e).__name__, e)
        ) from e


def _run(name: str, cmd: list[str], shell: bool = False) -> float:
    print(f"\n=== {name} ===", flush=True)
    print("exec:", " ".join(cmd) if not shell else cmd, flush=True)
    t0 = time.perf_counter()
    rc = subprocess.call(cmd, cwd=str(ROOT), shell=shell)
    wall = time.perf_counter() - t0
    status = "ok" if rc == 0 else f"FAIL rc={rc}"
    print(f"{name}: {status}  elapsed={wall:.1f}s", flush=True)
    if rc != 0:
        sys.exit(f"[run_e2e] stage {name!r} failed")
    return wall


def stage_infra():
    _run("infra", ["docker", "compose", "-f", "etl4/infra/docker-compose.yml",
                   "up", "-d"])


def stage_schema():
    sql = (ROOT / "etl4" / "infra" / "create_tables.sql").read_text()
    _run("schema", ["docker", "exec", "-i", "etl4-postgres",
                    "psql", "-U", "etl4", "-d", "etl4", "-c", sql])


def stage_ingest(source: str, limit: int):
    cmd = [PY, "-m", "etl4.orchestration.ingest_runner", "--source", source]
    if source == "zenodo":
        cmd += ["--latest"]
    elif source == "live":
        cmd += ["--limit", str(limit)]
    _run("ingest", cmd)


def stage_bronze():
    _run("bronze", [PY, "-m", "etl4.vault.bronze_flightlists"])


def stage_vault():
    _run("vault", [PY, "-m", "etl4.vault.raw_vault"])


def stage_marts():
    _run("marts", [PY, "-m", "etl4.vault.marts"])


def stage_qa():
    _run("qa", [PY, "-m", "etl4.qa.validate"])


def stage_sink(ym: str):
    _run("sink", [PY, "-m", "etl4.vault.postgres_sink", "--ym", ym])


def stage_reconcile():
    _run("reconcile", [PY, "-m", "etl4.qa.reconcile"])


def stage_matrix(source: str, profiles: list, methods: list | None,
                 workers: int | None, bronze: str, ym: str | None):
    cmd = [PY, "etl4/bench/run_matrix.py",
           "--bronze", bronze, "--source", source,
           "--profiles", *profiles]
    if methods:
        cmd += ["--methods", *methods]
    if workers is not None:
        cmd += ["--workers", str(workers)]
    if ym is not None:
        cmd += ["--ym", ym]
    _run("matrix", cmd)


def stage_eval_mart():
    """Build fact_experiment_result: one row per (noise, method)-latest run,
    pivoted from pipeline_runs x eval_results. Writes a parquet to
    data/gold/marts/fact_experiment_result/ and (re)creates the Postgres
    table of the same name."""
    import pandas as pd
    import psycopg

    print("\n=== eval_mart ===", flush=True)
    t0 = time.perf_counter()

    out = ROOT / "data" / "gold" / "marts" / "fact_experiment_result" / "part.parquet"
    dsn = "postgresql://etl4:change_me@localhost:5432/etl4"

    # Latest row per (source, method, noise) across any source (synthetic + s3 + rest).
    # Earlier filter `WHERE source='s3'` excluded the WI-1 synthetic matrix from the
    # mart; relaxed so Pareto + metric heatmap panels see every populated cell.
    # `position_rmse_m` matches the T-003-era metric key written by metrics.py.
    pivot = """
    WITH latest AS (
      SELECT DISTINCT ON (source, noise, method) run_id, run_date, source, method, noise,
             rows_in, rows_out, wall_secs, started_at
        FROM pipeline_runs
       ORDER BY source, noise, method, started_at DESC
    )
    SELECT l.run_id, l.run_date, l.source, l.method, l.noise,
           l.rows_in, l.rows_out, l.wall_secs, l.started_at,
           MAX(CASE WHEN er.metric='continuity'      THEN er.value END) AS continuity,
           MAX(CASE WHEN er.metric='n_flights'       THEN er.value END) AS n_flights,
           MAX(CASE WHEN er.metric='n_deviations'    THEN er.value END) AS n_deviations,
           MAX(CASE WHEN er.metric='position_rmse_m' THEN er.value END) AS rmse_m,
           MAX(CASE WHEN er.metric='seg_f1'          THEN er.value END) AS seg_f1,
           MAX(CASE WHEN er.metric='seg_precision'   THEN er.value END) AS seg_precision,
           MAX(CASE WHEN er.metric='seg_recall'      THEN er.value END) AS seg_recall,
           MAX(CASE WHEN er.metric='coverage_ratio'  THEN er.value END) AS coverage_ratio,
           MAX(CASE WHEN er.metric='boundary_err_s'  THEN er.value END) AS boundary_err_s
      FROM latest l
      LEFT JOIN eval_results er USING(run_id)
     GROUP BY l.run_id, l.run_date, l.source, l.method, l.noise,
              l.rows_in, l.rows_out, l.wall_secs, l.started_at
     ORDER BY l.source, l.noise, l.method
    """
    # DROP VIEW first: v_pareto_frontier (added by T-004) depends on
    # fact_experiment_result, so a bare DROP TABLE fails. Recreate the view
    # at the tail so the dependency restores idempotently.
    ddl = """
    DROP VIEW IF EXISTS v_pareto_frontier;
    DROP TABLE IF EXISTS fact_experiment_result;
    CREATE TABLE fact_experiment_result (
      run_id INTEGER PRIMARY KEY, run_date DATE, source TEXT,
      method TEXT, noise TEXT, rows_in BIGINT, rows_out BIGINT,
      wall_secs DOUBLE PRECISION, started_at TIMESTAMPTZ,
      continuity DOUBLE PRECISION, n_flights DOUBLE PRECISION,
      n_deviations DOUBLE PRECISION, rmse_m DOUBLE PRECISION,
      seg_f1 DOUBLE PRECISION, seg_precision DOUBLE PRECISION,
      seg_recall DOUBLE PRECISION, coverage_ratio DOUBLE PRECISION,
      boundary_err_s DOUBLE PRECISION);
    CREATE INDEX idx_fer_method ON fact_experiment_result(method);
    CREATE INDEX idx_fer_noise  ON fact_experiment_result(noise);
    """
    view_ddl = """
    CREATE OR REPLACE VIEW v_pareto_frontier AS
    SELECT fer.method, fer.noise,
           fer.rmse_m         AS accuracy_y,
           fer.wall_secs      AS cost_x,
           CASE WHEN fer.wall_secs > 0 THEN fer.rmse_m / fer.wall_secs ELSE NULL END AS efficiency
      FROM fact_experiment_result fer
     WHERE fer.rmse_m IS NOT NULL
       AND fer.wall_secs IS NOT NULL;
    """
    cols = ["run_id","run_date","source","method","noise","rows_in","rows_out",
            "wall_secs","started_at","continuity","n_flights","n_deviations",
            "rmse_m","seg_f1","seg_precision","seg_recall","coverage_ratio",
            "boundary_err_s"]

    with psycopg.connect(dsn) as conn:
        df = pd.read_sql(pivot, conn)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, index=False)

    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        for stmt in ddl.strip().split(";"):
            if stmt.strip():
                cur.execute(stmt)
        buf = io.StringIO()
        df[cols].to_csv(buf, index=False, header=False, na_rep="")
        buf.seek(0)
        with cur.copy(f"COPY fact_experiment_result ({', '.join(cols)}) "
                      f"FROM STDIN WITH (FORMAT csv, NULL '')") as cp:
            cp.write(buf.read())
        for stmt in view_ddl.strip().split(";"):
            if stmt.strip():
                cur.execute(stmt)

    wall = time.perf_counter() - t0
    print(f"eval_mart: ok  rows={len(df)}  elapsed={wall:.1f}s", flush=True)


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--source", default="zenodo",
                   choices=["zenodo", "live", "s3", "rest", "synthetic"],
                   help="batch (zenodo|s3), live REST stream (live), REST-states "
                        "replay (rest), or synthetic noise fixture (synthetic). "
                        "Only zenodo|live are supported by stage_ingest; "
                        "others require --only / --skip ingest.")
    p.add_argument("--live-limit", type=int, default=2000,
                   help="record count for --source live")
    p.add_argument("--bronze", default=None,
                   help="bronze tree for the matrix stage; "
                        "defaults to data/bronze/synthetic for --source synthetic, "
                        "else data/bronze/raw_states")
    p.add_argument("--ym", default=None,
                   help="partition month YYYY-MM. Required when --source is one "
                        "of s3|zenodo|rest, and when stage_sink is in the run set. "
                        "Optional for synthetic (auto-defaults to SYN-002A).")
    p.add_argument("--profiles", nargs="+",
                   default=["N0", "N1", "N2", "N3"])
    p.add_argument("--methods", nargs="+", default=None,
                   help="methods to run in the matrix stage "
                        "(default: all three)")
    p.add_argument("--workers", type=int, default=None,
                   help="parallel workers for the matrix stage")
    p.add_argument("--skip", nargs="+", default=[], choices=STAGES,
                   help="stages to skip")
    p.add_argument("--only", nargs="+", default=None, choices=STAGES,
                   help="run only these stages (overrides --skip)")
    p.add_argument("--skip-drift-check", action="store_true",
                   help="bypass the watermark-vs-filesystem pre-flight check")
    a = p.parse_args()

    run = set(a.only) if a.only else set(STAGES) - set(a.skip)

    # Argparse-level validation BEFORE any DB / FS work so operators see the
    # actionable error even when Postgres is down.
    matrix_source = a.source if a.source in MATRIX_SOURCES else "zenodo"
    if a.source in REAL_SOURCES and a.ym is None:
        p.error("--ym required when --source is one of %s"
                % "|".join(REAL_SOURCES))
    if "matrix" in run and matrix_source in REAL_SOURCES and a.ym is None:
        # stage_matrix forwards --source=live -> matrix_source=zenodo, which
        # run_matrix would reject mid-run as "--ym required". Fail fast.
        p.error("--ym required for stage_matrix when effective source is %s "
                "(got --source=%s mapped to matrix_source=%s)"
                % (matrix_source, a.source, matrix_source))
    if "sink" in run and (a.ym is None or not _YM_CALENDAR_RE.match(a.ym or "")):
        p.error("--ym YYYY-MM required for stage_sink "
                "(postgres_sink rejects the SYN-* sentinel)")
    if "ingest" in run and a.source not in INGEST_SOURCES:
        p.error("stage_ingest only supports --source %s (got %s); "
                "pass --skip ingest or --only ... to exclude it"
                % ("|".join(INGEST_SOURCES), a.source))

    # Resolve bronze default per source so --source synthetic does not feed
    # real state parquet into the synthetic noise-truth path.
    if a.bronze is None:
        a.bronze = ("data/bronze/synthetic" if a.source == "synthetic"
                    else "data/bronze/raw_states")

    print(f"[run_e2e] stages: {[s for s in STAGES if s in run]}", flush=True)
    print(f"[run_e2e] source={a.source}  ym={a.ym}  bronze={a.bronze}", flush=True)
    t_all = time.perf_counter()

    if "infra"     in run: stage_infra()
    if "schema"    in run: stage_schema()

    # Drift check runs AFTER infra+schema so a fresh machine does not crash
    # against UndefinedTable / OperationalError. On --only skipping both,
    # the caller is responsible for Postgres being reachable; the check
    # surfaces the real error if not.
    if not a.skip_drift_check:
        check_watermark_vs_filesystem()

    if "ingest"    in run: stage_ingest(a.source, a.live_limit)
    if "bronze"    in run: stage_bronze()
    if "vault"     in run: stage_vault()
    if "marts"     in run: stage_marts()
    if "qa"        in run: stage_qa()
    if "sink"      in run: stage_sink(a.ym)
    if "reconcile" in run: stage_reconcile()
    if "matrix"    in run:
        stage_matrix(matrix_source, a.profiles, a.methods, a.workers,
                     a.bronze, a.ym)
    if "eval_mart" in run: stage_eval_mart()

    print(f"\n[run_e2e] all done in {time.perf_counter() - t_all:.1f}s", flush=True)


if __name__ == "__main__":
    main()
