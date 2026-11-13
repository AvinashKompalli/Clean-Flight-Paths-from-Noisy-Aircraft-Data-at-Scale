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
   8. sink         COPY marts into Postgres
   9. reconcile    row-count reconciliation bronze -> vault -> marts  (gates matrix)
  10. matrix       3-method x N-noise reconstruction matrix  (telemetry -> pipeline_runs + eval_results)
  11. eval_mart    fact_experiment_result mart (pivot of pipeline_runs x eval_results)

Usage:
  python etl4/run_e2e.py                               # everything, zenodo ingest
  python etl4/run_e2e.py --skip infra schema          # resume mid-chain
  python etl4/run_e2e.py --only matrix eval_mart      # just these two
  python etl4/run_e2e.py --profiles N0 N1             # shorter matrix
  python etl4/run_e2e.py --source live --live-limit 2000  # live REST stream instead
"""
from __future__ import annotations

import argparse
import io
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PY = sys.executable

STAGES = ["infra", "schema", "ingest", "bronze", "vault",
          "marts", "qa", "sink", "reconcile", "matrix", "eval_mart"]


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


def stage_sink():
    _run("sink", [PY, "-m", "etl4.vault.postgres_sink"])


def stage_reconcile():
    _run("reconcile", [PY, "-m", "etl4.qa.reconcile"])


def stage_matrix(profiles: list[str], bronze: str):
    _run("matrix", [PY, "etl4/bench/run_matrix.py",
                    "--bronze", bronze, "--source", "zenodo",
                    "--profiles", *profiles])


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

    pivot = """
    WITH latest AS (
      SELECT DISTINCT ON (noise, method) run_id, run_date, source, method, noise,
             rows_in, rows_out, wall_secs, started_at
        FROM pipeline_runs
       WHERE source = 's3'
       ORDER BY noise, method, started_at DESC
    )
    SELECT l.run_id, l.run_date, l.source, l.method, l.noise,
           l.rows_in, l.rows_out, l.wall_secs, l.started_at,
           MAX(CASE WHEN er.metric='continuity'   THEN er.value END) AS continuity,
           MAX(CASE WHEN er.metric='n_flights'    THEN er.value END) AS n_flights,
           MAX(CASE WHEN er.metric='n_deviations' THEN er.value END) AS n_deviations,
           MAX(CASE WHEN er.metric='rmse_m'       THEN er.value END) AS rmse_m,
           MAX(CASE WHEN er.metric='seg_f1'       THEN er.value END) AS seg_f1
      FROM latest l
      LEFT JOIN eval_results er USING(run_id)
     GROUP BY l.run_id, l.run_date, l.source, l.method, l.noise,
              l.rows_in, l.rows_out, l.wall_secs, l.started_at
     ORDER BY l.noise, l.method
    """
    ddl = """
    DROP TABLE IF EXISTS fact_experiment_result;
    CREATE TABLE fact_experiment_result (
      run_id INTEGER PRIMARY KEY, run_date DATE, source TEXT,
      method TEXT, noise TEXT, rows_in BIGINT, rows_out BIGINT,
      wall_secs DOUBLE PRECISION, started_at TIMESTAMPTZ,
      continuity DOUBLE PRECISION, n_flights DOUBLE PRECISION,
      n_deviations DOUBLE PRECISION, rmse_m DOUBLE PRECISION,
      seg_f1 DOUBLE PRECISION);
    CREATE INDEX idx_fer_method ON fact_experiment_result(method);
    CREATE INDEX idx_fer_noise  ON fact_experiment_result(noise);
    """
    cols = ["run_id","run_date","source","method","noise","rows_in","rows_out",
            "wall_secs","started_at","continuity","n_flights","n_deviations",
            "rmse_m","seg_f1"]

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

    wall = time.perf_counter() - t0
    print(f"eval_mart: ok  rows={len(df)}  elapsed={wall:.1f}s", flush=True)


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--source", default="zenodo", choices=["zenodo", "live"],
                   help="zenodo (batch, primary) or live (REST stream, secondary)")
    p.add_argument("--live-limit", type=int, default=2000,
                   help="record count for --source live")
    p.add_argument("--bronze", default="data/bronze/raw_states",
                   help="bronze tree for the matrix stage")
    p.add_argument("--profiles", nargs="+",
                   default=["N0", "N1", "N2", "N3"])
    p.add_argument("--skip", nargs="+", default=[], choices=STAGES,
                   help="stages to skip")
    p.add_argument("--only", nargs="+", default=None, choices=STAGES,
                   help="run only these stages (overrides --skip)")
    a = p.parse_args()

    run = set(a.only) if a.only else set(STAGES) - set(a.skip)
    print(f"[run_e2e] stages: {[s for s in STAGES if s in run]}", flush=True)
    t_all = time.perf_counter()

    if "infra"     in run: stage_infra()
    if "schema"    in run: stage_schema()
    if "ingest"    in run: stage_ingest(a.source, a.live_limit)
    if "bronze"    in run: stage_bronze()
    if "vault"     in run: stage_vault()
    if "marts"     in run: stage_marts()
    if "qa"        in run: stage_qa()
    if "sink"      in run: stage_sink()
    if "reconcile" in run: stage_reconcile()
    if "matrix"    in run: stage_matrix(a.profiles, a.bronze)
    if "eval_mart" in run: stage_eval_mart()

    print(f"\n[run_e2e] all done in {time.perf_counter() - t_all:.1f}s", flush=True)


if __name__ == "__main__":
    main()
