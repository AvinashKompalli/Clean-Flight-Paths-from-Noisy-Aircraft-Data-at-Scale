"""T-23 smoke: idempotent stage_eval_mart + v_eval_mart_ym view.

Covers acceptance criteria for T-23 (architecture.md section 5, PR5):

  1. stage_eval_mart runs end-to-end and exits 0
  2. fact_experiment_result has `last_ym TEXT` column after the run
  3. v_eval_mart_ym view presents the 6 columns
     (ym, source, cells, slowest_wall_s, mean_rmse_m, mean_seg_f1)
  4. v_eval_mart_ym rows == count(DISTINCT (last_ym, source))
  5. pipeline_watermarks row for ('eval_mart','fact_experiment_result')
     is present, last_ym is a calendar YYYY-MM, row_count == len(mart)
  6. Second invocation is idempotent:
     - fact_experiment_result row count unchanged
     - v_pareto_frontier row count unchanged (regression guard for
       dashboard 08 which froze on the WI-1 baseline)
     - v_eval_mart_ym row count unchanged
     - pipeline_watermarks.updated_at strictly advances

Smoke runs against the live Postgres instance (etl4-postgres). It does
NOT patch or isolate state; the assertions are self-balancing (compare
before vs after rather than hard-coding counts).

Prints "T23_SMOKE OK" when all assertions pass. Windows cp1252-safe
(no arrows, no em-dashes).

Invoke:
    python multi-agent-engineering/projects/PROJ-20261118-impr01-partition-contract/artifacts/t23_eval_mart_smoke.py
"""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO_ROOT))

import psycopg

from etl4.run_e2e import stage_eval_mart

DSN = os.getenv("POSTGRES_DSN", "postgresql://etl4:change_me@localhost:5432/etl4")
_YM_CAL = re.compile(r"^[0-9]{4}-(0[1-9]|1[0-2])$")


def _q_one(sql: str):
    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
    return row[0] if row else None


def _column_exists(table: str, column: str) -> bool:
    return bool(_q_one(
        "SELECT 1 FROM information_schema.columns "
        f"WHERE table_name='{table}' AND column_name='{column}'"
    ))


def _view_columns(view: str) -> list:
    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = %s ORDER BY ordinal_position",
            (view,),
        )
        return [r[0] for r in cur.fetchall()]


def _ac_1_stage_runs() -> None:
    stage_eval_mart()
    print("  AC-1 OK  stage_eval_mart exit=0 (first invocation)")


def _ac_2_last_ym_column() -> None:
    assert _column_exists("fact_experiment_result", "last_ym"), (
        "fact_experiment_result missing last_ym column"
    )
    print("  AC-2 OK  fact_experiment_result.last_ym present")


def _ac_3_view_shape() -> None:
    cols = _view_columns("v_eval_mart_ym")
    expected = ["ym", "source", "cells", "slowest_wall_s",
                "mean_rmse_m", "mean_seg_f1"]
    assert cols == expected, (
        "v_eval_mart_ym columns differ: expected %r got %r" % (expected, cols)
    )
    print("  AC-3 OK  v_eval_mart_ym 6 columns in exact order")


def _ac_4_view_cardinality() -> None:
    expected = _q_one(
        "SELECT count(*) FROM ("
        "  SELECT DISTINCT last_ym, source FROM fact_experiment_result"
        ") d"
    )
    actual = _q_one("SELECT count(*) FROM v_eval_mart_ym")
    assert expected == actual, (
        "v_eval_mart_ym cardinality mismatch: expected %d distinct "
        "(last_ym, source) pairs, got %d view rows" % (expected, actual)
    )
    assert actual >= 1, "v_eval_mart_ym empty - at least one row expected"
    print("  AC-4 OK  v_eval_mart_ym rows=%d == distinct (ym, source) pairs"
          % actual)


def _ac_5_watermark_present() -> None:
    row = None
    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT last_ym, row_count FROM pipeline_watermarks "
            "WHERE layer='eval_mart' AND table_name='fact_experiment_result'"
        )
        row = cur.fetchone()
    assert row is not None, "pipeline_watermarks row for eval_mart missing"
    last_ym, row_count = row
    assert _YM_CAL.match(last_ym), (
        "eval_mart last_ym=%r not calendar YYYY-MM" % last_ym
    )
    fer_rows = _q_one("SELECT count(*) FROM fact_experiment_result")
    assert row_count == fer_rows, (
        "watermark row_count=%d disagrees with fact_experiment_result "
        "count=%d" % (row_count, fer_rows)
    )
    print("  AC-5 OK  watermark eval_mart/%s row_count=%d (matches mart)"
          % (last_ym, row_count))


def _ac_6_idempotent() -> None:
    before = {
        "fer": _q_one("SELECT count(*) FROM fact_experiment_result"),
        "pareto": _q_one("SELECT count(*) FROM v_pareto_frontier"),
        "vem": _q_one("SELECT count(*) FROM v_eval_mart_ym"),
        "wm_ts": _q_one(
            "SELECT updated_at FROM pipeline_watermarks "
            "WHERE layer='eval_mart' AND table_name='fact_experiment_result'"
        ),
    }
    stage_eval_mart()
    after = {
        "fer": _q_one("SELECT count(*) FROM fact_experiment_result"),
        "pareto": _q_one("SELECT count(*) FROM v_pareto_frontier"),
        "vem": _q_one("SELECT count(*) FROM v_eval_mart_ym"),
        "wm_ts": _q_one(
            "SELECT updated_at FROM pipeline_watermarks "
            "WHERE layer='eval_mart' AND table_name='fact_experiment_result'"
        ),
    }
    assert after["fer"] == before["fer"], (
        "fact_experiment_result count changed: %d -> %d"
        % (before["fer"], after["fer"])
    )
    assert after["pareto"] == before["pareto"], (
        "v_pareto_frontier count changed: %d -> %d (dashboard 08 regression)"
        % (before["pareto"], after["pareto"])
    )
    assert after["vem"] == before["vem"], (
        "v_eval_mart_ym count changed: %d -> %d"
        % (before["vem"], after["vem"])
    )
    assert after["wm_ts"] > before["wm_ts"], (
        "watermark.updated_at did not advance: %s -> %s"
        % (before["wm_ts"], after["wm_ts"])
    )
    print("  AC-6 OK  second invocation idempotent "
          "(fer=%d pareto=%d vem=%d; updated_at advanced)"
          % (after["fer"], after["pareto"], after["vem"]))


def main() -> None:
    _ac_1_stage_runs()
    _ac_2_last_ym_column()
    _ac_3_view_shape()
    _ac_4_view_cardinality()
    _ac_5_watermark_present()
    _ac_6_idempotent()
    print("T23_SMOKE OK")


if __name__ == "__main__":
    main()
