"""Tests for orchestration/ingest_metrics.py and orchestration/ingest_runner.py.

Uses the live Postgres instance (localhost:5432, etl4/change_me/etl4). Rows are
written under source='pytest' and deleted in a finally block.
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

import psycopg
import pytest

from etl4.orchestration.ingest_metrics import DSN, ingest_run

ROOT = Path(__file__).resolve().parents[2]
CLEAN_SQL = "DELETE FROM ingest_runs WHERE source='pytest'"


def _purge() -> None:
    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(CLEAN_SQL)


def test_ingest_run_happy_path():
    try:
        with ingest_run("pytest", "batch", extras={"case": "happy"}) as rec:
            time.sleep(0.05)
            rec["rows_out"] = 42
            rec["bytes_out"] = 1024

        with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT status, wall_seconds, rows_out, bytes_out, error_message "
                "FROM ingest_runs WHERE source='pytest' "
                "ORDER BY run_id DESC LIMIT 1"
            )
            row = cur.fetchone()
        assert row is not None, "no row written for happy path"
        status, wall, rows_out, bytes_out, err = row
        assert status == "ok"
        assert wall is not None and wall > 0
        assert rows_out == 42
        assert bytes_out == 1024
        assert err is None
    finally:
        _purge()


def test_ingest_run_error_path():
    try:
        with pytest.raises(RuntimeError, match="boom"):
            with ingest_run("pytest", "batch", extras={"case": "error"}) as rec:
                rec["rows_out"] = 7
                raise RuntimeError("boom")

        with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT status, error_message, rows_out "
                "FROM ingest_runs WHERE source='pytest' "
                "ORDER BY run_id DESC LIMIT 1"
            )
            row = cur.fetchone()
        assert row is not None, "no row written for error path"
        status, err, rows_out = row
        assert status == "error"
        assert err is not None and "RuntimeError" in err and "boom" in err
        assert rows_out == 7
    finally:
        _purge()


def test_ingest_runner_rejects_bad_source():
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT) + os.pathsep + env.get("PYTHONPATH", "")
    try:
        proc = subprocess.run(
            [sys.executable, "-m", "etl4.orchestration.ingest_runner",
             "--source", "nope"],
            cwd=str(ROOT), env=env,
            capture_output=True, text=True, timeout=30,
        )
        assert proc.returncode != 0, (
            f"expected non-zero exit for --source nope, got {proc.returncode}\n"
            f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
        )
    finally:
        _purge()
