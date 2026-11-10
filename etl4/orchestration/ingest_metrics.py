"""Ingest-run telemetry helper.

Context manager that opens an `ingest_runs` row on enter, closes it on exit
with wall time + rows + bytes + status. Powers dashboard 05_ingest_comparison.
"""
from __future__ import annotations

import json
import os
import time
import traceback
from contextlib import contextmanager

import psycopg

DSN = os.getenv("POSTGRES_DSN", "postgresql://etl4:change_me@localhost:5432/etl4")


@contextmanager
def ingest_run(source: str, pattern: str, extras: dict | None = None):
    """Usage:
        with ingest_run("s3", "batch") as rec:
            ... do work ...
            rec["rows_out"]  = n
            rec["bytes_out"] = b
    """
    rec: dict = {"rows_out": 0, "bytes_out": 0, "extras": extras or {}}
    t0 = time.perf_counter()
    conn = psycopg.connect(DSN, autocommit=True)
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO ingest_runs (source, pattern, extras) VALUES (%s, %s, %s) "
            "RETURNING run_id",
            (source, pattern, json.dumps(rec["extras"])),
        )
        run_id = cur.fetchone()[0]
    status, err = "ok", None
    try:
        yield rec
    except BaseException:
        status, err = "error", traceback.format_exc()
        raise
    finally:
        wall = time.perf_counter() - t0
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE ingest_runs SET finished_at=now(), wall_seconds=%s, "
                "rows_out=%s, bytes_out=%s, status=%s, error_message=%s, extras=%s "
                "WHERE run_id=%s",
                (wall, int(rec["rows_out"]), int(rec["bytes_out"]),
                 status, err, json.dumps(rec["extras"]), run_id),
            )
        conn.close()
