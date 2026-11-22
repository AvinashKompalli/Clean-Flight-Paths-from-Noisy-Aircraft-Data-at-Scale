"""T-02 smoke: exercises watermark get/set + txn-sharing contract.

Covers acceptance criteria 3-5 for T-02:
  3. get() on empty table returns None; after set(...) inside a txn returns
     the value; malformed last_ym raises CheckViolation.
  4. set() is idempotent -- calling twice with same args yields identical
     final state (except updated_at).
  5. Txn sharing: one connection does set() on pipeline_watermarks AND an
     INSERT on a sibling table (pipeline_runs); rollback drops BOTH.

Prints "WATERMARK SMOKE OK" when all assertions pass. Cleans up test data.
Windows cp1252-safe (no arrows, no em-dashes).

Invoke:
    python multi-agent-engineering/projects/PROJ-20261118-impr01-partition-contract/artifacts/t02_watermark_smoke.py
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO_ROOT))

import psycopg
from psycopg import errors as pg_errors

from etl4.vault import watermark

DSN = os.getenv("POSTGRES_DSN", "postgresql://etl4:change_me@localhost:5432/etl4")

# Isolated test keys so we never collide with real registry rows.
TEST_LAYER = "test_t02_smoke"
TEST_TABLE_A = "smoke_table_a"
TEST_TABLE_B = "smoke_table_b"
TEST_TABLE_C = "smoke_table_c"
TEST_YM_1 = "2022-07"
TEST_YM_2 = "2022-08"
TEST_YM_BAD = "2022-7"  # fails CHECK regex (no zero pad)


def _cleanup():
    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "DELETE FROM pipeline_watermarks WHERE layer = %s",
            (TEST_LAYER,),
        )
        cur.execute(
            "DELETE FROM pipeline_runs WHERE source = %s",
            (f"__smoke_{TEST_LAYER}",),
        )


def _ensure_sibling_schema():
    """pipeline_runs.ym may be NULL on current (pre-migration) schema; both
    shapes work because this smoke only INSERTs then rolls back."""
    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'pipeline_runs' AND column_name = 'ym'"
        )
        return cur.fetchone() is not None


def _count_run(conn, sentinel: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM pipeline_runs WHERE source = %s",
            (sentinel,),
        )
        return cur.fetchone()[0]


def _get_wm(conn, layer: str, table_name: str):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT last_ym, row_count, updated_at FROM pipeline_watermarks "
            "WHERE layer = %s AND table_name = %s",
            (layer, table_name),
        )
        return cur.fetchone()


def run():
    _cleanup()
    has_ym_col = _ensure_sibling_schema()

    # --- AC-3a: empty-table get returns None ---
    v = watermark.get(TEST_LAYER, TEST_TABLE_A)
    assert v is None, f"expected None on empty, got {v!r}"

    # --- AC-3b: set inside a txn, then get sees the value ---
    with psycopg.connect(DSN, autocommit=False) as conn:
        watermark.set(TEST_LAYER, TEST_TABLE_A, TEST_YM_1, 100, conn)
        conn.commit()

    v = watermark.get(TEST_LAYER, TEST_TABLE_A)
    assert v == TEST_YM_1, f"expected {TEST_YM_1!r}, got {v!r}"

    # --- AC-3c: malformed last_ym raises CheckViolation ---
    raised = False
    try:
        with psycopg.connect(DSN, autocommit=False) as conn:
            watermark.set(TEST_LAYER, TEST_TABLE_B, TEST_YM_BAD, 0, conn)
            conn.commit()
    except pg_errors.CheckViolation:
        raised = True
    assert raised, "malformed last_ym should have raised CheckViolation"

    # Nothing should have been persisted for TEST_TABLE_B.
    v = watermark.get(TEST_LAYER, TEST_TABLE_B)
    assert v is None, f"expected None after CheckViolation rollback, got {v!r}"

    # --- AC-4: idempotence. Call set twice with same args; only updated_at moves.
    with psycopg.connect(DSN, autocommit=False) as conn:
        watermark.set(TEST_LAYER, TEST_TABLE_C, TEST_YM_2, 42, conn)
        conn.commit()
    with psycopg.connect(DSN, autocommit=True) as conn:
        first = _get_wm(conn, TEST_LAYER, TEST_TABLE_C)
    time.sleep(0.05)  # ensure now() tick for updated_at comparison
    with psycopg.connect(DSN, autocommit=False) as conn:
        watermark.set(TEST_LAYER, TEST_TABLE_C, TEST_YM_2, 42, conn)
        conn.commit()
    with psycopg.connect(DSN, autocommit=True) as conn:
        second = _get_wm(conn, TEST_LAYER, TEST_TABLE_C)

    assert first[0] == second[0] == TEST_YM_2, "last_ym changed across idempotent calls"
    assert first[1] == second[1] == 42, "row_count changed across idempotent calls"
    assert second[2] >= first[2], "updated_at should advance (monotonic)"

    # --- AC-5: txn sharing. One conn, two inserts (watermark + pipeline_runs),
    # roll back, confirm BOTH disappear.
    sentinel = f"__smoke_{TEST_LAYER}"
    # Pre-state: no watermark for TABLE_A under layer "sibling_txn_test", no runs.
    with psycopg.connect(DSN, autocommit=True) as conn:
        pre_wm = _get_wm(conn, TEST_LAYER, "sibling_txn_table")
        pre_run = _count_run(conn, sentinel)
    assert pre_wm is None
    assert pre_run == 0

    with psycopg.connect(DSN, autocommit=False) as conn:
        watermark.set(TEST_LAYER, "sibling_txn_table", "2022-09", 7, conn)
        with conn.cursor() as cur:
            if has_ym_col:
                cur.execute(
                    "INSERT INTO pipeline_runs "
                    "(run_date, source, method, noise, rows_in, rows_out, wall_secs, ym) "
                    "VALUES (CURRENT_DATE, %s, 'kinematic', 'N0', 1, 1, 0.1, %s)",
                    (sentinel, "2022-09"),
                )
            else:
                cur.execute(
                    "INSERT INTO pipeline_runs "
                    "(run_date, source, method, noise, rows_in, rows_out, wall_secs) "
                    "VALUES (CURRENT_DATE, %s, 'kinematic', 'N0', 1, 1, 0.1)",
                    (sentinel,),
                )
            # Mid-txn sanity: both visible on this connection.
            mid_wm = _get_wm(conn, TEST_LAYER, "sibling_txn_table")
            mid_run = _count_run(conn, sentinel)
        assert mid_wm is not None and mid_wm[0] == "2022-09", \
            f"mid-txn watermark not visible: {mid_wm!r}"
        assert mid_run == 1, f"mid-txn run not visible: {mid_run}"
        conn.rollback()

    # Post-rollback: BOTH must be gone (atomic rollback across the two tables).
    with psycopg.connect(DSN, autocommit=True) as conn:
        post_wm = _get_wm(conn, TEST_LAYER, "sibling_txn_table")
        post_run = _count_run(conn, sentinel)
    assert post_wm is None, f"watermark survived rollback: {post_wm!r}"
    assert post_run == 0, f"sibling run survived rollback: {post_run}"

    # Also verify the COMMIT side of the same pattern: both must be present.
    with psycopg.connect(DSN, autocommit=False) as conn:
        watermark.set(TEST_LAYER, "sibling_txn_table", "2022-10", 11, conn)
        with conn.cursor() as cur:
            if has_ym_col:
                cur.execute(
                    "INSERT INTO pipeline_runs "
                    "(run_date, source, method, noise, rows_in, rows_out, wall_secs, ym) "
                    "VALUES (CURRENT_DATE, %s, 'kinematic', 'N0', 1, 1, 0.1, %s)",
                    (sentinel, "2022-10"),
                )
            else:
                cur.execute(
                    "INSERT INTO pipeline_runs "
                    "(run_date, source, method, noise, rows_in, rows_out, wall_secs) "
                    "VALUES (CURRENT_DATE, %s, 'kinematic', 'N0', 1, 1, 0.1)",
                    (sentinel,),
                )
        conn.commit()

    with psycopg.connect(DSN, autocommit=True) as conn:
        post_commit_wm = _get_wm(conn, TEST_LAYER, "sibling_txn_table")
        post_commit_run = _count_run(conn, sentinel)
    assert post_commit_wm is not None and post_commit_wm[0] == "2022-10"
    assert post_commit_run == 1

    # --- Cleanup: delete all smoke rows ---
    _cleanup()
    with psycopg.connect(DSN, autocommit=True) as conn:
        residual_wm = _get_wm(conn, TEST_LAYER, TEST_TABLE_A)
        residual_run = _count_run(conn, sentinel)
    assert residual_wm is None
    assert residual_run == 0

    print("WATERMARK SMOKE OK", flush=True)


if __name__ == "__main__":
    try:
        run()
    except AssertionError as e:
        print(f"SMOKE FAIL: {e}", flush=True)
        sys.exit(1)
