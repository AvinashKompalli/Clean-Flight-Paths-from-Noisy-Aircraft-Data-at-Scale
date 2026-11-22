"""Unit tests for etl4/vault/watermark.py (T-02, IMPR-01).

Covers the load-bearing contract: watermark set() runs inside the caller's
transaction (no autocommit), get() reads committed state through a fresh
autocommit connection, CHECK regex rejects malformed last_ym, and the
(layer, table_name) key is honoured.

Hits the real etl4-postgres container -- there is nothing to mock here
because the invariant under test IS the DB behaviour (CHECK constraint,
transaction visibility, UPSERT idempotence). If Postgres is unreachable,
the whole module SKIPs rather than failing.

All test rows live under layer prefix ``__t25_`` so an autouse fixture can
scrub them before and after every test, even if an earlier test crashed
mid-run.
"""
from __future__ import annotations

import os
import time

import psycopg
import pytest
from psycopg import errors as pg_errors

from etl4.vault import watermark

DSN = os.getenv("POSTGRES_DSN", "postgresql://etl4:change_me@localhost:5432/etl4")

# Every test writes under this layer prefix so cleanup is a single LIKE delete.
LAYER = "__t25_unit"


def _postgres_reachable() -> bool:
    try:
        with psycopg.connect(DSN, connect_timeout=3) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _postgres_reachable(),
    reason="etl4-postgres unreachable; watermark unit tests require live DB",
)


def _purge() -> None:
    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "DELETE FROM pipeline_watermarks WHERE layer LIKE %s",
            (LAYER + "%",),
        )


@pytest.fixture(autouse=True)
def _scrub_test_rows():
    _purge()
    yield
    _purge()


def _fetch_full(layer: str, table_name: str):
    """Return (last_ym, row_count, updated_at) or None."""
    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT last_ym, row_count, updated_at "
            "FROM pipeline_watermarks WHERE layer = %s AND table_name = %s",
            (layer, table_name),
        )
        return cur.fetchone()


# ---------------------------------------------------------------------------
# 1. get() on empty table returns None
# ---------------------------------------------------------------------------
def test_get_returns_none_when_no_row():
    v = watermark.get(LAYER, "no_such_table")
    assert v is None


# ---------------------------------------------------------------------------
# 2. set() + get() round-trip across connection boundaries
# ---------------------------------------------------------------------------
def test_set_then_get_round_trip():
    with psycopg.connect(DSN, autocommit=False) as conn:
        watermark.set(LAYER, "round_trip", "2022-07", 1234, conn)
        conn.commit()

    assert watermark.get(LAYER, "round_trip") == "2022-07"

    full = _fetch_full(LAYER, "round_trip")
    assert full is not None
    assert full[0] == "2022-07"
    assert full[1] == 1234


# ---------------------------------------------------------------------------
# 3. set() is idempotent -- repeat with same args leaves last_ym + row_count
#    unchanged, updated_at monotonically advances (>= because now() resolution
#    may coalesce two very fast calls to the same tick)
# ---------------------------------------------------------------------------
def test_set_is_idempotent():
    with psycopg.connect(DSN, autocommit=False) as conn:
        watermark.set(LAYER, "idem", "2022-08", 42, conn)
        conn.commit()
    first = _fetch_full(LAYER, "idem")

    time.sleep(0.05)  # help now() tick

    with psycopg.connect(DSN, autocommit=False) as conn:
        watermark.set(LAYER, "idem", "2022-08", 42, conn)
        conn.commit()
    second = _fetch_full(LAYER, "idem")

    assert first[0] == second[0] == "2022-08"
    assert first[1] == second[1] == 42
    assert second[2] >= first[2]


# ---------------------------------------------------------------------------
# 4. CHECK violation on malformed last_ym -- the DDL regex is the
#    ^[0-9]{4}-(0[1-9]|1[0-2])$ pattern; these forms must all raise.
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "bad_ym",
    [
        "2022-7",    # single-digit month, no zero pad
        "2022-13",   # month > 12
        "2022/07",   # wrong separator
        "22-07",     # two-digit year
    ],
)
def test_set_rejects_malformed_last_ym(bad_ym):
    with pytest.raises(pg_errors.CheckViolation):
        with psycopg.connect(DSN, autocommit=False) as conn:
            watermark.set(LAYER, "bad_ym", bad_ym, 0, conn)
            conn.commit()

    # CheckViolation should have left no row behind.
    assert watermark.get(LAYER, "bad_ym") is None


# ---------------------------------------------------------------------------
# 5. set() does NOT commit on behalf of the caller. Rolling back the caller's
#    transaction must drop the pending watermark write.
# ---------------------------------------------------------------------------
def test_set_does_not_autocommit_so_rollback_drops_write():
    with psycopg.connect(DSN, autocommit=False) as conn:
        watermark.set(LAYER, "rollback_case", "2022-09", 7, conn)
        # Mid-txn: the write IS visible on THIS connection.
        with conn.cursor() as cur:
            cur.execute(
                "SELECT last_ym FROM pipeline_watermarks "
                "WHERE layer = %s AND table_name = %s",
                (LAYER, "rollback_case"),
            )
            assert cur.fetchone() == ("2022-09",)
        conn.rollback()

    # A separate autocommit connection sees no committed row.
    assert watermark.get(LAYER, "rollback_case") is None


# ---------------------------------------------------------------------------
# 6. set() respects caller's commit -- same pattern as #5, but commit this
#    time; a separate connection then sees the write.
# ---------------------------------------------------------------------------
def test_set_respects_caller_commit():
    with psycopg.connect(DSN, autocommit=False) as conn:
        watermark.set(LAYER, "commit_case", "2022-10", 99, conn)
        conn.commit()

    assert watermark.get(LAYER, "commit_case") == "2022-10"


# ---------------------------------------------------------------------------
# 7. get() keys on (layer, table_name) exactly. Three rows inserted under
#    different keys; each get() returns the right value and non-existent
#    keys return None.
# ---------------------------------------------------------------------------
def test_get_keys_on_layer_table_pair():
    rows = [
        (LAYER + "_a", "tab1", "2022-01", 1),
        (LAYER + "_a", "tab2", "2022-02", 2),
        (LAYER + "_b", "tab1", "2022-03", 3),
    ]
    with psycopg.connect(DSN, autocommit=False) as conn:
        for layer, table, ym, cnt in rows:
            watermark.set(layer, table, ym, cnt, conn)
        conn.commit()

    assert watermark.get(LAYER + "_a", "tab1") == "2022-01"
    assert watermark.get(LAYER + "_a", "tab2") == "2022-02"
    assert watermark.get(LAYER + "_b", "tab1") == "2022-03"

    # Non-existent (layer, table) permutations all return None.
    assert watermark.get(LAYER + "_a", "tab3") is None
    assert watermark.get(LAYER + "_c", "tab1") is None
    assert watermark.get(LAYER + "_b", "tab2") is None


# ---------------------------------------------------------------------------
# 8. Edge: calendar boundaries. '2022-01' and '2022-12' are accepted;
#    '2022-00' and '2022-13' are rejected.
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("good_ym", ["2022-01", "2022-12"])
def test_set_accepts_calendar_boundary_months(good_ym):
    with psycopg.connect(DSN, autocommit=False) as conn:
        watermark.set(LAYER, "boundary_ok", good_ym, 0, conn)
        conn.commit()
    assert watermark.get(LAYER, "boundary_ok") == good_ym


@pytest.mark.parametrize("bad_ym", ["2022-00", "2022-13"])
def test_set_rejects_out_of_range_months(bad_ym):
    with pytest.raises(pg_errors.CheckViolation):
        with psycopg.connect(DSN, autocommit=False) as conn:
            watermark.set(LAYER, "boundary_bad", bad_ym, 0, conn)
            conn.commit()
    assert watermark.get(LAYER, "boundary_bad") is None


# ---------------------------------------------------------------------------
# 9. (Bonus) set() with row_count=0 is legal; set() can advance last_ym on a
#    subsequent call. Guards the UPSERT-DO-UPDATE branch, which the
#    idempotence test (#3) exercises only with identical args.
# ---------------------------------------------------------------------------
def test_set_upsert_advances_last_ym_on_second_call():
    with psycopg.connect(DSN, autocommit=False) as conn:
        watermark.set(LAYER, "advance", "2022-05", 0, conn)
        conn.commit()
    assert watermark.get(LAYER, "advance") == "2022-05"

    with psycopg.connect(DSN, autocommit=False) as conn:
        watermark.set(LAYER, "advance", "2022-06", 500, conn)
        conn.commit()

    full = _fetch_full(LAYER, "advance")
    assert full is not None
    assert full[0] == "2022-06"
    assert full[1] == 500
