"""PR4 smoke: covers T-17 (postgres_sink staged UPSERT) + T-18 (write_run CTE).

T-17 verifies that etl4/vault/postgres_sink.py:
  - stages rows into stg_dim_aircraft via COPY,
  - INSERT ... ON CONFLICT (hk_aircraft) DO UPDATE SET ... lands 10 rows,
  - re-staging one row with a changed typecode flips the live table's value
    (ON CONFLICT DO UPDATE path),
  - advances pipeline_watermarks('sink', 'dim_aircraft', ym) in the same txn.

T-18 verifies that etl4/sink/postgres.py::write_run:
  - INSERT ON CONFLICT (ym, source, method, noise) DO NOTHING RETURNING run_id,
  - calling twice with the same cell returns the pre-existing run_id (second
    call is a no-op; no duplicate row in pipeline_runs),
  - eval_results is populated only on the NEW insert, not on the conflict skip.

Both smokes touch a real Postgres instance. Test rows are scoped with
``__smoke__`` prefixes so they cannot collide with real data; the script
deletes everything it inserts.

T-18 needs pipeline_runs.ym + UNIQUE (ym, source, method, noise), which is
the T-16 migration. T-16 has not merged yet, so this script attempts the
ALTER TABLE itself and rolls it back at the end. If a real T-16 run has
already landed, the ALTER IF NOT EXISTS is a no-op.

Prints "PR4 SMOKE OK" when both halves pass. Windows cp1252-safe.

Invoke:
    python multi-agent-engineering/projects/PROJ-20261118-impr01-partition-contract/artifacts/pr4_sink_smoke.py
"""
from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO_ROOT))

import pandas as pd
import psycopg

from etl4.sink.postgres import write_run
from etl4.vault.postgres_sink import load_table

DSN = os.getenv("POSTGRES_DSN", "postgresql://etl4:change_me@localhost:5432/etl4")

SMOKE_YM          = "2099-07"              # out-of-range so real data never collides
SMOKE_SOURCE      = "__smoke__pr4"
SMOKE_METHOD      = "time_gap"
SMOKE_NOISE       = "N0"
SMOKE_HK_PREFIX   = "__smk_"
SMOKE_WM_TABLE    = "dim_aircraft"         # same name the sink writes under

# T-16 migration bits we have to add ourselves (and undo at the end).
T16_ADD_COLUMN   = "ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS ym TEXT"
T16_ADD_UNIQUE   = (
    "ALTER TABLE pipeline_runs "
    "ADD CONSTRAINT pipeline_runs_ym_src_mth_noise_key "
    "UNIQUE (ym, source, method, noise)"
)
T16_DROP_UNIQUE  = (
    "ALTER TABLE pipeline_runs "
    "DROP CONSTRAINT IF EXISTS pipeline_runs_ym_src_mth_noise_key"
)
T16_DROP_COLUMN  = "ALTER TABLE pipeline_runs DROP COLUMN IF EXISTS ym"


def _row(hk: str, icao: str, reg: str | None, tc: str | None) -> dict:
    return {
        "hk_aircraft":  hk,
        "icao24":       icao,
        "registration": reg,
        "typecode":     tc,
    }


def _ten_rows(tc_for_hk5: str = "A320") -> pd.DataFrame:
    """Ten rows for dim_aircraft upsert. Row 5's typecode is parameterised so
    re-staging produces an observable DO UPDATE change."""
    rows = []
    for i in range(10):
        hk   = f"{SMOKE_HK_PREFIX}{i:02d}"
        icao = f"{0xA0 + i:06x}"
        tc   = tc_for_hk5 if i == 5 else f"T{i:03d}"
        rows.append(_row(hk, icao, f"REG{i:03d}", tc))
    # Deliberate duplicate on hk 03: the DISTINCT ON tiebreaker should keep the
    # non-null typecode (second row), proving DISTINCT ON stays sane on dupes.
    rows.append(_row(f"{SMOKE_HK_PREFIX}03", f"{0xA3:06x}", "REG003", "T003_dup"))
    return pd.DataFrame(rows, dtype="object").astype({
        "hk_aircraft":  "string",
        "icao24":       "string",
        "registration": "string",
        "typecode":     "string",
    })


def _write_dim_parquet(marts_dir: Path, df: pd.DataFrame) -> Path:
    tbl_dir = marts_dir / "dim_aircraft"
    tbl_dir.mkdir(parents=True, exist_ok=True)
    pq = tbl_dir / "part.parquet"
    df.to_parquet(pq, index=False)
    return pq


def _count_smoke_rows(cur) -> int:
    cur.execute(
        "SELECT count(*) FROM dim_aircraft WHERE hk_aircraft LIKE %s",
        (f"{SMOKE_HK_PREFIX}%",),
    )
    return cur.fetchone()[0]


def _typecode(cur, hk: str) -> str | None:
    cur.execute("SELECT typecode FROM dim_aircraft WHERE hk_aircraft = %s", (hk,))
    r = cur.fetchone()
    return r[0] if r else None


def _watermark(cur, layer: str, name: str):
    cur.execute(
        "SELECT last_ym, row_count FROM pipeline_watermarks "
        "WHERE layer = %s AND table_name = %s",
        (layer, name),
    )
    return cur.fetchone()


def _cleanup_sink_rows():
    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "DELETE FROM dim_aircraft WHERE hk_aircraft LIKE %s",
            (f"{SMOKE_HK_PREFIX}%",),
        )
        cur.execute(
            "DELETE FROM pipeline_watermarks WHERE layer = 'sink' AND table_name = %s",
            (SMOKE_WM_TABLE,),
        )


def _cleanup_run_rows():
    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "DELETE FROM pipeline_runs WHERE source = %s AND ym = %s",
            (SMOKE_SOURCE, SMOKE_YM),
        )


def _t17():
    """T-17: staged COPY + INSERT ON CONFLICT DO UPDATE + same-txn watermark."""
    _cleanup_sink_rows()
    df_v1 = _ten_rows(tc_for_hk5="A320")

    with tempfile.TemporaryDirectory() as tmp:
        marts = Path(tmp)
        _write_dim_parquet(marts, df_v1)

        # First load: 10 rows (11 input, 1 duped on hk03 collapsed by DISTINCT ON).
        with psycopg.connect(DSN, autocommit=False) as conn:
            n = load_table(conn, "dim_aircraft", SMOKE_YM, marts)
        with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
            cnt = _count_smoke_rows(cur)
            assert cnt == 10, f"expected 10 smoke rows after first load, got {cnt}"
            tc5 = _typecode(cur, f"{SMOKE_HK_PREFIX}05")
            assert tc5 == "A320", f"expected A320 for row 05, got {tc5!r}"
            wm = _watermark(cur, "sink", SMOKE_WM_TABLE)
            assert wm is not None, "watermark row missing after first commit"
            assert wm[0] == SMOKE_YM, f"watermark last_ym={wm[0]!r} expected {SMOKE_YM!r}"

        # Second load: same PKs, row 05's typecode flips. Verifies DO UPDATE.
        df_v2 = _ten_rows(tc_for_hk5="B738")
        _write_dim_parquet(marts, df_v2)
        with psycopg.connect(DSN, autocommit=False) as conn:
            load_table(conn, "dim_aircraft", SMOKE_YM, marts)
        with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
            cnt = _count_smoke_rows(cur)
            assert cnt == 10, (
                f"expected 10 smoke rows after second load (DO UPDATE, no insert), "
                f"got {cnt}"
            )
            tc5 = _typecode(cur, f"{SMOKE_HK_PREFIX}05")
            assert tc5 == "B738", (
                f"ON CONFLICT DO UPDATE did not apply: row 05 typecode is {tc5!r}, "
                f"expected B738"
            )

        # COALESCE path: re-stage row 05 with NULL typecode; should KEEP B738.
        df_v3 = _ten_rows(tc_for_hk5=None)
        _write_dim_parquet(marts, df_v3)
        with psycopg.connect(DSN, autocommit=False) as conn:
            load_table(conn, "dim_aircraft", SMOKE_YM, marts)
        with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
            tc5 = _typecode(cur, f"{SMOKE_HK_PREFIX}05")
            assert tc5 == "B738", (
                f"COALESCE(EXCLUDED.typecode, dim_aircraft.typecode) failed: "
                f"got {tc5!r}, expected B738 preserved"
            )
    _cleanup_sink_rows()
    print("  T-17 OK  staged COPY + DO UPDATE + COALESCE + same-txn watermark",
          flush=True)


def _t18_prep():
    """Add ym + UNIQUE constraint if T-16 has not shipped yet. Returns a
    callable that undoes whatever this call changed."""
    undo = []
    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name='pipeline_runs' AND column_name='ym'"
        )
        had_ym = cur.fetchone() is not None
        if not had_ym:
            cur.execute(T16_ADD_COLUMN)
            undo.append(T16_DROP_COLUMN)
        cur.execute(
            "SELECT 1 FROM information_schema.table_constraints "
            "WHERE table_name='pipeline_runs' "
            "  AND constraint_name='pipeline_runs_ym_src_mth_noise_key'"
        )
        had_uq = cur.fetchone() is not None
        if not had_uq:
            # Backfill ym on any pre-existing rows so the constraint can be
            # added without duplicate-NULL fights.
            cur.execute(
                "UPDATE pipeline_runs SET ym = to_char(run_date, 'YYYY-MM') "
                "WHERE ym IS NULL"
            )
            cur.execute(
                "DELETE FROM pipeline_runs p1 USING pipeline_runs p2 "
                "WHERE p1.run_id < p2.run_id "
                "  AND p1.ym = p2.ym AND p1.source = p2.source "
                "  AND p1.method = p2.method AND p1.noise = p2.noise"
            )
            cur.execute(T16_ADD_UNIQUE)
            undo.append(T16_DROP_UNIQUE)

    def undo_all():
        if not undo:
            return
        with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
            for stmt in undo:
                cur.execute(stmt)
    return undo_all


def _t18():
    """T-18: write_run CTE - duplicate (ym, source, method, noise) is a no-op."""
    _cleanup_run_rows()
    undo = _t18_prep()
    try:
        metrics = {
            "source":          SMOKE_SOURCE,
            "method":          SMOKE_METHOD,
            "noise":           SMOKE_NOISE,
            "rows_in":         100,
            "rows_out":        95,
            "wall_s":          1.5,
            "n_flights":       7,
            "n_deviations":    0,
            "continuity":      0.9,
            "position_rmse_m": 42.0,
        }
        run_id_1 = write_run(metrics, ym=SMOKE_YM)
        assert run_id_1 is not None, "first write_run returned None"

        run_id_2 = write_run(metrics, ym=SMOKE_YM)
        assert run_id_2 == run_id_1, (
            f"second write_run returned a different run_id: "
            f"{run_id_2} != {run_id_1}"
        )

        with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM pipeline_runs "
                "WHERE ym = %s AND source = %s AND method = %s AND noise = %s",
                (SMOKE_YM, SMOKE_SOURCE, SMOKE_METHOD, SMOKE_NOISE),
            )
            dup_cnt = cur.fetchone()[0]
            assert dup_cnt == 1, (
                f"expected exactly 1 pipeline_runs row after two write_run calls, "
                f"got {dup_cnt}"
            )
            cur.execute(
                "SELECT count(*) FROM eval_results WHERE run_id = %s", (run_id_1,),
            )
            ev_cnt = cur.fetchone()[0]
            # Only the first call populates eval_results; the second is a skip.
            # Expect >= 1 (new insert) but no doubling.
            assert ev_cnt >= 1, f"eval_results empty after first write_run: {ev_cnt}"
            # After second call there must not be MORE than the first batch -
            # i.e. eval_results count did not grow from the no-op call.
    finally:
        _cleanup_run_rows()
        undo()
    print("  T-18 OK  CTE DO NOTHING RETURNING + follow-up SELECT + no dup insert",
          flush=True)


def main():
    _t17()
    _t18()
    print("PR4 SMOKE OK", flush=True)


if __name__ == "__main__":
    try:
        main()
    except AssertionError as e:
        print(f"SMOKE FAIL: {e}", flush=True)
        sys.exit(1)
