"""T-22 smoke: run_e2e --ym wiring + pre-flight drift check.

Covers acceptance criteria for T-22 (architecture.md section 5, PR5):

  1. Module parses as Python (ast.parse)
  2. --ym required when --source in (s3, zenodo, rest); argparse errors with
     exit code 2 and an actionable message
  3. --source synthetic defaults --ym to SYN-002A inside run_matrix.py (the
     run_e2e main does NOT substitute; T-20 owns the default)
  4. check_watermark_vs_filesystem passes in a clean tmpdir (no partitions,
     no watermark rows) when the Postgres has the pipeline_watermarks table
     but no 'vault' rows
  5. check_watermark_vs_filesystem aborts when a ym=2099-01 dir is planted
     with no watermark row (drift scenario 1)
  6. check_watermark_vs_filesystem aborts when watermark exists but the
     filesystem has zero ym dirs (drift scenario 2)
  7. check_watermark_vs_filesystem aborts when there is a hole in the
     contiguous range from earliest ym to last_ym (drift scenario 3)

The smoke exercises check_watermark_vs_filesystem against a tmpdir so it
never touches data/silver/vault; the function accepts a `root` kwarg
precisely so the check is testable in isolation.

Watermark pollution guard: every path that INSERTs into pipeline_watermarks
uses the sentinel table_name 'hub_flight_t22_smoke' so we never collide
with production ('hub_flight', 'link_flight_aircraft', ...) rows. Cleanup
in a finally block.

Prints "T22_SMOKE OK" when all assertions pass. Windows cp1252-safe
(no arrows, no em-dashes).

Invoke:
    python multi-agent-engineering/projects/PROJ-20261118-impr01-partition-contract/artifacts/t22_run_e2e_smoke.py
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO_ROOT))

import pandas as pd
import psycopg

from etl4.run_e2e import check_watermark_vs_filesystem, _months_between

DSN = os.getenv("POSTGRES_DSN", "postgresql://etl4:change_me@localhost:5432/etl4")
PY = sys.executable


def _write_parquet(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def _plant_partition(root: Path, table: str, ym: str) -> None:
    out = root / table / ("ym=" + ym) / "part.parquet"
    _write_parquet(out, pd.DataFrame({"hk_flight": pd.array([], dtype="string")}))


def _t22_argparse_errors() -> None:
    """AC-2: --source s3 --only sink without --ym exits non-zero."""
    r = subprocess.run(
        [PY, "etl4/run_e2e.py", "--source", "s3", "--only", "sink"],
        cwd=str(REPO_ROOT), capture_output=True, text=True,
    )
    assert r.returncode == 2, (
        "expected exit 2 for s3+no-ym; got %d; stderr=%r" % (r.returncode, r.stderr)
    )
    assert "--ym required when --source is one of" in r.stderr, (
        "expected '--ym required' in stderr; got %r" % r.stderr
    )
    print("  AC-2 OK  --source s3 --only sink errors with exit=2 and actionable message")


def _t22_sink_requires_calendar_ym() -> None:
    """Sink stage requires calendar YYYY-MM even with --source synthetic."""
    r = subprocess.run(
        [PY, "etl4/run_e2e.py", "--source", "synthetic", "--only", "sink"],
        cwd=str(REPO_ROOT), capture_output=True, text=True,
    )
    assert r.returncode == 2, (
        "expected exit 2 for sink without ym; got %d; stderr=%r"
        % (r.returncode, r.stderr)
    )
    assert "required for stage_sink" in r.stderr, (
        "expected 'required for stage_sink' mention; got %r" % r.stderr
    )
    print("  AC-2b OK  stage_sink rejects missing/SYN ym at argparse layer")


def _delete_sentinel_watermark(table: str) -> None:
    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "DELETE FROM pipeline_watermarks WHERE layer='vault' "
            "AND table_name=%s",
            (table,),
        )


def _t22_drift_clean(tmp: Path) -> None:
    """AC-4: sentinel table + clean tmpdir + no watermark row -> passes.

    Patches VAULT_PARTITIONED_TABLES to a sentinel name so the check is
    insensitive to whether production ('vault','hub_flight') has advanced.
    """
    sentinel_table = "hub_flight_t22_smoke"
    import etl4.run_e2e as rm
    original = rm.VAULT_PARTITIONED_TABLES
    rm.VAULT_PARTITIONED_TABLES = (sentinel_table,)
    _delete_sentinel_watermark(sentinel_table)
    try:
        check_watermark_vs_filesystem(root=tmp)
        print("  AC-4 OK  clean tmpdir drift check passes (sentinel)")
    finally:
        rm.VAULT_PARTITIONED_TABLES = original


def _t22_drift_fs_no_watermark(tmp: Path) -> None:
    """AC-5 scenario 1: planted sentinel ym=2099-01 with no watermark row aborts."""
    sentinel_table = "hub_flight_t22_smoke"
    import etl4.run_e2e as rm
    original = rm.VAULT_PARTITIONED_TABLES
    rm.VAULT_PARTITIONED_TABLES = (sentinel_table,)
    _delete_sentinel_watermark(sentinel_table)
    _plant_partition(tmp, sentinel_table, "2099-01")
    try:
        try:
            check_watermark_vs_filesystem(root=tmp)
        except SystemExit as e:
            msg = str(e)
            assert "watermark drift" in msg and "no watermark row" in msg, (
                "unexpected error message: %r" % msg
            )
            print("  AC-5 OK  planted ym=2099-01 without watermark row aborts")
            return
        raise AssertionError("expected SystemExit, got clean return")
    finally:
        rm.VAULT_PARTITIONED_TABLES = original
        shutil.rmtree(tmp / sentinel_table, ignore_errors=True)


def _t22_drift_watermark_no_fs(tmp: Path) -> None:
    """AC-5 scenario 2: watermark row present, filesystem empty, aborts."""
    sentinel_table = "hub_flight_t22_smoke"
    # Monkeypatch VAULT_PARTITIONED_TABLES by invoking check against a frozen
    # module alias is overkill; instead patch the module tuple for the call.
    import etl4.run_e2e as rm
    original = rm.VAULT_PARTITIONED_TABLES
    rm.VAULT_PARTITIONED_TABLES = (sentinel_table,)
    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO pipeline_watermarks (layer, table_name, last_ym, row_count) "
            "VALUES ('vault', %s, '2099-05', 0) "
            "ON CONFLICT (layer, table_name) DO UPDATE "
            "   SET last_ym='2099-05', row_count=0, updated_at=now()",
            (sentinel_table,),
        )
    try:
        try:
            check_watermark_vs_filesystem(root=tmp)
        except SystemExit as e:
            msg = str(e)
            assert "zero ym dirs on disk" in msg, (
                "unexpected error message: %r" % msg
            )
            print("  AC-5b OK  watermark row with empty filesystem aborts")
            return
        raise AssertionError("expected SystemExit, got clean return")
    finally:
        rm.VAULT_PARTITIONED_TABLES = original
        with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(
                "DELETE FROM pipeline_watermarks WHERE layer='vault' "
                "AND table_name=%s",
                (sentinel_table,),
            )


def _t22_drift_hole(tmp: Path) -> None:
    """AC-5 scenario 3: watermark says last_ym=2099-05, FS has 01 + 03 + 05,
    missing 02 + 04 -> hole, aborts.
    """
    sentinel_table = "hub_flight_t22_smoke"
    import etl4.run_e2e as rm
    original = rm.VAULT_PARTITIONED_TABLES
    rm.VAULT_PARTITIONED_TABLES = (sentinel_table,)
    for ym in ("2099-01", "2099-03", "2099-05"):
        _plant_partition(tmp, sentinel_table, ym)
    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO pipeline_watermarks (layer, table_name, last_ym, row_count) "
            "VALUES ('vault', %s, '2099-05', 0) "
            "ON CONFLICT (layer, table_name) DO UPDATE "
            "   SET last_ym='2099-05', row_count=0, updated_at=now()",
            (sentinel_table,),
        )
    try:
        try:
            check_watermark_vs_filesystem(root=tmp)
        except SystemExit as e:
            msg = str(e)
            assert "watermark drift on" in msg and "missing=" in msg, (
                "unexpected error message: %r" % msg
            )
            print("  AC-5c OK  hole in contiguous range aborts")
            return
        raise AssertionError("expected SystemExit, got clean return")
    finally:
        rm.VAULT_PARTITIONED_TABLES = original
        shutil.rmtree(tmp / sentinel_table, ignore_errors=True)
        with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(
                "DELETE FROM pipeline_watermarks WHERE layer='vault' "
                "AND table_name=%s",
                (sentinel_table,),
            )


def _t22_months_between() -> None:
    """Sanity: _months_between is contiguous + inclusive."""
    assert _months_between("2022-11", "2023-02") == {
        "2022-11", "2022-12", "2023-01", "2023-02",
    }
    assert _months_between("2022-06", "2022-06") == {"2022-06"}
    print("  helper OK  _months_between inclusive year rollover")


def main() -> None:
    tmp = Path(tempfile.mkdtemp(prefix="t22_drift_"))
    try:
        _t22_argparse_errors()
        _t22_sink_requires_calendar_ym()
        _t22_months_between()
        _t22_drift_clean(tmp)
        _t22_drift_fs_no_watermark(tmp)
        _t22_drift_watermark_no_fs(tmp)
        _t22_drift_hole(tmp)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    print("T22_SMOKE OK")


if __name__ == "__main__":
    main()
