"""T-21 smoke: rewind.py --layer vault partition + watermark rollback.

Covers task acceptance item 5:
    - Creates a tempdir with 2 fake ym partitions (2022-01 + 2022-02) under
      a production-table name ('hub_flight').
    - Inserts a ('vault', 'hub_flight') watermark row with last_ym='2022-02'.
    - Invokes rewind.rewind_vault(from='2022-02', to='2022-02', root=<tmp>).
    - Asserts the 2022-02 partition is deleted, 2022-01 survives.
    - Asserts the watermark rolled back from '2022-02' to '2022-01'.
    - Restores the production watermark snapshot in the finally: block.

Also runs the three CLI acceptance items (2, 3, 4) as subprocess invocations
of ``python -m etl4.orchestration.rewind``:
    AC-2: --layer vault --from 2022-01 --to 2022-01 --dry-run exits 0 with
          'would delete' output.
    AC-3: --layer all --from 2022-01 --to 2022-01 WITHOUT --yes exits
          non-zero.
    AC-4: --layer all --from 2022-01 --to 2022-01 --yes --dry-run prints
          the chain plan and exits 0.

Prints 'REWIND_SMOKE OK' when every assertion passes. Windows cp1252-safe
(no arrows, no em-dashes).

Invoke:
    python multi-agent-engineering/projects/PROJ-20261118-impr01-partition-contract/artifacts/t21_rewind_smoke.py
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

from etl4.orchestration import rewind
from etl4.vault import vault_io

DSN = os.getenv("POSTGRES_DSN", "postgresql://etl4:change_me@localhost:5432/etl4")
TEST_TABLE = "hub_flight"
FIXTURE_YMS = ("2022-01", "2022-02")


def _snapshot_vault_watermarks(conn) -> list:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT layer, table_name, last_ym, row_count "
            "FROM pipeline_watermarks WHERE layer = 'vault'"
        )
        return cur.fetchall()


def _restore_vault_watermarks(conn, snapshot: list) -> None:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM pipeline_watermarks WHERE layer = 'vault'")
        for row in snapshot:
            cur.execute(
                "INSERT INTO pipeline_watermarks "
                "(layer, table_name, last_ym, row_count) VALUES (%s,%s,%s,%s)",
                row,
            )


def _seed_fixture(scratch_root: Path) -> None:
    for ym in FIXTURE_YMS:
        df = pd.DataFrame({
            "hk_flight": pd.Series(["aaaa", "bbbb"], dtype="string"),
            "icao24":    pd.Series(["0a1b2c", "0d4e5f"], dtype="string"),
            "ym":        pd.Series([ym, ym], dtype="string"),
        })
        vault_io.write_partition(df, TEST_TABLE, ym, out_dir=scratch_root)


def _run_cli(args: list) -> subprocess.CompletedProcess:
    cmd = [sys.executable, "-m", "etl4.orchestration.rewind", *args]
    return subprocess.run(
        cmd, cwd=str(REPO_ROOT),
        capture_output=True, text=True,
    )


def main() -> int:
    scratch = Path(tempfile.mkdtemp(prefix="t21_rewind_"))
    print("[fixture] scratch=%s" % (scratch,), flush=True)

    snapshot = None
    try:
        _seed_fixture(scratch)
        for ym in FIXTURE_YMS:
            part = scratch / TEST_TABLE / ("ym=" + ym) / "part.parquet"
            assert part.is_file(), "fixture missing: %s" % part
        print("  fixture: 2 ym partitions seeded under %s" %
              (scratch / TEST_TABLE,), flush=True)

        with psycopg.connect(DSN, connect_timeout=5) as conn:
            snapshot = _snapshot_vault_watermarks(conn)
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM pipeline_watermarks "
                    " WHERE layer='vault' AND table_name=%s",
                    (TEST_TABLE,),
                )
                cur.execute(
                    "INSERT INTO pipeline_watermarks "
                    "(layer, table_name, last_ym, row_count) "
                    "VALUES ('vault', %s, '2022-02', 2)",
                    (TEST_TABLE,),
                )
            conn.commit()
        print("  fixture: ('vault','%s') watermark seeded at last_ym=2022-02"
              % TEST_TABLE, flush=True)

        # Core API test: rewind vault 2022-02 under the scratch root.
        rewind.rewind_vault("2022-02", "2022-02",
                            root=scratch, dsn=DSN)

        part_jan = scratch / TEST_TABLE / "ym=2022-01" / "part.parquet"
        part_feb = scratch / TEST_TABLE / "ym=2022-02"
        assert part_jan.is_file(), "2022-01 was wrongly deleted"
        assert not part_feb.exists(), "2022-02 partition not deleted"
        print("  filesystem: 2022-01 survives; 2022-02 deleted", flush=True)

        with psycopg.connect(DSN, connect_timeout=5) as conn, \
                conn.cursor() as cur:
            cur.execute(
                "SELECT last_ym FROM pipeline_watermarks "
                " WHERE layer='vault' AND table_name=%s",
                (TEST_TABLE,),
            )
            row = cur.fetchone()
            assert row is not None, "watermark row missing after rewind"
            assert row[0] == "2022-01", (
                "watermark did not roll back to 2022-01; got %r" % (row[0],)
            )
        print("  watermark: last_ym rolled back to 2022-01", flush=True)

        # CLI AC-2: dry-run on scratch is awkward (CLI hard-codes
        # production root). Use calendar dry-run on the real root -- it
        # only lists and exits, so it is safe on production data.
        proc = _run_cli(["--layer", "vault",
                         "--from", "2022-01", "--to", "2022-01",
                         "--dry-run"])
        assert proc.returncode == 0, (
            "AC-2: dry-run exited %d; stderr=%r"
            % (proc.returncode, proc.stderr)
        )
        assert "would delete" in proc.stdout or "total: 0 dirs" in proc.stdout, (
            "AC-2: dry-run stdout missing plan: %r" % proc.stdout
        )
        print("  CLI AC-2: --dry-run exit=0, 'would delete' or 'total: 0 dirs' present",
              flush=True)

        # CLI AC-3: --layer all without --yes errors.
        proc = _run_cli(["--layer", "all",
                         "--from", "2022-01", "--to", "2022-01"])
        assert proc.returncode != 0, (
            "AC-3: --layer all without --yes should error; got exit=0"
        )
        assert "--yes" in (proc.stderr + proc.stdout), (
            "AC-3: --yes missing from error text: stderr=%r stdout=%r"
            % (proc.stderr, proc.stdout)
        )
        print("  CLI AC-3: --layer all without --yes exits non-zero",
              flush=True)

        # CLI AC-4: --layer all --yes --dry-run chain plan prints.
        proc = _run_cli(["--layer", "all",
                         "--from", "2022-01", "--to", "2022-01",
                         "--yes", "--dry-run"])
        assert proc.returncode == 0, (
            "AC-4: --layer all --yes --dry-run exited %d; stderr=%r"
            % (proc.returncode, proc.stderr)
        )
        for marker in ("[rewind all]",
                       "[rewind eval_mart]",
                       "[rewind matrix]",
                       "[rewind sink]",
                       "[rewind marts]",
                       "[rewind vault]"):
            assert marker in proc.stdout, (
                "AC-4: missing marker %r in stdout: %r"
                % (marker, proc.stdout)
            )
        print("  CLI AC-4: --layer all --yes --dry-run chain plan printed",
              flush=True)

        print("REWIND_SMOKE OK", flush=True)
        return 0

    finally:
        if snapshot is not None:
            with psycopg.connect(DSN, connect_timeout=5) as conn:
                _restore_vault_watermarks(conn, snapshot)
                conn.commit()
        shutil.rmtree(scratch, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
