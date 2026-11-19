"""T-12 smoke: migrate_partition_ym.py end-to-end.

Six self-contained cases over fake monoliths in a tmpdir. No real data
is touched. Watermark upserts are skipped (``--skip-watermark``) so the
smoke runs offline and leaves no DB side effects.

Cases
-----
1. ``no_monolith``: the script reports ``no_monolith`` for every table
   whose monolith does not exist.
2. ``dry_run_plan``: dry-run over a fake ``sat_state_observation``
   monolith with 3 months of data prints per-ym row counts without
   touching disk; st_mtime_ns of the monolith preserved.
3. ``NULL ym abort``: a fake monolith with a NULL ``time`` row triggers
   ``row_count_mismatch``; no ``ym=*`` dirs are left behind; monolith
   mtime preserved; exit code is 1.
4. ``migrate_happy_path``: a fake ``fact_trajectory_enriched`` monolith
   is migrated; every ym partition's row count sums to the monolith's
   footer row count; the ``ym`` column IS added inside each partition
   file body (matches T-08 writer convention, raw_vault.py line 543);
   ``_DONE`` sentinel is present beside every ``part.parquet``.
5. ``idempotency``: re-running case 4 reports ``already_migrated`` for
   every table; exit code is 0; partition mtime_ns is preserved on the
   second run.
6. ``postgres_sql_block``: stdout carries the ``fact_trajectory_enriched``
   ADD COLUMN SQL block verbatim.
7. ``unsupported_table``: a real run selecting ``link_flight_route``
   (or any table whose TableSpec carries an ``unsupported_reason``)
   exits 1 and skips watermark seeding. Dry-run on the same table is
   WARN but exits 0.

Invoke::

    python multi-agent-engineering/projects/PROJ-20261118-impr01-partition-contract/artifacts/t12_migration_smoke.py

Prints ``T12_SMOKE OK`` on success. Windows cp1252-safe.
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

import pandas as pd  # noqa: E402
import pyarrow as pa  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402


PY = sys.executable
MODULE = "etl4.scripts.migrate_partition_ym"


def _write_monolith(path: Path, table: pa.Table) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path, compression="snappy")


def _make_sat_state_monolith(root: Path, rows_per_month: dict[str, int]) -> Path:
    """Fake ``sat_state_observation`` monolith with int64 ``time`` epoch.

    Each month gets a deterministic epoch inside the month so
    ``ym_from_epoch`` maps rows back to the right partition.
    """
    anchors = {
        "2022-06": int(pd.Timestamp("2022-06-15", tz="UTC").timestamp()),
        "2022-07": int(pd.Timestamp("2022-07-15", tz="UTC").timestamp()),
        "2026-11": int(pd.Timestamp("2026-11-13", tz="UTC").timestamp()),
    }
    times: list[int] = []
    for ym, n in rows_per_month.items():
        if ym not in anchors:
            raise ValueError(f"no anchor for {ym}")
        times.extend([anchors[ym]] * n)
    tbl = pa.table({
        "hk_aircraft": pa.array(["a" * 32] * len(times), type=pa.string()),
        "time":        pa.array(times, type=pa.int64()),
        "lat":         pa.array([51.5] * len(times), type=pa.float64()),
    })
    p = root / "data" / "silver" / "vault" / "sat_state_observation" / "part.parquet"
    _write_monolith(p, tbl)
    return p


def _make_sat_state_monolith_with_null_time(root: Path) -> Path:
    """Fake monolith with one NULL ``time`` row (ym derivation loss case)."""
    t0 = int(pd.Timestamp("2022-06-15", tz="UTC").timestamp())
    t1 = int(pd.Timestamp("2022-07-15", tz="UTC").timestamp())
    tbl = pa.table({
        "hk_aircraft": pa.array(["a" * 32, "b" * 32, "c" * 32], type=pa.string()),
        "time":        pa.array([t0, None, t1], type=pa.int64()),
    })
    p = root / "data" / "silver" / "vault" / "sat_state_observation" / "part.parquet"
    _write_monolith(p, tbl)
    return p


def _make_fact_traj_monolith(root: Path) -> Path:
    t_jun = int(pd.Timestamp("2022-06-15", tz="UTC").timestamp())
    t_jul = int(pd.Timestamp("2022-07-15", tz="UTC").timestamp())
    t_nov = int(pd.Timestamp("2026-11-13", tz="UTC").timestamp())
    tbl = pa.table({
        "hk_aircraft": pa.array(["a" * 32] * 6, type=pa.string()),
        "icao24":      pa.array(["0a1b2c"] * 6, type=pa.string()),
        "time":        pa.array(
            [t_jun, t_jun + 1, t_jul, t_jul + 1, t_nov, t_nov + 1],
            type=pa.int64(),
        ),
        "lat":         pa.array([51.5] * 6, type=pa.float64()),
    })
    p = root / "data" / "gold" / "marts" / "fact_trajectory_enriched" / "part.parquet"
    _write_monolith(p, tbl)
    return p


def _make_hub_flight_monolith(root: Path) -> Path:
    tbl = pa.table({
        "hk_flight": pa.array(["h1", "h2", "h3"], type=pa.string()),
        "callsign":  pa.array(["ABC", "DEF", "GHI"], type=pa.string()),
        "day":       pa.array(
            [pd.Timestamp("2022-06-15").date(),
             pd.Timestamp("2022-07-10").date(),
             pd.Timestamp("2026-11-05").date()],
            type=pa.date32(),
        ),
    })
    p = root / "data" / "silver" / "vault" / "hub_flight" / "part.parquet"
    _write_monolith(p, tbl)
    return p


def _run(args: list[str], cwd: Path) -> subprocess.CompletedProcess:
    cmd = [PY, "-m", MODULE] + args + ["--skip-watermark"]
    env = dict(os.environ)
    env.pop("POSTGRES_DSN", None)
    return subprocess.run(
        cmd,
        cwd=str(cwd),
        env=env,
        capture_output=True,
        text=True,
    )


def _case1_no_monolith(root: Path) -> None:
    res = _run(
        ["--vault-dir", str(root / "data/silver/vault"),
         "--marts-dir", str(root / "data/gold/marts")],
        cwd=REPO_ROOT,
    )
    if res.returncode != 0:
        raise AssertionError(
            "case1 no_monolith: expected exit=0, got %d; stderr=%s"
            % (res.returncode, res.stderr)
        )
    if res.stdout.count("no_monolith") < 7:
        raise AssertionError(
            "case1 no_monolith: expected 7 'no_monolith' lines; stdout=%s"
            % (res.stdout,)
        )
    print("[case1] no_monolith OK (exit=0, 7/7 tables reported no_monolith)", flush=True)


def _case2_dry_run(root: Path) -> None:
    _make_sat_state_monolith(root, {"2022-06": 3, "2022-07": 2, "2026-11": 1})
    monolith = root / "data" / "silver" / "vault" / "sat_state_observation" / "part.parquet"
    mtime_before = monolith.stat().st_mtime_ns
    res = _run(
        ["--tables", "sat_state_observation", "--dry-run",
         "--vault-dir", str(root / "data/silver/vault"),
         "--marts-dir", str(root / "data/gold/marts")],
        cwd=REPO_ROOT,
    )
    if res.returncode != 0:
        raise AssertionError("case2 dry_run: exit=%d stderr=%s" % (res.returncode, res.stderr))
    for ym, n in [("2022-06", 3), ("2022-07", 2), ("2026-11", 1)]:
        line = "ym=%s rows=%d" % (ym, n)
        if line not in res.stdout:
            raise AssertionError(
                "case2 dry_run missing %r in stdout; got:\n%s" % (line, res.stdout)
            )
    tbl_dir = root / "data" / "silver" / "vault" / "sat_state_observation"
    leftover = list(tbl_dir.glob("ym=*"))
    if leftover:
        raise AssertionError("case2 dry_run left ym dirs behind: %s" % leftover)
    if monolith.stat().st_mtime_ns != mtime_before:
        raise AssertionError("case2 dry_run modified the monolith mtime")
    print("[case2] dry_run OK (3 months, counts verified, no fs change)", flush=True)
    monolith.unlink()


def _case3_null_abort(root: Path) -> None:
    _make_sat_state_monolith_with_null_time(root)
    tbl_dir = root / "data" / "silver" / "vault" / "sat_state_observation"
    mono = tbl_dir / "part.parquet"
    mtime_before = mono.stat().st_mtime_ns
    res = _run(
        ["--tables", "sat_state_observation",
         "--vault-dir", str(root / "data/silver/vault"),
         "--marts-dir", str(root / "data/gold/marts")],
        cwd=REPO_ROOT,
    )
    if res.returncode == 0:
        raise AssertionError(
            "case3 NULL abort: expected non-zero exit, got 0; stdout=%s stderr=%s"
            % (res.stdout, res.stderr)
        )
    if "row_count_mismatch" not in res.stdout:
        raise AssertionError(
            "case3 NULL abort: expected 'row_count_mismatch' in stdout; got:\n%s"
            % (res.stdout,)
        )
    if "NULL time" not in res.stdout:
        raise AssertionError(
            "case3 NULL abort: expected 'NULL time' in detail; got:\n%s"
            % (res.stdout,)
        )
    leftover = list(tbl_dir.glob("ym=*"))
    if leftover:
        raise AssertionError("case3 NULL abort left ym dirs behind: %s" % leftover)
    tmps = list(tbl_dir.rglob("*.tmp"))
    if tmps:
        raise AssertionError("case3 NULL abort left tmp files: %s" % tmps)
    if mono.stat().st_mtime_ns != mtime_before:
        raise AssertionError("case3 NULL abort modified monolith mtime")
    print("[case3] NULL abort OK (exit=1, row_count_mismatch, no partitions written, mtime preserved)", flush=True)
    mono.unlink()


def _case4_migrate_happy(root: Path) -> None:
    monolith = _make_fact_traj_monolith(root)
    expected = pq.ParquetFile(monolith).metadata.num_rows
    res = _run(
        ["--tables", "fact_trajectory_enriched",
         "--vault-dir", str(root / "data/silver/vault"),
         "--marts-dir", str(root / "data/gold/marts")],
        cwd=REPO_ROOT,
    )
    if res.returncode != 0:
        raise AssertionError(
            "case4 migrate: exit=%d stderr=%s stdout=%s"
            % (res.returncode, res.stderr, res.stdout)
        )
    tbl_dir = root / "data" / "gold" / "marts" / "fact_trajectory_enriched"
    yms = sorted(p.name.split("=", 1)[1] for p in tbl_dir.glob("ym=*"))
    if yms != ["2022-06", "2022-07", "2026-11"]:
        raise AssertionError("case4 migrate: unexpected ym dirs %s" % yms)
    total = 0
    for ym in yms:
        fp = tbl_dir / f"ym={ym}" / "part.parquet"
        if not fp.is_file():
            raise AssertionError("case4 migrate: missing %s" % fp)
        done = tbl_dir / f"ym={ym}" / "_DONE"
        if not done.is_file():
            raise AssertionError("case4 migrate: missing _DONE sentinel %s" % done)
        tbl_pa = pq.read_table(fp)
        if "ym" not in tbl_pa.column_names:
            raise AssertionError(
                "case4 migrate: 'ym' column absent from file body for %s" % fp
            )
        ym_vals = set(tbl_pa["ym"].to_pylist())
        if ym_vals != {ym}:
            raise AssertionError(
                "case4 migrate: in-file 'ym' column has %s but dir is ym=%s"
                % (ym_vals, ym)
            )
        total += tbl_pa.num_rows
    if total != expected:
        raise AssertionError(
            "case4 migrate: partition sum %d != monolith %d" % (total, expected)
        )
    tmps = list(tbl_dir.rglob("*.tmp"))
    if tmps:
        raise AssertionError("case4 migrate left tmp files: %s" % tmps)
    print(
        "[case4] happy path OK (3 partitions, sum=%d == monolith=%d, "
        "in-file ym OK, _DONE sentinel present)"
        % (total, expected), flush=True,
    )


def _case5_idempotent(root: Path) -> None:
    """Rerun case 4 inputs. Everything should report already_migrated.

    mtime is asserted only on the ``ym=*/part.parquet`` files. The
    top-level monolith is tracked separately in case3; it is not
    rewritten, but Windows ``Path.stat`` on an untouched file can still
    bump ``st_mtime_ns`` if some other process (e.g. a virus scanner)
    touches access metadata, so we skip it here. The ym partitions are
    inside a newly-created directory tree with no external observers.
    """
    tbl_dir = root / "data" / "gold" / "marts" / "fact_trajectory_enriched"
    part_mtimes = {
        str(p.relative_to(tbl_dir)): p.stat().st_mtime_ns
        for p in sorted(tbl_dir.glob("ym=*/part.parquet"))
    }
    res = _run(
        ["--tables", "fact_trajectory_enriched",
         "--vault-dir", str(root / "data/silver/vault"),
         "--marts-dir", str(root / "data/gold/marts")],
        cwd=REPO_ROOT,
    )
    if res.returncode != 0:
        raise AssertionError(
            "case5 idempotent: exit=%d stderr=%s" % (res.returncode, res.stderr)
        )
    if "already_migrated" not in res.stdout:
        raise AssertionError(
            "case5 idempotent: expected 'already_migrated'; got:\n%s" % res.stdout
        )
    for p in sorted(tbl_dir.glob("ym=*/part.parquet")):
        rel = str(p.relative_to(tbl_dir))
        if p.stat().st_mtime_ns != part_mtimes.get(rel):
            raise AssertionError(
                "case5 idempotent: mtime drift on %s" % p
            )
    print("[case5] idempotent OK (already_migrated, ym partition mtime preserved)", flush=True)


def _case7_unsupported(root: Path) -> None:
    """A table flagged ``unsupported_reason`` fails a real run (exit 1,
    no watermark). Dry-run on the same table is WARN (exit 0).

    We write a fake ``link_flight_route`` monolith (its real
    unsupported_reason references the real column gap). The script must
    reject it on a real run WITHOUT reading the monolith footer for a
    schema check (the unsupported branch is pre-schema).
    """
    tbl = pa.table({
        "hk_flight_route": pa.array(["r1", "r2", "r3"], type=pa.string()),
    })
    p = root / "data" / "silver" / "vault" / "link_flight_route" / "part.parquet"
    _write_monolith(p, tbl)
    try:
        real = _run(
            ["--tables", "link_flight_route",
             "--vault-dir", str(root / "data/silver/vault"),
             "--marts-dir", str(root / "data/gold/marts")],
            cwd=REPO_ROOT,
        )
        if real.returncode == 0:
            raise AssertionError(
                "case7 unsupported real-run: expected exit 1, got 0; stdout=%s"
                % real.stdout
            )
        if "schema_mismatch" not in real.stdout:
            raise AssertionError(
                "case7 unsupported real-run: expected 'schema_mismatch' in "
                "stdout; got:\n%s" % real.stdout
            )
        if "FAIL" not in real.stdout:
            raise AssertionError(
                "case7 unsupported real-run: expected 'FAIL' tag; got:\n%s"
                % real.stdout
            )
        dr = _run(
            ["--tables", "link_flight_route", "--dry-run",
             "--vault-dir", str(root / "data/silver/vault"),
             "--marts-dir", str(root / "data/gold/marts")],
            cwd=REPO_ROOT,
        )
        if dr.returncode != 0:
            raise AssertionError(
                "case7 unsupported dry-run: expected exit 0, got %d; stdout=%s"
                % (dr.returncode, dr.stdout)
            )
        if "WARN" not in dr.stdout:
            raise AssertionError(
                "case7 unsupported dry-run: expected 'WARN' tag; got:\n%s"
                % dr.stdout
            )
        tbl_dir = p.parent
        if list(tbl_dir.glob("ym=*")):
            raise AssertionError(
                "case7 unsupported left ym dirs behind: %s"
                % list(tbl_dir.glob("ym=*"))
            )
        print(
            "[case7] unsupported OK (real-run exit=1, dry-run exit=0, "
            "no ym dirs written)", flush=True,
        )
    finally:
        if p.exists():
            p.unlink()


def _case6_sql_block(root: Path) -> None:
    """Final run's stdout must carry the Postgres migration SQL block."""
    res = _run(
        ["--tables", "fact_trajectory_enriched",
         "--vault-dir", str(root / "data/silver/vault"),
         "--marts-dir", str(root / "data/gold/marts")],
        cwd=REPO_ROOT,
    )
    for needle in (
        "---- Postgres-side migration SQL (operator applies) ----",
        "ADD COLUMN IF NOT EXISTS ym TEXT",
        "to_char(to_timestamp(time), 'YYYY-MM')",
        "CREATE INDEX IF NOT EXISTS idx_fact_traj_ym",
        "---- end SQL ----",
    ):
        if needle not in res.stdout:
            raise AssertionError(
                "case6 sql block: missing %r in stdout; got:\n%s"
                % (needle, res.stdout)
            )
    print("[case6] SQL block OK (ADD COLUMN + backfill + index all present)", flush=True)


def main() -> int:
    tmp = tempfile.mkdtemp(prefix="t12_migration_")
    try:
        root = Path(tmp)
        (root / "data" / "silver" / "vault").mkdir(parents=True, exist_ok=True)
        (root / "data" / "gold" / "marts").mkdir(parents=True, exist_ok=True)

        _case1_no_monolith(root)
        _case2_dry_run(root)
        _case3_null_abort(root)
        _case4_migrate_happy(root)
        _case5_idempotent(root)
        _case6_sql_block(root)
        _case7_unsupported(root)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    print("T12_SMOKE OK", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
