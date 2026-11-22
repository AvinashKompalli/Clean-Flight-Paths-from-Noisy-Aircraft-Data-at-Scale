"""Smoke: per-ym rewrite of sat_state_observation (pandas path).

Covers four scenarios:

  1. Bootstrap: builds ym=YYYY-MM/part.parquet for sat_state_observation
     across two real bronze months (date=2022-06-27 and date=2026-11-12),
     with a ``_DONE`` sentinel beside each ``part.parquet``.
  2. Watermark-gated resume: back-to-back run is a no-op (mtimes
     unchanged); watermark bypasses the sentinel branch entirely.
  3. Sentinel-gated resume: delete the sat watermark only, keep the
     partitions and their ``_DONE`` sentinels. A correct implementation
     must reaffirm the watermark from the sentinel gate without
     rewriting any partition. This guards against a crash between a
     successful partition write and its watermark commit.
  4. Watermark pre-filter: with the sat watermark preset to
     ``last_ym=2022-06``, running against the same two-month bronze
     produces ONLY the ``ym=2026-11`` partition, and
     ``_group_state_files_by_ym`` returns only the 2026-11 file set.

Fixture strategy:
  Real bronze. Source dirs:
    data/bronze/raw_states/date=2022-06-27/hour=0/part-0.parquet
    data/bronze/raw_states/date=2026-11-12/hour=17/part-1794504578.parquet
  Both are real OpenSky slices. ``head(5000)`` keeps per-ym build cheap.
  Fixture is copied into a scratch directory with the same ``date=/hour=``
  path layout so ``extract_ym_from_path`` resolves identically to the
  production tree.

  ``flightlist_dir`` is an empty scratch dir so the flightlist loop no-ops
  and we do not need to touch 48 real monthly files or the four flightlist
  vault watermarks.

Watermark pollution guard:
  pipeline_watermarks is a global table. All ``('vault', *)`` rows are
  snapshotted before the run and restored in the ``finally`` block.

Prints "SAT_SMOKE OK" when all assertions pass. Windows cp1252-safe.

Invoke:
    python multi-agent-engineering/projects/PROJ-20261118-impr01-partition-contract/artifacts/t10_sat_smoke.py
"""
from __future__ import annotations

import os
import shutil
import sys
import tempfile
import time as _time_mod
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO_ROOT))

import pandas as pd
import psycopg
import pyarrow.parquet as pq

from etl4.vault import vault_io, watermark
from etl4.vault.raw_vault import (
    _group_state_files_by_ym,
    build_raw_vault,
)

DSN = os.getenv("POSTGRES_DSN", "postgresql://etl4:change_me@localhost:5432/etl4")

FIXTURE_MONTHS = [
    {
        "date": "2022-06-27",
        "hour": "0",
        "src": REPO_ROOT / "data" / "bronze" / "raw_states" /
               "date=2022-06-27" / "hour=0" / "part-0.parquet",
        "ym":  "2022-06",
    },
    {
        "date": "2026-11-12",
        "hour": "17",
        "src": REPO_ROOT / "data" / "bronze" / "raw_states" /
               "date=2026-11-12" / "hour=17" / "part-1794504578.parquet",
        "ym":  "2026-11",
    },
]


def _snapshot_vault_watermarks(conn) -> list[tuple]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT layer, table_name, last_ym, row_count "
            "FROM pipeline_watermarks WHERE layer = 'vault'"
        )
        return cur.fetchall()


def _restore_vault_watermarks(conn, snapshot: list[tuple]) -> None:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM pipeline_watermarks WHERE layer = 'vault'")
        for row in snapshot:
            cur.execute(
                "INSERT INTO pipeline_watermarks "
                "(layer, table_name, last_ym, row_count) VALUES (%s,%s,%s,%s)",
                row,
            )


def _set_sat_watermark(conn, last_ym: str, row_count: int = 0) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO pipeline_watermarks "
            "(layer, table_name, last_ym, row_count) "
            "VALUES ('vault','sat_state_observation', %s, %s) "
            "ON CONFLICT (layer, table_name) DO UPDATE "
            "   SET last_ym=EXCLUDED.last_ym, row_count=EXCLUDED.row_count, "
            "       updated_at=now()",
            (last_ym, row_count),
        )


def _build_fixture(scratch: Path, n_rows: int = 5000) -> Path:
    """Copy head(n_rows) of two real monthly bronze files into scratch with
    the canonical ``date=/hour=`` path layout."""
    bronze_dir = scratch / "bronze" / "raw_states"
    for m in FIXTURE_MONTHS:
        src = m["src"]
        if not src.exists():
            raise FileNotFoundError(f"fixture source missing: {src}")
        dst_dir = bronze_dir / f"date={m['date']}" / f"hour={m['hour']}"
        dst_dir.mkdir(parents=True, exist_ok=True)
        df = pd.read_parquet(src).head(n_rows)
        df.to_parquet(dst_dir / src.name, index=False)
    return bronze_dir


def _vault_watermark_rows(conn) -> list[tuple]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT table_name, last_ym, row_count FROM pipeline_watermarks "
            "WHERE layer = 'vault' ORDER BY table_name"
        )
        return cur.fetchall()


def _sat_mtimes(out_dir: Path) -> dict:
    out: dict = {}
    sat_dir = out_dir / "sat_state_observation"
    for ym_dir in sorted(sat_dir.glob("ym=*")):
        p = ym_dir / "part.parquet"
        if p.is_file():
            out[ym_dir.name] = p.stat().st_mtime_ns
    return out


def run():
    scratch = Path(tempfile.mkdtemp(prefix="t10_sat_smoke_"))

    with psycopg.connect(DSN, autocommit=True) as conn:
        snapshot = _snapshot_vault_watermarks(conn)
    try:
        with psycopg.connect(DSN, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM pipeline_watermarks WHERE layer='vault'")

        bronze_dir = _build_fixture(scratch, n_rows=5000)

        # Empty flightlist dir forces the flightlist loop to a no-op so we
        # exercise only the sat path.
        fl_dir = scratch / "flightlists" / "v23.00"
        fl_dir.mkdir(parents=True, exist_ok=True)

        ym_list = sorted(m["ym"] for m in FIXTURE_MONTHS)

        # ---- RUN 1: bootstrap ------------------------------------------------
        out1 = scratch / "silver_run1" / "vault"
        out1.mkdir(parents=True)
        print("--- RUN 1 (bootstrap, both months) ---", flush=True)
        t0 = _time_mod.perf_counter()
        counts1 = build_raw_vault(
            flightlist_dir=fl_dir,
            states_dir=bronze_dir,
            out_dir=out1,
            include_states=True,
            engine="pandas",
        )
        t_run1 = _time_mod.perf_counter() - t0

        for ym in ym_list:
            ym_dir = out1 / "sat_state_observation" / f"ym={ym}"
            assert (ym_dir / "part.parquet").is_file(), f"missing {ym_dir}/part.parquet"
            assert (ym_dir / "_DONE").is_file(), f"missing {ym_dir}/_DONE"
            back = pq.read_table(ym_dir / "part.parquet").to_pandas()
            assert "ym" in back.columns, f"ym column missing from {ym_dir} file body"
            assert set(back["ym"].dropna().unique()) == {ym}, (
                f"{ym_dir}: in-file ym does not match path: "
                f"{sorted(set(back['ym'].dropna().unique()))}"
            )
            assert len(back) > 0

        assert counts1["sat_state_observation"] > 0, (
            "sat_state_observation count should be > 0 after bootstrap"
        )

        with psycopg.connect(DSN, autocommit=True) as conn:
            wm = {r[0]: r[1] for r in _vault_watermark_rows(conn)}
        assert wm.get("sat_state_observation") == ym_list[-1], (
            f"sat watermark expected {ym_list[-1]}, got {wm.get('sat_state_observation')!r}"
        )

        mt1 = _sat_mtimes(out1)
        assert set(mt1.keys()) == {f"ym={ym}" for ym in ym_list}, (
            f"expected partitions {ym_list}, got {sorted(mt1)}"
        )

        # ---- RUN 2: watermark-gated resume (same out_dir, no bronze change)
        print("--- RUN 2 (watermark resume, expected no-op) ---", flush=True)
        t0 = _time_mod.perf_counter()
        counts2 = build_raw_vault(
            flightlist_dir=fl_dir,
            states_dir=bronze_dir,
            out_dir=out1,
            include_states=True,
            engine="pandas",
        )
        t_run2 = _time_mod.perf_counter() - t0

        assert counts2["sat_state_observation"] == counts1["sat_state_observation"], (
            f"resume regression: sat rows {counts1['sat_state_observation']} "
            f"-> {counts2['sat_state_observation']}"
        )
        mt2 = _sat_mtimes(out1)
        for k in mt1:
            assert mt1[k] == mt2[k], f"mtime changed on resume for {k}"

        # ---- RUN 2b: sentinel-gated resume ---------------------------------
        # Delete the sat watermark only; keep out1 and its _DONE sentinels.
        # Correct resume must reaffirm the watermark from the sentinel gate
        # so a crash-between-write-and-commit does not permanently strand
        # a materialised partition behind a stale watermark.
        with psycopg.connect(DSN, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM pipeline_watermarks "
                    "WHERE layer='vault' AND table_name='sat_state_observation'"
                )
        print("--- RUN 2b (sentinel resume, watermark deleted) ---", flush=True)
        t0 = _time_mod.perf_counter()
        counts2b = build_raw_vault(
            flightlist_dir=fl_dir,
            states_dir=bronze_dir,
            out_dir=out1,
            include_states=True,
            engine="pandas",
        )
        t_run2b = _time_mod.perf_counter() - t0
        assert counts2b["sat_state_observation"] == counts1["sat_state_observation"], (
            f"sentinel-resume regression: sat rows {counts1['sat_state_observation']} "
            f"-> {counts2b['sat_state_observation']}"
        )
        mt2b = _sat_mtimes(out1)
        for k in mt1:
            assert mt1[k] == mt2b[k], (
                f"sentinel-resume rewrote {k} (mtime changed)"
            )
        with psycopg.connect(DSN, autocommit=True) as conn:
            wm2b = {r[0]: r[1] for r in _vault_watermark_rows(conn)}
        assert wm2b.get("sat_state_observation") == ym_list[-1], (
            "sentinel-resume failed to reaffirm sat watermark: "
            f"expected {ym_list[-1]}, got {wm2b.get('sat_state_observation')!r}"
        )

        # ---- RUN 3: watermark-driven partial filter -----------------------
        # Preset sat watermark to 2022-06. Fresh out_dir so the _DONE gate
        # is empty and only the watermark filter can prevent re-processing
        # that month.
        with psycopg.connect(DSN, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM pipeline_watermarks WHERE layer='vault'")
            _set_sat_watermark(conn, "2022-06")

        # First verify the file-prefilter helper agrees with the filter.
        hub_files = _group_state_files_by_ym(bronze_dir, "2022-06")
        assert set(hub_files.keys()) == {"2026-11"}, (
            f"prefilter: expected only 2026-11, got {sorted(hub_files)}"
        )

        out3 = scratch / "silver_run3" / "vault"
        out3.mkdir(parents=True)
        print("--- RUN 3 (watermark=2022-06, fresh out_dir) ---", flush=True)
        t0 = _time_mod.perf_counter()
        counts3 = build_raw_vault(
            flightlist_dir=fl_dir,
            states_dir=bronze_dir,
            out_dir=out3,
            include_states=True,
            engine="pandas",
        )
        t_run3 = _time_mod.perf_counter() - t0

        sat3_dir = out3 / "sat_state_observation"
        yms_on_disk = sorted(d.name for d in sat3_dir.glob("ym=*"))
        assert yms_on_disk == ["ym=2026-11"], (
            f"regression: expected only ym=2026-11 under {sat3_dir}, "
            f"got {yms_on_disk}"
        )
        # Row count equality: run 3 sat rows should match run 1's 2026-11 only.
        rc_2026 = vault_io.row_count(
            "sat_state_observation", ym_filter="2026-11", root=out1,
        )
        assert counts3["sat_state_observation"] == rc_2026, (
            f"row count: run3 sat={counts3['sat_state_observation']}, "
            f"run1 2026-11={rc_2026}"
        )

        # Watermark advanced to 2026-11 (not 2022-06 anymore).
        with psycopg.connect(DSN, autocommit=True) as conn:
            wm3 = {r[0]: r[1] for r in _vault_watermark_rows(conn)}
        assert wm3.get("sat_state_observation") == "2026-11", (
            f"watermark expected 2026-11, got {wm3.get('sat_state_observation')!r}"
        )

        # ---- Evidence dump --------------------------------------------------
        print(f"timings: run1={t_run1:.3f}s run2(wm)={t_run2:.3f}s "
              f"run2b(sentinel)={t_run2b:.3f}s run3(filter)={t_run3:.3f}s",
              flush=True)
        print("--- pipeline_watermarks WHERE layer='vault' (post-run3) ---",
              flush=True)
        with psycopg.connect(DSN, autocommit=True) as conn:
            for row in _vault_watermark_rows(conn):
                print(f"  layer=vault table_name={row[0]} last_ym={row[1]} "
                      f"row_count={row[2]}", flush=True)

        print(f"sat_state_observation rows: run1={counts1['sat_state_observation']:,} "
              f"run2={counts2['sat_state_observation']:,} "
              f"run2b={counts2b['sat_state_observation']:,} "
              f"run3={counts3['sat_state_observation']:,}", flush=True)
        print(f"run1 partitions: {sorted(mt1.keys())}", flush=True)
        print(f"run3 partitions: {yms_on_disk}", flush=True)

        print("SAT_SMOKE OK", flush=True)
    finally:
        with psycopg.connect(DSN, autocommit=True) as conn:
            _restore_vault_watermarks(conn, snapshot)
        shutil.rmtree(scratch, ignore_errors=True)


if __name__ == "__main__":
    try:
        run()
    except AssertionError as e:
        print(f"SMOKE FAIL: {e}", flush=True)
        sys.exit(1)
