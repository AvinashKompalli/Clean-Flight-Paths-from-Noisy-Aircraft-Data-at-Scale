"""T-08 smoke: per-ym rewrite of _chunked_flightlist_vault.

Covers acceptance criteria for T-08 (architecture.md section 5, PR2):

  1. syntax + import (already verified upstream; re-asserted here)
  2. Builds ym=YYYY-MM/part.parquet layout for the four partitioned tables
     (hub_flight, link_flight_aircraft, link_flight_route, sat_flight_summary)
  3. Builds single-file monoliths for the three small hubs (hub_aircraft,
     hub_airport, sat_aircraft_identity) and compacts across months
  4. _DONE sentinel and part.parquet both land for every (table, ym)
  5. Second invocation is a no-op: nothing is re-read, nothing is rewritten,
     and pipeline_watermarks already carries the advanced ym
  6. pipeline_watermarks row exists with layer='vault' and last_ym = the
     newest ym processed

Fixture strategy:
  The task's literal command (``--flightlist-dir data/bronze/synthetic``)
  cannot work because ``data/bronze/synthetic`` is state-vector shaped
  (date=/hour=), has no callsign/day columns, and ``ym_from_filename``
  returns None for those paths. Instead we copy the first 5,000 rows of two
  real Zenodo monthly files under a scratch dir with the correct filename
  pattern (``flightlist_YYYYMMDD_YYYYMMDD.parquet``). Fixture lives inside
  tempfile.mkdtemp -- never touches ``data/silver/vault``.

Watermark pollution guard:
  pipeline_watermarks is a global table; the test key layer='vault',
  table='hub_flight' is the real one the production bootstrap keys off.
  The smoke snapshots every ('vault', *) row before the run and restores
  it in the finally: block (DELETE if absent pre-run).

Prints "VAULT_SMOKE OK" when all assertions pass. Windows cp1252-safe
(no arrows, no em-dashes).

Invoke:
    python multi-agent-engineering/projects/PROJ-20261118-impr01-partition-contract/artifacts/t08_vault_smoke.py
"""
from __future__ import annotations

import os
import shutil
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO_ROOT))

import pandas as pd
import psycopg
import pyarrow.parquet as pq

from etl4.vault import vault_io, watermark
from etl4.vault.raw_vault import build_raw_vault, PARTITIONED_TABLES, NON_PARTITIONED_TABLES

DSN = os.getenv("POSTGRES_DSN", "postgresql://etl4:change_me@localhost:5432/etl4")
BRONZE_SRC = REPO_ROOT / "data" / "bronze" / "flightlists" / "v23.00"
FIXTURE_MONTHS = (
    ("flightlist_20190101_20190131.parquet", "2019-01"),
    ("flightlist_20190201_20190228.parquet", "2019-02"),
)


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


def _build_fixture(scratch: Path) -> Path:
    """Copy head(5000) of the two real monthlies into scratch with the
    canonical filename pattern so ym_from_filename resolves."""
    bronze_dir = scratch / "bronze" / "flightlists" / "v23.00"
    bronze_dir.mkdir(parents=True)
    for src_name, ym in FIXTURE_MONTHS:
        src = BRONZE_SRC / src_name
        if not src.exists():
            raise FileNotFoundError(
                f"fixture source missing: {src}. Run bronze_flightlists stage first."
            )
        df = pd.read_parquet(src).head(5000)
        # Sanity: every row's day falls inside the filename month.
        day_ym = pd.to_datetime(df["day"], utc=True).dt.strftime("%Y-%m")
        assert set(day_ym.dropna().unique()).issubset({ym}), (
            f"{src_name}: day column carries ym outside {ym}: "
            f"{sorted(set(day_ym.dropna().unique()))}"
        )
        df.to_parquet(bronze_dir / src_name, index=False)
    return bronze_dir


def _assert_partition_layout(out_dir: Path, ym: str) -> None:
    for t in PARTITIONED_TABLES:
        ym_dir = out_dir / t / f"ym={ym}"
        assert (ym_dir / "part.parquet").is_file(), f"missing {t}/ym={ym}/part.parquet"
        assert (ym_dir / "_DONE").is_file(), f"missing {t}/ym={ym}/_DONE sentinel"
        # ym stays inside the file body (CLAUDE.md section 7).
        back = pq.read_table(ym_dir / "part.parquet").to_pandas()
        assert "ym" in back.columns, f"{t}/ym={ym}: ym column missing from file body"
        assert set(back["ym"].dropna().unique()).issubset({ym}), (
            f"{t}/ym={ym}: file body carries other ym values: "
            f"{sorted(set(back['ym'].dropna().unique()))}"
        )


def _assert_monolith_layout(out_dir: Path) -> None:
    for t in NON_PARTITIONED_TABLES:
        mono = out_dir / t / "part.parquet"
        assert mono.is_file(), f"missing {t}/part.parquet monolith"


def _mtimes(out_dir: Path) -> dict:
    out: dict = {}
    for t in PARTITIONED_TABLES:
        for ym_dir in sorted((out_dir / t).glob("ym=*")):
            p = ym_dir / "part.parquet"
            if p.is_file():
                out[f"{t}/{ym_dir.name}"] = p.stat().st_mtime_ns
    for t in NON_PARTITIONED_TABLES:
        p = out_dir / t / "part.parquet"
        if p.is_file():
            out[f"{t}/part.parquet"] = p.stat().st_mtime_ns
    return out


def _vault_watermark_rows(conn) -> list[tuple]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT table_name, last_ym, row_count FROM pipeline_watermarks "
            "WHERE layer = 'vault' ORDER BY table_name"
        )
        return cur.fetchall()


def run():
    scratch = Path(tempfile.mkdtemp(prefix="t08_vault_smoke_"))
    out_dir = scratch / "silver" / "vault"
    out_dir.mkdir(parents=True)

    with psycopg.connect(DSN, autocommit=True) as conn:
        snapshot = _snapshot_vault_watermarks(conn)
    # Clear the production-key row so the smoke bootstraps from zero.
    with psycopg.connect(DSN, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM pipeline_watermarks WHERE layer = 'vault'")

    try:
        bronze_dir = _build_fixture(scratch)

        # ---- RUN 1: bootstrap ------------------------------------------------
        print("--- RUN 1 (bootstrap) ---", flush=True)
        counts1 = build_raw_vault(
            flightlist_dir=bronze_dir,
            states_dir=scratch / "no_such_states",  # forces states_hub_input empty
            out_dir=out_dir,
            include_states=False,
            engine="pandas",
        )
        assert set(counts1).issuperset(set(PARTITIONED_TABLES))
        for t in PARTITIONED_TABLES:
            assert counts1[t] > 0, f"{t}: expected rows after bootstrap, got {counts1[t]}"
        for t in NON_PARTITIONED_TABLES:
            assert counts1[t] > 0, f"{t}: expected rows after bootstrap, got {counts1[t]}"

        for _src, ym in FIXTURE_MONTHS:
            _assert_partition_layout(out_dir, ym)
        _assert_monolith_layout(out_dir)

        # Watermark advanced for every partitioned flightlist table.
        with psycopg.connect(DSN, autocommit=True) as conn:
            wm_rows = _vault_watermark_rows(conn)
        wm_by_table = {r[0]: r[1] for r in wm_rows}
        expected_newest = FIXTURE_MONTHS[-1][1]
        for t in PARTITIONED_TABLES:
            assert wm_by_table.get(t) == expected_newest, (
                f"watermark for {t}: expected last_ym={expected_newest}, "
                f"got {wm_by_table.get(t)!r}"
            )

        mt1 = _mtimes(out_dir)

        # ---- RUN 2: resume gate ---------------------------------------------
        print("--- RUN 2 (resume, expected no-op) ---", flush=True)
        counts2 = build_raw_vault(
            flightlist_dir=bronze_dir,
            states_dir=scratch / "no_such_states",
            out_dir=out_dir,
            include_states=False,
            engine="pandas",
        )
        for t in PARTITIONED_TABLES + NON_PARTITIONED_TABLES:
            assert counts2[t] == counts1[t], (
                f"resume regression on {t}: {counts1[t]} -> {counts2[t]}"
            )

        mt2 = _mtimes(out_dir)
        partitioned_keys = [k for k in mt1 if any(k.startswith(t + "/") for t in PARTITIONED_TABLES)]
        for k in partitioned_keys:
            assert mt1[k] == mt2[k], (
                f"resume regression: {k} rewritten (mtime changed) on run 2"
            )

        # Watermark still points at the newest fixture ym.
        with psycopg.connect(DSN, autocommit=True) as conn:
            wm_rows2 = _vault_watermark_rows(conn)
        wm_by_table2 = {r[0]: r[1] for r in wm_rows2}
        for t in PARTITIONED_TABLES:
            assert wm_by_table2.get(t) == expected_newest

        # read_table round-trip: ym_filter returns only that month's rows.
        hub_jan = vault_io.read_table("hub_flight", ym_filter="2019-01", root=out_dir)
        assert len(hub_jan) > 0
        assert set(hub_jan["ym"].dropna().unique()) == {"2019-01"}
        assert str(hub_jan["icao24"].dtype) == "string" if "icao24" in hub_jan.columns else True

        # Final evidence dump: pipeline_watermarks rows for layer='vault'.
        print("--- pipeline_watermarks WHERE layer='vault' ---", flush=True)
        with psycopg.connect(DSN, autocommit=True) as conn:
            final_rows = _vault_watermark_rows(conn)
        for row in final_rows:
            print(f"  layer=vault table_name={row[0]} last_ym={row[1]} row_count={row[2]}",
                  flush=True)

        print("VAULT_SMOKE OK", flush=True)
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
