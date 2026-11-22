"""T-14 smoke: per-ym marts build on a T-08 partitioned vault fixture.

Covers acceptance criteria for T-14 (architecture.md section 5, PR3):

  1. syntax + import (re-asserted here)
  2. Builds data/gold/marts/dim_aircraft/part.parquet + dim_airport/part.parquet
     as monoliths (full-refresh, non-partitioned small dims)
  3. Builds data/gold/marts/fact_flight/ym=YYYY-MM/part.parquet and
     data/gold/marts/fact_trajectory_enriched/ym=YYYY-MM/part.parquet for
     every ym present in the partitioned hub_flight tree
  4. fact_trajectory_enriched.ym column populated via vault_io.ym_from_epoch
     and matches the partition name bit-for-bit (architecture Open Question 5)
  5. Second invocation is a no-op per pipeline_watermarks('marts',
     'fact_trajectory_enriched') -- mtimes on fact partitions unchanged
  6. pipeline_watermarks rows exist for ('marts', 'fact_flight') and
     ('marts', 'fact_trajectory_enriched') with last_ym = newest fixture ym

Fixture strategy:
  Mirrors t08_vault_smoke's approach -- builds a scratch 2-month flightlist
  bronze from head(5000) of real monthly Zenodo files, runs build_raw_vault
  (pandas, no states) to get the four partitioned flightlist tables and the
  three small-hub monoliths. Then synthesizes a tiny sat_state_observation
  partition per ym (10 rows each) joined to the real hub_aircraft hashes, so
  build_fact_trajectory_enriched_for_ym has non-empty input to exercise the
  ym_from_epoch assignment.

Watermark pollution guard:
  pipeline_watermarks is a global table; the test keys layer='marts',
  table_name in ('fact_flight','fact_trajectory_enriched') are the real
  production keys. The smoke snapshots every ('marts', *) row before the
  run and restores it in the finally: block. Vault watermark rows are
  snapshotted + restored identically because build_raw_vault writes
  layer='vault' watermarks too.

Prints "MARTS SMOKE OK" when all assertions pass. Windows cp1252-safe
(no arrows, no em-dashes).

Invoke:
    python multi-agent-engineering/projects/PROJ-20261118-impr01-partition-contract/artifacts/t14_marts_smoke.py
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

from etl4.vault import marts, vault_io, watermark
from etl4.vault.raw_vault import build_raw_vault
from etl4.vault.schema import hash_series

DSN = os.getenv("POSTGRES_DSN", "postgresql://etl4:change_me@localhost:5432/etl4")
BRONZE_SRC = REPO_ROOT / "data" / "bronze" / "flightlists" / "v23.00"
FIXTURE_MONTHS = (
    ("flightlist_20190101_20190131.parquet", "2019-01", 1546300800),  # 2019-01-01 UTC
    ("flightlist_20190201_20190228.parquet", "2019-02", 1548979200),  # 2019-02-01 UTC
)


def _snapshot_watermarks(conn, layer: str) -> list[tuple]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT layer, table_name, last_ym, row_count "
            "FROM pipeline_watermarks WHERE layer = %s",
            (layer,),
        )
        return cur.fetchall()


def _restore_watermarks(conn, layer: str, snapshot: list[tuple]) -> None:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM pipeline_watermarks WHERE layer = %s", (layer,))
        for row in snapshot:
            cur.execute(
                "INSERT INTO pipeline_watermarks "
                "(layer, table_name, last_ym, row_count) VALUES (%s,%s,%s,%s)",
                row,
            )


def _build_flightlist_fixture(scratch: Path) -> Path:
    bronze_dir = scratch / "bronze" / "flightlists" / "v23.00"
    bronze_dir.mkdir(parents=True)
    for src_name, ym, _epoch0 in FIXTURE_MONTHS:
        src = BRONZE_SRC / src_name
        if not src.exists():
            raise FileNotFoundError(
                f"fixture source missing: {src}. Run bronze_flightlists stage first."
            )
        df = pd.read_parquet(src).head(5000)
        day_ym = pd.to_datetime(df["day"], utc=True).dt.strftime("%Y-%m")
        assert set(day_ym.dropna().unique()).issubset({ym}), (
            f"{src_name}: day column carries ym outside {ym}"
        )
        df.to_parquet(bronze_dir / src_name, index=False)
    return bronze_dir


def _write_state_obs_fixture(vault_root: Path) -> dict:
    """Synthesize a 10-row sat_state_observation partition per ym, keyed on
    real hub_aircraft hashes so the fact_trajectory_enriched merge yields
    non-empty rows."""
    hub_a = vault_io.read_table("hub_aircraft", root=vault_root)
    assert not hub_a.empty, "hub_aircraft monolith is empty -- fixture precondition"
    sample_hks = hub_a["hk_aircraft"].head(10).reset_index(drop=True)
    out: dict[str, int] = {}
    for _src, ym, epoch0 in FIXTURE_MONTHS:
        n = len(sample_hks)
        frame = pd.DataFrame({
            "hk_aircraft":  sample_hks,
            "time":         pd.Series(range(epoch0, epoch0 + n * 60, 60), dtype="Int64"),
            "lat":          pd.Series([50.0 + i * 0.01 for i in range(n)], dtype="float64"),
            "lon":          pd.Series([8.0  + i * 0.01 for i in range(n)], dtype="float64"),
            "baroaltitude": pd.Series([10000.0 + i * 100 for i in range(n)], dtype="float64"),
            "velocity":     pd.Series([250.0 for _ in range(n)], dtype="float64"),
            "heading":      pd.Series([ 90.0 for _ in range(n)], dtype="float64"),
            "callsign":     pd.Series([f"TST{i:03d}" for i in range(n)], dtype="string"),
            "ym":           pd.Series([ym] * n, dtype="string"),
        })
        vault_io.write_partition(frame, "sat_state_observation", ym,
                                 out_dir=vault_root)
        out[ym] = n
    return out


def _mtimes(marts_dir: Path) -> dict:
    out: dict = {}
    for t in ("fact_flight", "fact_trajectory_enriched"):
        for ym_dir in sorted((marts_dir / t).glob("ym=*")):
            p = ym_dir / "part.parquet"
            if p.is_file():
                out[f"{t}/{ym_dir.name}"] = p.stat().st_mtime_ns
    for t in ("dim_aircraft", "dim_airport"):
        p = marts_dir / t / "part.parquet"
        if p.is_file():
            out[f"{t}/part.parquet"] = p.stat().st_mtime_ns
    return out


def _marts_watermark_rows(conn) -> list[tuple]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT table_name, last_ym, row_count FROM pipeline_watermarks "
            "WHERE layer = 'marts' ORDER BY table_name"
        )
        return cur.fetchall()


def run():
    scratch   = Path(tempfile.mkdtemp(prefix="t14_marts_smoke_"))
    vault_dir = scratch / "silver" / "vault"
    marts_dir = scratch / "gold"   / "marts"
    vault_dir.mkdir(parents=True)
    marts_dir.mkdir(parents=True)

    with psycopg.connect(DSN, autocommit=True) as conn:
        vault_snap  = _snapshot_watermarks(conn, "vault")
        marts_snap  = _snapshot_watermarks(conn, "marts")
        with conn.cursor() as cur:
            cur.execute("DELETE FROM pipeline_watermarks WHERE layer IN ('vault','marts')")

    try:
        bronze_dir = _build_flightlist_fixture(scratch)

        print("--- fixture: build partitioned vault via build_raw_vault ---",
              flush=True)
        build_raw_vault(
            flightlist_dir=bronze_dir,
            states_dir=scratch / "no_such_states",
            out_dir=vault_dir,
            include_states=False,
            engine="pandas",
        )
        for _src, ym, _ in FIXTURE_MONTHS:
            assert (vault_dir / "hub_flight" / f"ym={ym}" / "part.parquet").is_file(), \
                f"hub_flight/ym={ym}/part.parquet missing after fixture build"
        print("--- fixture: synthesize per-ym sat_state_observation ---",
              flush=True)
        state_counts = _write_state_obs_fixture(vault_dir)
        print(f"  sat_state_observation per-ym counts: {state_counts}", flush=True)

        # ---- RUN 1: bootstrap --------------------------------------------
        print("--- RUN 1 (bootstrap marts) ---", flush=True)
        counts1 = marts.build_marts(
            out_dir=marts_dir, vault_root=vault_dir, dsn=DSN,
        )
        print(f"  counts: {counts1}", flush=True)

        # Dims written as monoliths.
        for dim in ("dim_aircraft", "dim_airport"):
            dim_path = marts_dir / dim / "part.parquet"
            assert dim_path.is_file(), f"missing {dim}/part.parquet monolith"
            back = pq.read_table(dim_path).to_pandas()
            assert len(back) > 0, f"{dim} wrote 0 rows"

        # Facts written per-ym.
        for _src, ym, _ in FIXTURE_MONTHS:
            for fact in ("fact_flight", "fact_trajectory_enriched"):
                part = marts_dir / fact / f"ym={ym}" / "part.parquet"
                assert part.is_file(), f"missing {fact}/ym={ym}/part.parquet"

            # fact_trajectory_enriched: ym column populated AND matches partition name
            ft = pq.read_table(
                marts_dir / "fact_trajectory_enriched" / f"ym={ym}" / "part.parquet"
            ).to_pandas()
            assert "ym" in ft.columns, (
                f"fact_trajectory_enriched/ym={ym}: ym column missing from body"
            )
            assert set(ft["ym"].dropna().unique()) == {ym}, (
                f"fact_trajectory_enriched/ym={ym}: body carries other ym values "
                f"{sorted(set(ft['ym'].dropna().unique()))}"
            )
            assert len(ft) == state_counts[ym], (
                f"fact_trajectory_enriched/ym={ym}: row-count drift "
                f"{len(ft)} vs state fixture {state_counts[ym]}"
            )

        # Watermarks: both fact tables point at the newest fixture ym.
        with psycopg.connect(DSN, autocommit=True) as conn:
            wm_rows = _marts_watermark_rows(conn)
        wm_by_table = {r[0]: r[1] for r in wm_rows}
        expected_newest = FIXTURE_MONTHS[-1][1]
        for t in ("fact_flight", "fact_trajectory_enriched"):
            assert wm_by_table.get(t) == expected_newest, (
                f"watermark for {t}: expected last_ym={expected_newest}, "
                f"got {wm_by_table.get(t)!r}"
            )

        mt1 = _mtimes(marts_dir)

        # ---- RUN 2: resume gate ------------------------------------------
        print("--- RUN 2 (resume, fact partitions expected no-op) ---",
              flush=True)
        counts2 = marts.build_marts(
            out_dir=marts_dir, vault_root=vault_dir, dsn=DSN,
        )
        print(f"  counts (fact_* should be 0 on resume): {counts2}", flush=True)
        for t in ("fact_flight", "fact_trajectory_enriched"):
            assert counts2[t] == 0, (
                f"resume regression on {t}: expected 0 rows processed, got {counts2[t]}"
            )

        mt2 = _mtimes(marts_dir)
        partitioned_keys = [k for k in mt1 if any(k.startswith(t + "/") for t in
                                                  ("fact_flight", "fact_trajectory_enriched"))]
        for k in partitioned_keys:
            assert mt1[k] == mt2[k], (
                f"resume regression: {k} rewritten (mtime changed) on run 2"
            )

        # Watermark still points at newest fixture ym.
        with psycopg.connect(DSN, autocommit=True) as conn:
            wm_rows2 = _marts_watermark_rows(conn)
        wm_by_table2 = {r[0]: r[1] for r in wm_rows2}
        for t in ("fact_flight", "fact_trajectory_enriched"):
            assert wm_by_table2.get(t) == expected_newest

        # Round-trip: read_table(ym_filter=ym) returns only that ym's rows.
        ft_jan = vault_io.read_table(
            "fact_trajectory_enriched", ym_filter="2019-01", root=marts_dir,
        )
        assert len(ft_jan) > 0
        assert set(ft_jan["ym"].dropna().unique()) == {"2019-01"}

        print("--- pipeline_watermarks WHERE layer='marts' ---", flush=True)
        for row in wm_rows2:
            print(f"  layer=marts table_name={row[0]} last_ym={row[1]} "
                  f"row_count={row[2]}", flush=True)

        print("MARTS SMOKE OK", flush=True)
    finally:
        with psycopg.connect(DSN, autocommit=True) as conn:
            _restore_watermarks(conn, "marts", marts_snap)
            _restore_watermarks(conn, "vault", vault_snap)
        shutil.rmtree(scratch, ignore_errors=True)


if __name__ == "__main__":
    try:
        run()
    except AssertionError as e:
        print(f"SMOKE FAIL: {e}", flush=True)
        sys.exit(1)
