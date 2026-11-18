"""T-05 smoke: exercises etl4.vault.vault_io end-to-end.

Covers acceptance criteria 3-4 for T-05:
  * All 4 reader-fallback cases (empty / monolith-only / partitioned-only /
    mixed-partitioned-wins)
  * Round-trip write_partition then read_table
  * icao24 dtype is pandas 'string' on every read
  * row_count matches pyarrow.parquet.ParquetFile().metadata.num_rows exactly
  * list_ym returns sorted 'YYYY-MM' partitions only
  * ym derivation helpers (ym_from_filename, ym_from_date, ym_from_epoch,
    extract_ym_from_frame, extract_ym_from_path)
  * ym_filter forms: None / str / list / tuple range
  * columns= pushdown
  * write_partition rejects malformed ym; accepts SYN sentinel
  * write_partition is deterministic (byte-identical twice)

Prints "VAULT_IO SMOKE OK" when all assertions pass. Operates entirely inside
a tempdir so it never touches real silver/vault paths. Windows cp1252-safe
(no arrows, no em-dashes).

Invoke:
    python multi-agent-engineering/projects/PROJ-20261118-impr01-partition-contract/artifacts/t05_vault_io_smoke.py
"""
from __future__ import annotations

import hashlib
import shutil
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO_ROOT))

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from etl4.vault.vault_io import (
    read_table,
    write_partition,
    row_count,
    list_ym,
    ym_from_filename,
    ym_from_date,
    ym_from_epoch,
    extract_ym_from_frame,
    extract_ym_from_path,
    SILVER_VAULT_ROOT,
    MARTS_ROOT,
    PARTITIONED_TABLES,
    NON_PARTITIONED_TABLES,
)


def _mkdf(icao_hex: list, times: list, extra_cols: dict | None = None) -> pd.DataFrame:
    df = pd.DataFrame({
        "icao24": pd.array(icao_hex, dtype="string"),
        "time": pd.array(times, dtype="Int64"),
        "lat": [10.0 + i for i in range(len(icao_hex))],
        "lon": [20.0 + i for i in range(len(icao_hex))],
    })
    if extra_cols:
        for k, v in extra_cols.items():
            df[k] = v
    return df


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def run():
    tmp = Path(tempfile.mkdtemp(prefix="vault_io_smoke_"))
    try:
        # ------------------------------------------------------------------
        # CASE 1: empty layout -- table dir does not exist -> empty frame, 0
        # rows, [] list.
        # ------------------------------------------------------------------
        df_empty = read_table("nothing_here", root=tmp)
        assert isinstance(df_empty, pd.DataFrame) and df_empty.empty and len(df_empty.columns) == 0, \
            "empty layout should return bare empty DataFrame"
        assert row_count("nothing_here", root=tmp) == 0
        assert list_ym("nothing_here", root=tmp) == []

        # Empty table dir (exists but holds nothing) -> same result.
        (tmp / "empty_dir").mkdir(parents=True)
        assert read_table("empty_dir", root=tmp).empty
        assert row_count("empty_dir", root=tmp) == 0
        assert list_ym("empty_dir", root=tmp) == []

        # ------------------------------------------------------------------
        # CASE 2: monolithic layout only (non-partitioned table).
        # ------------------------------------------------------------------
        mono_dir = tmp / "mono_only"
        mono_dir.mkdir(parents=True)
        mono_df = _mkdf(["0a1b2c", "0d4e5f", "ab12cd"], [100, 200, 300])
        pq.write_table(pa.Table.from_pandas(mono_df, preserve_index=False),
                       mono_dir / "part.parquet")

        got = read_table("mono_only", root=tmp)
        assert len(got) == 3, f"monolith full read should yield 3 rows, got {len(got)}"
        assert str(got["icao24"].dtype) == "string", \
            f"icao24 dtype must be 'string', got {got['icao24'].dtype!r}"
        # 0a1b2c must NOT be parsed as int 10534700
        assert got["icao24"].iloc[0] == "0a1b2c"

        # ym_filter on a monolith is unsatisfiable -> empty DataFrame.
        got2 = read_table("mono_only", ym_filter="2022-07", root=tmp)
        assert got2.empty, "monolith + ym_filter should return empty"

        # row_count honours the same rule.
        assert row_count("mono_only", root=tmp) == 3
        assert row_count("mono_only", ym_filter="2022-07", root=tmp) == 0

        # list_ym on monolith-only -> [].
        assert list_ym("mono_only", root=tmp) == []

        # Column pushdown: only one column returned.
        got3 = read_table("mono_only", columns=["icao24"], root=tmp)
        assert list(got3.columns) == ["icao24"]
        assert str(got3["icao24"].dtype) == "string"

        # ------------------------------------------------------------------
        # CASE 3: partitioned layout only -- 4 ym partitions.
        # ------------------------------------------------------------------
        # 2022-07 epoch = 1656633600, 2022-08 = 1659312000, etc.
        epochs_by_ym = {
            "2022-03": 1646136000,
            "2022-04": 1648814400,
            "2022-05": 1651406400,
            "2022-06": 1654084800,
            "2022-07": 1656763200,
        }
        for ym, epoch in epochs_by_ym.items():
            df = _mkdf([f"icao{ym[-2:]}1", f"icao{ym[-2:]}2"],
                       [epoch, epoch + 60],
                       extra_cols={"ym": [ym, ym]})
            write_partition(df, "part_only", ym, out_dir=tmp)

        # Full read -- all 5 months, 2 rows each.
        got = read_table("part_only", root=tmp)
        assert len(got) == 10, f"partitioned full read should be 10 rows, got {len(got)}"
        assert str(got["icao24"].dtype) == "string"

        # Single-month ym_filter.
        got_jul = read_table("part_only", ym_filter="2022-07", root=tmp)
        assert len(got_jul) == 2
        assert set(got_jul["ym"]) == {"2022-07"}

        # Missing ym_filter -> empty.
        got_miss = read_table("part_only", ym_filter="1999-01", root=tmp)
        assert got_miss.empty

        # list filter.
        got_sub = read_table("part_only", ym_filter=["2022-03", "2022-05"], root=tmp)
        assert len(got_sub) == 4
        assert set(got_sub["ym"]) == {"2022-03", "2022-05"}

        # Tuple range (inclusive).
        got_rng = read_table("part_only", ym_filter=("2022-04", "2022-06"), root=tmp)
        assert len(got_rng) == 6
        assert set(got_rng["ym"]) == {"2022-04", "2022-05", "2022-06"}

        # row_count parity.
        assert row_count("part_only", root=tmp) == 10
        assert row_count("part_only", ym_filter="2022-07", root=tmp) == 2
        assert row_count("part_only", ym_filter=("2022-04", "2022-06"), root=tmp) == 6

        # list_ym sorted ascending.
        assert list_ym("part_only", root=tmp) == [
            "2022-03", "2022-04", "2022-05", "2022-06", "2022-07",
        ]

        # ------------------------------------------------------------------
        # CASE 4: mixed layout -- partitioned WINS; stale monolith ignored.
        # ------------------------------------------------------------------
        mix_dir = tmp / "mixed"
        mix_dir.mkdir(parents=True)
        # Stale monolith first (would carry 500 rows if read).
        stale = _mkdf([f"stale{i:03x}" for i in range(500)], list(range(500)))
        pq.write_table(pa.Table.from_pandas(stale, preserve_index=False),
                       mix_dir / "part.parquet")
        # Partitions on top -- only 3 rows.
        for ym in ("2022-07", "2022-08"):
            write_partition(_mkdf(["aa0001"], [1_000_000],
                                  extra_cols={"ym": [ym]}),
                            "mixed", ym, out_dir=tmp)

        got = read_table("mixed", root=tmp)
        assert len(got) == 2, f"mixed layout: partitioned wins; expected 2 rows, got {len(got)}"
        assert "icao24" in got.columns and got["icao24"].iloc[0] == "aa0001"
        # row_count mirrors read_table.
        assert row_count("mixed", root=tmp) == 2
        assert list_ym("mixed", root=tmp) == ["2022-07", "2022-08"]

        # ------------------------------------------------------------------
        # CASE 5: round-trip write_partition -> read_table preserves icao24,
        # retains ym in file body, is deterministic (same bytes twice).
        # ------------------------------------------------------------------
        rt_df = _mkdf(["0a1b2c", "ab12cd"], [1656763200, 1656763260],
                      extra_cols={"ym": ["2022-07", "2022-07"]})
        p1 = write_partition(rt_df, "roundtrip", "2022-07", out_dir=tmp)
        h1 = _sha256(p1)
        p2 = write_partition(rt_df, "roundtrip", "2022-07", out_dir=tmp)
        h2 = _sha256(p2)
        assert h1 == h2, "write_partition must be byte-deterministic for identical input"

        # ym retained as a column in the file body (not consumed by partition_cols).
        back = read_table("roundtrip", root=tmp)
        assert "ym" in back.columns, "ym must remain in file body (write_to_dataset pitfall)"
        assert set(back["ym"]) == {"2022-07"}
        assert str(back["icao24"].dtype) == "string"
        # Hex-looking 0a1b2c survives as a string, not int 10534700.
        assert "0a1b2c" in set(back["icao24"])

        # ------------------------------------------------------------------
        # CASE 6: write_partition ym validation.
        # ------------------------------------------------------------------
        for bad in ("2022-13", "2022-7", "22-07", "foo", "SYN-", "2022-00"):
            raised = False
            try:
                write_partition(rt_df, "badym", bad, out_dir=tmp)
            except ValueError:
                raised = True
            assert raised, f"write_partition should have rejected ym={bad!r}"

        # SYN sentinel is accepted.
        p_syn = write_partition(rt_df, "synoat", "SYN-002A", out_dir=tmp)
        assert p_syn.exists()
        assert list_ym("synoat", root=tmp) == ["SYN-002A"]

        # ------------------------------------------------------------------
        # CASE 7: no half-written artefact survives. Verify after a write
        # the dir holds exactly part.parquet (no stray .tmp).
        # ------------------------------------------------------------------
        for p in (tmp / "roundtrip" / "ym=2022-07").iterdir():
            assert not p.name.endswith(".tmp"), f"stray tmp artefact: {p}"
        assert (tmp / "roundtrip" / "ym=2022-07" / "part.parquet").is_file()

        # ------------------------------------------------------------------
        # CASE 8: Spark-shard reader extension. Multiple *.parquet in one
        # ym dir should all be read.
        # ------------------------------------------------------------------
        spark_dir = tmp / "sparkshards" / "ym=2022-07"
        spark_dir.mkdir(parents=True)
        shard_a = _mkdf(["aaaaaa"], [1])
        shard_b = _mkdf(["bbbbbb", "cccccc"], [2, 3])
        pq.write_table(pa.Table.from_pandas(shard_a, preserve_index=False),
                       spark_dir / "part-00000-abc.parquet")
        pq.write_table(pa.Table.from_pandas(shard_b, preserve_index=False),
                       spark_dir / "part-00001-def.parquet")
        got_spark = read_table("sparkshards", root=tmp)
        assert len(got_spark) == 3, \
            f"Spark-shard reader should sum 1+2=3 rows, got {len(got_spark)}"
        assert str(got_spark["icao24"].dtype) == "string"
        assert row_count("sparkshards", root=tmp) == 3

        # ------------------------------------------------------------------
        # CASE 9: ym derivation helpers.
        # ------------------------------------------------------------------
        # ym_from_filename: version-agnostic, basename-only.
        assert ym_from_filename("flightlist_20220715_20220722.parquet") == "2022-07"
        assert ym_from_filename(
            r"C:\data\bronze\flightlists\v23.00\flightlist_20220815_20220822.parquet"
        ) == "2022-08"
        assert ym_from_filename("notaflightlist.parquet") is None

        # ym_from_date: vectorised, utc, nullable.
        s_date = ym_from_date(pd.Series(["2022-07-15", "2022-08-01"]))
        assert list(s_date) == ["2022-07", "2022-08"]
        assert str(s_date.dtype) == "string"

        # ym_from_epoch: UTC, bit-identical to Postgres to_char(to_timestamp,...).
        s_epoch = ym_from_epoch(pd.Series([1656763200, 1659441600], dtype="Int64"))
        assert list(s_epoch) == ["2022-07", "2022-08"]
        assert str(s_epoch.dtype) == "string"

        # extract_ym_from_frame dispatch.
        fl_df = pd.DataFrame({"day": pd.to_datetime(["2022-07-15"])})
        assert list(extract_ym_from_frame(fl_df, "flightlist")) == ["2022-07"]
        sfs_df = pd.DataFrame({"firstseen": pd.array([1656763200], dtype="Int64")})
        assert list(extract_ym_from_frame(sfs_df, "sat_flight_summary")) == ["2022-07"]
        sso_df = pd.DataFrame({"time": pd.array([1656763200], dtype="Int64")})
        assert list(extract_ym_from_frame(sso_df, "sat_state_observation")) == ["2022-07"]
        fte_df = pd.DataFrame({"time": pd.array([1656763200], dtype="Int64")})
        assert list(extract_ym_from_frame(fte_df, "fact_trajectory_enriched")) == ["2022-07"]
        raised = False
        try:
            extract_ym_from_frame(fl_df, "mystery_source")
        except ValueError:
            raised = True
        assert raised, "unknown source should have raised ValueError"

        # extract_ym_from_path precedence: ym= > date= > flightlist filename.
        assert extract_ym_from_path("data/silver/vault/hub_flight/ym=2022-07/part.parquet") == "2022-07"
        assert extract_ym_from_path("data/bronze/raw_states/date=2022-08-15/hour=00/part.parquet") == "2022-08"
        assert extract_ym_from_path("data/bronze/flightlists/v23.00/flightlist_20220915_20220922.parquet") == "2022-09"
        # ym= wins over date= when both are in the path.
        assert extract_ym_from_path("data/silver/vault/whatever/ym=2022-07/from_date=2022-08-15/part.parquet") == "2022-07"
        # SYN sentinel.
        assert extract_ym_from_path("tmp/ym=SYN-002A/part.parquet") == "SYN-002A"
        raised = False
        try:
            extract_ym_from_path("tmp/nomarker/part.parquet")
        except ValueError:
            raised = True
        assert raised, "path with no recognisable marker should raise ValueError"

        # ------------------------------------------------------------------
        # CASE 10: module-level constants sanity.
        # ------------------------------------------------------------------
        assert isinstance(SILVER_VAULT_ROOT, Path)
        assert isinstance(MARTS_ROOT, Path)
        assert "hub_flight" in PARTITIONED_TABLES
        assert "hub_aircraft" in NON_PARTITIONED_TABLES
        assert PARTITIONED_TABLES.isdisjoint(NON_PARTITIONED_TABLES)

        # ------------------------------------------------------------------
        # CASE 11: row_count O(files) matches direct pyarrow footer sum on
        # the partitioned fixture.
        # ------------------------------------------------------------------
        direct = 0
        for f in sorted((tmp / "part_only").rglob("*.parquet")):
            direct += pq.ParquetFile(f).metadata.num_rows
        assert direct == row_count("part_only", root=tmp) == 10

        print("VAULT_IO SMOKE OK", flush=True)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    try:
        run()
    except AssertionError as e:
        print(f"SMOKE FAIL: {e}", flush=True)
        sys.exit(1)
