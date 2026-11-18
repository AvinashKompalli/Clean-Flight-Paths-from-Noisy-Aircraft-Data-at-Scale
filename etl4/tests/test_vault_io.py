"""Unit tests for etl4/vault/vault_io.py (T-05/T-06, IMPR-01).

Anchors the invariants listed in vault_io_spec.md:
  * Round-trip write-then-read preserves ``icao24`` as pandas ``string`` dtype.
  * Reader fallback - all four layout cases (empty / monolith-only /
    partitioned-only / mixed-partitioned-wins).
  * ``ym_filter`` forms (None / str / list / tuple range).
  * Column pushdown via pyarrow.
  * ``row_count`` equals the direct ``ParquetFile().metadata.num_rows`` sum.
  * ``list_ym`` sorted; empty on monolith-only.
  * Concurrent write to the same ym: exactly one survivor.
  * ym derivation helpers (filename / date / epoch / frame / path).
  * ``write_partition`` rejects malformed ym.
  * Spark-shard glob inside an ``ym=`` dir.

All filesystem state lives under ``tmp_path``; no production code is touched.
Windows cp1252-safe stdout (no arrows, no em-dashes).
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from etl4.vault.vault_io import (
    MARTS_ROOT,
    NON_PARTITIONED_TABLES,
    PARTITIONED_TABLES,
    SILVER_VAULT_ROOT,
    extract_ym_from_frame,
    extract_ym_from_path,
    list_ym,
    read_table,
    row_count,
    write_partition,
    ym_from_date,
    ym_from_epoch,
    ym_from_filename,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mkdf(icao_hex: list, times: list, extra: dict | None = None) -> pd.DataFrame:
    df = pd.DataFrame(
        {
            "icao24": pd.array(icao_hex, dtype="string"),
            "time": pd.array(times, dtype="Int64"),
            "lat": [10.0 + i for i in range(len(icao_hex))],
            "lon": [20.0 + i for i in range(len(icao_hex))],
        }
    )
    if extra:
        for k, v in extra.items():
            df[k] = v
    return df


def _write_monolith(table_dir: Path, df: pd.DataFrame) -> Path:
    table_dir.mkdir(parents=True, exist_ok=True)
    out = table_dir / "part.parquet"
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), out)
    return out


# ---------------------------------------------------------------------------
# 1-2. Round-trip read/write
# ---------------------------------------------------------------------------


def test_write_then_read_preserves_icao24_string_dtype(tmp_path: Path) -> None:
    df = _mkdf(["0a1b2c", "ab12cd", "deadbe"], [1, 2, 3])
    assert str(df["icao24"].dtype) == "string"
    write_partition(df, "roundtrip", "2022-07", out_dir=tmp_path)

    got = read_table("roundtrip", root=tmp_path)
    assert str(got["icao24"].dtype) == "string"
    # Hex-looking value must not be silently coerced to int 10534700.
    assert "0a1b2c" in set(got["icao24"])


def test_write_then_read_round_trip_equality(tmp_path: Path) -> None:
    df = _mkdf(["0a1b2c", "ab12cd"], [100, 200],
               extra={"ym": ["2022-07", "2022-07"]})
    write_partition(df, "eq", "2022-07", out_dir=tmp_path)

    back = read_table("eq", root=tmp_path)
    # Column order may differ, but content must survive the round-trip.
    for col in df.columns:
        assert col in back.columns, f"missing column {col} after round-trip"
    df_sorted = df[sorted(df.columns)].reset_index(drop=True)
    back_sorted = back[sorted(back.columns)].reset_index(drop=True)
    # icao24 arrives back as 'string' dtype (reader guarantees this).
    df_sorted["icao24"] = df_sorted["icao24"].astype("string")
    back_sorted["icao24"] = back_sorted["icao24"].astype("string")
    pd.testing.assert_frame_equal(
        df_sorted, back_sorted, check_dtype=False
    )


# ---------------------------------------------------------------------------
# 3-6. Reader fallback (the 4 layout cases)
# ---------------------------------------------------------------------------


def test_read_monolith_fallback_when_no_ym_dirs(tmp_path: Path) -> None:
    df = _mkdf(["0a1b2c", "0d4e5f", "ab12cd"], [100, 200, 300])
    _write_monolith(tmp_path / "mono", df)

    got = read_table("mono", root=tmp_path)
    assert len(got) == 3
    assert str(got["icao24"].dtype) == "string"


def test_read_partitioned_only(tmp_path: Path) -> None:
    for ym in ("2022-07", "2022-08", "2022-09"):
        write_partition(
            _mkdf([f"icao{ym[-2:]}1", f"icao{ym[-2:]}2"], [1, 2],
                  extra={"ym": [ym, ym]}),
            "part_only", ym, out_dir=tmp_path,
        )
    got = read_table("part_only", root=tmp_path)
    assert len(got) == 6
    assert set(got["ym"]) == {"2022-07", "2022-08", "2022-09"}
    assert str(got["icao24"].dtype) == "string"


def test_read_mixed_partitioned_wins_over_monolith(tmp_path: Path) -> None:
    mix_dir = tmp_path / "mixed"
    mix_dir.mkdir(parents=True)
    # Stale monolith - 500 rows that must NOT be read.
    stale = _mkdf([f"stale{i:03x}" for i in range(500)], list(range(500)))
    pq.write_table(pa.Table.from_pandas(stale, preserve_index=False),
                   mix_dir / "part.parquet")
    # Partitions - 2 rows total.
    for ym in ("2022-07", "2022-08"):
        write_partition(_mkdf(["aa0001"], [1_000_000],
                              extra={"ym": [ym]}),
                        "mixed", ym, out_dir=tmp_path)

    got = read_table("mixed", root=tmp_path)
    assert len(got) == 2
    assert "stale000" not in set(got["icao24"])
    assert got["icao24"].iloc[0] == "aa0001"


def test_read_empty_table_returns_empty_dataframe(tmp_path: Path) -> None:
    # Table dir doesn't exist.
    got = read_table("missing", root=tmp_path)
    assert isinstance(got, pd.DataFrame)
    assert got.empty
    assert len(got.columns) == 0

    # Table dir exists but is empty.
    (tmp_path / "empty_dir").mkdir()
    got2 = read_table("empty_dir", root=tmp_path)
    assert got2.empty
    assert len(got2.columns) == 0


# ---------------------------------------------------------------------------
# 7-10. ym filter forms
# ---------------------------------------------------------------------------


def _seed_partitioned_table(tmp_path: Path, name: str = "part") -> None:
    for ym in ("2022-06", "2022-07", "2022-08", "2022-09", "2022-10"):
        write_partition(
            _mkdf([f"a{ym[-2:]}1", f"a{ym[-2:]}2"], [1, 2],
                  extra={"ym": [ym, ym]}),
            name, ym, out_dir=tmp_path,
        )


def test_read_single_ym_filter(tmp_path: Path) -> None:
    _seed_partitioned_table(tmp_path)
    got = read_table("part", ym_filter="2022-07", root=tmp_path)
    assert len(got) == 2
    assert set(got["ym"]) == {"2022-07"}


def test_read_list_ym_filter(tmp_path: Path) -> None:
    _seed_partitioned_table(tmp_path)
    got = read_table("part", ym_filter=["2022-07", "2022-09"], root=tmp_path)
    assert len(got) == 4
    assert set(got["ym"]) == {"2022-07", "2022-09"}


def test_read_tuple_range_ym_filter(tmp_path: Path) -> None:
    _seed_partitioned_table(tmp_path)
    got = read_table("part", ym_filter=("2022-07", "2022-09"), root=tmp_path)
    assert len(got) == 6
    assert set(got["ym"]) == {"2022-07", "2022-08", "2022-09"}


def test_read_nonexistent_ym_returns_empty(tmp_path: Path) -> None:
    _seed_partitioned_table(tmp_path)
    got = read_table("part", ym_filter="2099-01", root=tmp_path)
    assert isinstance(got, pd.DataFrame)
    assert got.empty


# ---------------------------------------------------------------------------
# 11. Column pushdown
# ---------------------------------------------------------------------------


def test_read_columns_subset(tmp_path: Path) -> None:
    write_partition(
        _mkdf(["0a1b2c", "ab12cd"], [100, 200],
              extra={"ym": ["2022-07", "2022-07"]}),
        "cols", "2022-07", out_dir=tmp_path,
    )
    got = read_table("cols", columns=["icao24", "time"], root=tmp_path)
    assert list(got.columns) == ["icao24", "time"]
    assert str(got["icao24"].dtype) == "string"
    assert "lat" not in got.columns
    assert "lon" not in got.columns


# ---------------------------------------------------------------------------
# 12-13. row_count
# ---------------------------------------------------------------------------


def test_row_count_matches_pyarrow_footer(tmp_path: Path) -> None:
    _seed_partitioned_table(tmp_path)
    # Hand-count via direct pyarrow footer reads.
    direct = 0
    for f in sorted((tmp_path / "part").rglob("*.parquet")):
        direct += pq.ParquetFile(f).metadata.num_rows
    assert direct == 10
    assert row_count("part", root=tmp_path) == direct


def test_row_count_with_ym_filter(tmp_path: Path) -> None:
    _seed_partitioned_table(tmp_path)
    # Direct count for the matched partition only.
    direct = sum(
        pq.ParquetFile(f).metadata.num_rows
        for f in sorted((tmp_path / "part" / "ym=2022-07").glob("*.parquet"))
    )
    assert direct == 2
    assert row_count("part", ym_filter="2022-07", root=tmp_path) == direct
    assert row_count("part", ym_filter=("2022-07", "2022-09"), root=tmp_path) == 6


# ---------------------------------------------------------------------------
# 14. list_ym
# ---------------------------------------------------------------------------


def test_list_ym_sorted_and_empty_when_unpartitioned(tmp_path: Path) -> None:
    # Write partitions out of order.
    for ym in ("2022-09", "2022-07", "2022-08"):
        write_partition(_mkdf(["aa0001"], [1], extra={"ym": [ym]}),
                        "sorted_part", ym, out_dir=tmp_path)
    assert list_ym("sorted_part", root=tmp_path) == [
        "2022-07", "2022-08", "2022-09",
    ]

    # Monolith only - list_ym returns [].
    _write_monolith(tmp_path / "mono", _mkdf(["aa0001"], [1]))
    assert list_ym("mono", root=tmp_path) == []

    # Missing table - list_ym returns [].
    assert list_ym("ghost", root=tmp_path) == []


# ---------------------------------------------------------------------------
# 15. Concurrent write survivor
# ---------------------------------------------------------------------------


def test_concurrent_write_to_same_ym_single_survivor(tmp_path: Path) -> None:
    """Two writers race on the same ym; last writer wins atomically.

    After both complete, exactly ONE ``part.parquet`` exists in the target
    directory and NO ``.tmp`` artefacts linger. ``os.replace`` semantics are
    validated at the filesystem level, not mocked.
    """
    df_a = _mkdf(["aaaaaa"], [1], extra={"writer": ["A"]})
    df_b = _mkdf(["bbbbbb"], [2], extra={"writer": ["B"]})

    def _write(df: pd.DataFrame) -> Path:
        return write_partition(df, "race", "2022-07", out_dir=tmp_path)

    with ThreadPoolExecutor(max_workers=2) as ex:
        futures = [ex.submit(_write, df_a), ex.submit(_write, df_b)]
        results = [f.result() for f in futures]

    target_dir = tmp_path / "race" / "ym=2022-07"
    parquet_files = sorted(target_dir.glob("*.parquet"))
    assert len(parquet_files) == 1, (
        f"expected exactly one survivor, found {parquet_files}"
    )
    assert parquet_files[0].name == "part.parquet"
    # No stray temporary files left behind.
    tmps = list(target_dir.glob("*.tmp"))
    assert tmps == [], f"stray tmp artefacts: {tmps}"
    # Both writers returned the same target path.
    assert all(r == target_dir / "part.parquet" for r in results)

    # Surviving file is readable and contains one of the two writers' rows.
    got = read_table("race", root=tmp_path)
    assert len(got) == 1
    assert set(got["writer"]) in ({"A"}, {"B"})


# ---------------------------------------------------------------------------
# 16-18. ym helpers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "name, expected",
    [
        ("flightlist_20220715_20220722.parquet", "2022-07"),
        ("flightlist_20220101_20220108.parquet", "2022-01"),
        ("flightlist_20221231_20230107.parquet", "2022-12"),
        (r"C:\data\bronze\flightlists\v23.00\flightlist_20220815_20220822.parquet",
         "2022-08"),
        ("data/bronze/flightlists/v24.00/flightlist_20220915_20220922.parquet",
         "2022-09"),
        ("notaflightlist.parquet", None),
        ("flightlist_bad.parquet", None),
    ],
)
def test_ym_from_filename(name: str, expected: str | None) -> None:
    assert ym_from_filename(name) == expected


def test_ym_from_date_and_epoch() -> None:
    # ym_from_date on string + pandas Timestamp inputs.
    s_date = ym_from_date(pd.Series(["2022-07-15", "2022-08-01", "2023-01-31"]))
    assert list(s_date) == ["2022-07", "2022-08", "2023-01"]
    assert str(s_date.dtype) == "string"

    ts = ym_from_date(pd.Series(pd.to_datetime(["2022-11-02", "2024-02-29"])))
    assert list(ts) == ["2022-11", "2024-02"]

    # ym_from_epoch - UTC boundary. 1656763200 == 2022-07-02 12:00:00 UTC.
    s_epoch = ym_from_epoch(pd.Series([1656763200, 1659441600], dtype="Int64"))
    assert list(s_epoch) == ["2022-07", "2022-08"]
    assert str(s_epoch.dtype) == "string"


@pytest.mark.parametrize(
    "path, expected",
    [
        ("data/silver/vault/hub_flight/ym=2022-07/part.parquet", "2022-07"),
        ("data/bronze/raw_states/date=2022-08-15/hour=03/part.parquet", "2022-08"),
        ("data/bronze/flightlists/v23.00/flightlist_20220915_20220922.parquet",
         "2022-09"),
        # ym= wins when both ym= and date= are present.
        ("data/silver/vault/thing/ym=2022-07/from_date=2022-08-15/part.parquet",
         "2022-07"),
        # SYN sentinel.
        ("tmp/ym=SYN-002A/part.parquet", "SYN-002A"),
        # Windows paths with backslashes.
        (r"B:\data\bronze\raw_states\date=2022-07-15\hour=00\part.parquet",
         "2022-07"),
    ],
)
def test_extract_ym_from_path(path: str, expected: str) -> None:
    assert extract_ym_from_path(path) == expected


def test_extract_ym_from_path_raises_when_no_marker() -> None:
    with pytest.raises(ValueError):
        extract_ym_from_path("tmp/no_marker_here/part.parquet")


def test_extract_ym_from_frame_dispatch() -> None:
    fl = pd.DataFrame({"day": pd.to_datetime(["2022-07-15"])})
    assert list(extract_ym_from_frame(fl, "flightlist")) == ["2022-07"]

    sfs = pd.DataFrame({"firstseen": pd.array([1656763200], dtype="Int64")})
    assert list(extract_ym_from_frame(sfs, "sat_flight_summary")) == ["2022-07"]

    sso = pd.DataFrame({"time": pd.array([1656763200], dtype="Int64")})
    assert list(extract_ym_from_frame(sso, "sat_state_observation")) == ["2022-07"]

    fte = pd.DataFrame({"time": pd.array([1656763200], dtype="Int64")})
    assert list(extract_ym_from_frame(fte, "fact_trajectory_enriched")) == ["2022-07"]

    with pytest.raises(ValueError):
        extract_ym_from_frame(fl, "mystery_source")


# ---------------------------------------------------------------------------
# 19. Spark-shard accommodation
# ---------------------------------------------------------------------------


def test_read_accommodates_spark_shards_in_ym_dir(tmp_path: Path) -> None:
    spark_dir = tmp_path / "sparkshards" / "ym=2022-07"
    spark_dir.mkdir(parents=True)
    shard_a = _mkdf(["aaaaaa"], [1])
    shard_b = _mkdf(["bbbbbb", "cccccc"], [2, 3])
    pq.write_table(pa.Table.from_pandas(shard_a, preserve_index=False),
                   spark_dir / "part-00000-abc.parquet")
    pq.write_table(pa.Table.from_pandas(shard_b, preserve_index=False),
                   spark_dir / "part-00001-def.parquet")

    got = read_table("sparkshards", root=tmp_path)
    assert len(got) == 3
    assert set(got["icao24"]) == {"aaaaaa", "bbbbbb", "cccccc"}
    assert str(got["icao24"].dtype) == "string"
    assert row_count("sparkshards", root=tmp_path) == 3


# ---------------------------------------------------------------------------
# 20. Error path - malformed ym
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "bad",
    ["2022-13", "2022-7", "22-07", "foo", "SYN-", "2022-00", "2022/07", ""],
)
def test_write_partition_rejects_malformed_ym(tmp_path: Path, bad: str) -> None:
    df = _mkdf(["aa0001"], [1])
    with pytest.raises(ValueError):
        write_partition(df, "badym", bad, out_dir=tmp_path)


def test_write_partition_accepts_syn_sentinel(tmp_path: Path) -> None:
    df = _mkdf(["aa0001"], [1])
    out = write_partition(df, "synot", "SYN-002A", out_dir=tmp_path)
    assert out.exists()
    assert list_ym("synot", root=tmp_path) == ["SYN-002A"]


# ---------------------------------------------------------------------------
# Module-level constants sanity
# ---------------------------------------------------------------------------


def test_module_level_constants_shape() -> None:
    assert isinstance(SILVER_VAULT_ROOT, Path)
    assert isinstance(MARTS_ROOT, Path)
    assert "hub_flight" in PARTITIONED_TABLES
    assert "hub_aircraft" in NON_PARTITIONED_TABLES
    assert PARTITIONED_TABLES.isdisjoint(NON_PARTITIONED_TABLES)


# ---------------------------------------------------------------------------
# Monolith + ym_filter interaction (defence-in-depth for reader fallback)
# ---------------------------------------------------------------------------


def test_read_monolith_with_ym_filter_returns_empty(tmp_path: Path) -> None:
    """A monolithic layout has no month attribution on disk; any
    ``ym_filter`` is therefore unsatisfiable and must return an empty
    DataFrame, not the full monolith (vault_io_spec.md section 3 case 1).
    """
    df = _mkdf(["0a1b2c", "ab12cd"], [1, 2])
    _write_monolith(tmp_path / "mono", df)

    got = read_table("mono", ym_filter="2022-07", root=tmp_path)
    assert got.empty
    assert row_count("mono", ym_filter="2022-07", root=tmp_path) == 0
    # ym_filter=None still reads the full monolith.
    assert len(read_table("mono", root=tmp_path)) == 2
    assert row_count("mono", root=tmp_path) == 2
