"""Partition-aware IO for the silver vault and gold marts (IMPR-01, ADR 2026-11-13).

Single reader/writer surface over ``data/silver/vault/`` and
``data/gold/marts/`` under the ``ym=YYYY-MM`` partition contract. Three
concerns, one module:

1. Partition-aware reads with monolithic fallback. Readers call
   ``read_table(name, ym_filter=...)`` and get the right rows whether
   the table has been migrated yet or not (vault_io_spec.md section 3
   Reader fallback contract).

2. Atomic writes via ``.part.parquet.<pid>.tmp`` + ``os.replace``.
   Deterministic ``part.parquet`` filenames for resume; ``ym`` stays
   inside the file body when the caller includes it in the frame, so
   CLAUDE.md section 7 "read individual files and concat" continues to
   work.

3. ``ym`` derivation helpers. One source of truth shared by the vault
   writer, the migration script, the mart writer, and the Postgres
   backfill (architecture.md section 2.5).

Reader-fallback precedence when both layouts coexist (migration window):
partitioned wins; the stale monolith is ignored. Empty tables return
``pd.DataFrame()`` with no columns -- existing callers already handle
that shape (vault_io_spec.md section 5.3).

The reader globs ``<ym_dir>/*.parquet`` rather than only ``part.parquet``
so the Spark-emitted ``part-NNNNN-*.parquet`` shards from
``raw_vault_spark.py`` load transparently (vault_io_spec.md section 6
Spark writer note). The writer still emits a deterministic ``part.parquet``
for resume.

See: ``multi-agent-engineering/projects/PROJ-20261118-impr01-partition-contract/vault_io_spec.md``
"""
from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Optional, Union

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

__all__ = [
    "SILVER_VAULT_ROOT",
    "MARTS_ROOT",
    "PARTITIONED_TABLES",
    "NON_PARTITIONED_TABLES",
    "read_table",
    "write_partition",
    "row_count",
    "list_ym",
    "ym_from_filename",
    "ym_from_date",
    "ym_from_epoch",
    "extract_ym_from_frame",
    "extract_ym_from_path",
]

ROOT: Path = Path(__file__).resolve().parents[2]
SILVER_VAULT_ROOT: Path = ROOT / "data" / "silver" / "vault"
MARTS_ROOT:        Path = ROOT / "data" / "gold" / "marts"

PARTITIONED_TABLES: frozenset = frozenset({
    "hub_flight",
    "link_flight_aircraft",
    "link_flight_route",
    "sat_flight_summary",
    "sat_state_observation",
    "fact_flight",
    "fact_trajectory_enriched",
})

NON_PARTITIONED_TABLES: frozenset = frozenset({
    "hub_aircraft",
    "hub_airport",
    "sat_aircraft_identity",
    "dim_aircraft",
    "dim_airport",
    "fact_experiment_result",
})

YmFilter = Union[str, list, tuple, None]

_FL_PATTERN = re.compile(r"flightlist_(\d{4})(\d{2})\d{2}_\d{8}\.parquet$")
_PATH_DATE_PATTERN = re.compile(r"date=(\d{4})-(\d{2})-\d{2}")
_PATH_YM_PATTERN   = re.compile(r"ym=([0-9]{4}-(?:0[1-9]|1[0-2])|SYN-[0-9A-Fa-f]+)")
_YM_REGEX  = re.compile(r"^[0-9]{4}-(0[1-9]|1[0-2])$")
_SYN_REGEX = re.compile(r"^SYN-[0-9A-Fa-f]+$")


def ym_from_filename(path: Union[str, Path]) -> Optional[str]:
    """Flightlist bronze filename to ``'YYYY-MM'``; ``None`` when no match.

    Version-agnostic: the regex matches on the basename only, so bronze
    trees mixing ``v23.00/`` and future ``v24.00/`` subdirs both resolve.
    Accepts Windows backslash paths.
    """
    m = _FL_PATTERN.search(str(path))
    return f"{m.group(1)}-{m.group(2)}" if m else None


def ym_from_epoch(series: pd.Series) -> pd.Series:
    """Epoch-second column to ``'YYYY-MM'`` string column (nullable).

    Vectorised. Matches Postgres ``to_char(to_timestamp(time), 'YYYY-MM')``
    bit-for-bit under UTC, which is the invariant the
    ``fact_trajectory_enriched`` backfill depends on.
    """
    return (
        pd.to_datetime(series.astype("Int64"), unit="s", utc=True)
          .dt.strftime("%Y-%m")
          .astype("string")
    )


def ym_from_date(series: pd.Series) -> pd.Series:
    """Date / datetime column to ``'YYYY-MM'`` string column (nullable)."""
    return (
        pd.to_datetime(series, utc=True)
          .dt.strftime("%Y-%m")
          .astype("string")
    )


def extract_ym_from_frame(df: pd.DataFrame, source: str) -> pd.Series:
    """Dispatch to the right ym helper based on the frame's origin.

    Raises ``ValueError`` on unknown ``source`` so the mart writer never
    silently derives ``ym`` from the wrong column.
    """
    if source == "flightlist":
        return ym_from_date(df["day"])
    if source == "sat_flight_summary":
        return ym_from_epoch(df["firstseen"])
    if source == "sat_state_observation":
        return ym_from_epoch(df["time"])
    if source == "fact_trajectory_enriched":
        return ym_from_epoch(df["time"])
    raise ValueError(
        "unknown source %r; expected one of "
        "'flightlist', 'sat_flight_summary', 'sat_state_observation', "
        "'fact_trajectory_enriched'" % (source,)
    )


def extract_ym_from_path(path: Union[str, Path]) -> str:
    """Path to ``'YYYY-MM'``; raises ``ValueError`` when no segment matches.

    Precedence (highest first):
      1. ``ym=YYYY-MM`` or ``ym=SYN-<hex>`` anywhere in the path
      2. ``date=YYYY-MM-DD`` anywhere in the path
      3. ``flightlist_YYYYMMDD_...`` bronze filename
    """
    s = str(path)
    m = _PATH_YM_PATTERN.search(s)
    if m:
        return m.group(1)
    m = _PATH_DATE_PATTERN.search(s)
    if m:
        return "%s-%s" % (m.group(1), m.group(2))
    ym = ym_from_filename(s)
    if ym is not None:
        return ym
    raise ValueError("cannot derive ym from path: %r" % (s,))


def _ym_dirs(table_dir: Path) -> list:
    """Sorted list of ``ym=...`` directories under ``table_dir`` that
    hold at least one ``*.parquet`` file (pandas or Spark shards).

    The ``.part.parquet.<pid>.tmp`` writer-temp files end in ``.tmp`` and
    are correctly excluded by the ``*.parquet`` glob.
    """
    if not table_dir.is_dir():
        return []
    out = []
    for d in table_dir.glob("ym=*"):
        if d.is_dir() and any(d.glob("*.parquet")):
            out.append(d)
    return sorted(out)


def _parquet_files(ym_dir: Path) -> list:
    return sorted(p for p in ym_dir.glob("*.parquet") if p.is_file())


def _select_ym_dirs(ym_dirs: list, ym_filter: YmFilter) -> list:
    if ym_filter is None:
        return ym_dirs
    if isinstance(ym_filter, str):
        return [d for d in ym_dirs if d.name == ("ym=" + ym_filter)]
    if isinstance(ym_filter, tuple):
        if len(ym_filter) != 2:
            raise ValueError(
                "tuple ym_filter must be (from_ym, to_ym); got length %d"
                % (len(ym_filter),)
            )
        lo, hi = ym_filter
        return [d for d in ym_dirs if lo <= d.name.split("=", 1)[1] <= hi]
    if isinstance(ym_filter, list):
        wanted = set(("ym=" + y) for y in ym_filter)
        return [d for d in ym_dirs if d.name in wanted]
    raise TypeError(
        "ym_filter must be None | str | list[str] | tuple[str, str]; got %s"
        % (type(ym_filter).__name__,)
    )


def _read_one_file(path: Path, columns: Optional[list]) -> pd.DataFrame:
    tbl = pq.read_table(path, columns=columns)
    df = tbl.to_pandas()
    if "icao24" in df.columns:
        df["icao24"] = df["icao24"].astype("string")
    return df


def read_table(
    name: str,
    *,
    ym_filter: YmFilter = None,
    columns: Optional[list] = None,
    root: Path = SILVER_VAULT_ROOT,
) -> pd.DataFrame:
    """Read a vault / mart table; partition-aware with monolithic fallback.

    ``ym_filter`` accepts:
      * ``None``            -> all partitions (or monolith under fallback)
      * ``'YYYY-MM'``       -> exactly one partition
      * ``list['YYYY-MM']`` -> explicit set of partitions
      * ``(from, to)``      -> inclusive calendar-month range

    ``columns`` is pushed down to pyarrow. Returns an empty DataFrame
    (no columns inferred) when the table dir is absent, empty, or all
    ``ym_filter``-matched partitions miss. ``icao24``, when present in
    the result, is cast to pandas ``'string'`` dtype -- defends against
    pyarrow dataset readers that default to ``object`` and against
    pandas 3.x which defaults to ``str``.
    """
    table_dir = Path(root) / name
    ym_dirs = _ym_dirs(table_dir)

    if not ym_dirs:
        monolith = table_dir / "part.parquet"
        if monolith.is_file():
            if ym_filter is not None:
                return pd.DataFrame()
            return _read_one_file(monolith, columns)
        return pd.DataFrame()

    selected = _select_ym_dirs(ym_dirs, ym_filter)
    if not selected:
        return pd.DataFrame()

    frames = []
    for ym_dir in selected:
        for f in _parquet_files(ym_dir):
            frames.append(_read_one_file(f, columns))
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    if "icao24" in df.columns:
        df["icao24"] = df["icao24"].astype("string")
    return df


def write_partition(
    df: pd.DataFrame,
    name: str,
    ym: str,
    *,
    out_dir: Path = SILVER_VAULT_ROOT,
    compression: str = "snappy",
) -> Path:
    """Atomically write one month as ``<out_dir>/<name>/ym=<ym>/part.parquet``.

    Writes ``.part.parquet.<pid>.tmp`` in the same directory first, then
    ``os.replace``s onto the final name. ``os.replace`` is atomic on
    both Windows and POSIX when source and target are on the same
    filesystem; routing through the system temp dir would break that.

    ``ym`` must match ``^[0-9]{4}-(0[1-9]|1[0-2])$`` or the synthetic
    sentinel ``^SYN-[0-9A-Fa-f]+$``.
    """
    if not (_YM_REGEX.match(ym) or _SYN_REGEX.match(ym)):
        raise ValueError(
            "ym %r is not 'YYYY-MM' nor 'SYN-<hex>'" % (ym,)
        )
    target_dir = Path(out_dir) / name / ("ym=" + ym)
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / "part.parquet"
    tmp = target_dir / (".part.parquet.%d.tmp" % (os.getpid(),))

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, tmp, compression=compression)
    os.replace(tmp, target)
    return target


def row_count(
    name: str,
    *,
    ym_filter: YmFilter = None,
    root: Path = SILVER_VAULT_ROOT,
) -> int:
    """Row count of a vault / mart table from parquet footer only; O(files).

    Same partition / fallback resolution as :func:`read_table`.
    """
    table_dir = Path(root) / name
    ym_dirs = _ym_dirs(table_dir)

    if not ym_dirs:
        monolith = table_dir / "part.parquet"
        if monolith.is_file():
            if ym_filter is not None:
                return 0
            return pq.ParquetFile(monolith).metadata.num_rows
        return 0

    selected = _select_ym_dirs(ym_dirs, ym_filter)
    total = 0
    for ym_dir in selected:
        for f in _parquet_files(ym_dir):
            total += pq.ParquetFile(f).metadata.num_rows
    return total


def list_ym(name: str, *, root: Path = SILVER_VAULT_ROOT) -> list:
    """Sorted list of ``'YYYY-MM'`` (or ``'SYN-*'``) partitions for a table.

    Returns ``[]`` when the table is non-partitioned (monolith only) or
    does not exist on disk. Used by ``run_e2e`` to detect gaps against
    ``pipeline_watermarks.last_ym`` (FR-17).
    """
    ym_dirs = _ym_dirs(Path(root) / name)
    return sorted(d.name.split("=", 1)[1] for d in ym_dirs)
