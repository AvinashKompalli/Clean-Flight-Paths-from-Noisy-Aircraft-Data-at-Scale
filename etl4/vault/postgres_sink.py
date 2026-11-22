"""Bulk-load vault marts into Postgres via staged COPY + INSERT ON CONFLICT.

For each target the sink does, inside one transaction:

    1. CREATE TEMP TABLE stg_<target> (LIKE <target> INCLUDING DEFAULTS)
       ON COMMIT DROP
    2. COPY FROM STDIN into stg_<target>
    3. INSERT INTO <target> SELECT DISTINCT ON (<pk>) ... FROM stg_<target>
         ORDER BY <pk>, <tiebreaker>
         ON CONFLICT (<pk>) DO UPDATE SET <non_pk_assignments>
    4. Advance pipeline_watermarks('sink', '<target>', --ym) via
       etl4.vault.watermark.set in the SAME transaction
    5. COMMIT

The per-ym loop lives in the caller (etl4/run_e2e.py, orchestration DAGs).
This module loads exactly one ym per invocation.

Targets:
  dim_aircraft               -- non-partitioned on disk, idempotent upsert
  fact_trajectory_enriched   -- partitioned at data/gold/marts/.../ym=YYYY-MM/

See multi-agent-engineering/projects/PROJ-20261118-impr01-partition-contract/
architecture.md sections 4.1-4.3.
"""
from __future__ import annotations

import argparse
import io
import os
import re
import time
from pathlib import Path

import pandas as pd

from etl4.vault import watermark

ROOT      = Path(__file__).resolve().parents[2]
MARTS_DIR = ROOT / "data" / "gold" / "marts"

DEFAULT_DSN = os.getenv(
    "POSTGRES_DSN",
    "postgresql://etl4:change_me@localhost:5432/etl4",
)

_YM_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")

# Columns must match the Postgres DDL in etl4/infra/create_tables.sql.
# fact_trajectory_enriched carries ym after the IMPR-01 migration
# (architecture.md section 6.1). If the column is missing the staged
# INSERT will raise UndefinedColumn, which is the intended "fail
# informatively" signal.
TABLES = {
    "dim_aircraft": {
        "cols":     ["hk_aircraft", "icao24", "registration", "typecode"],
        "parquet":  "dim_aircraft/part.parquet",
        "pk":       ["hk_aircraft"],
        "partitioned": False,
    },
    "fact_trajectory_enriched": {
        "cols":     ["hk_aircraft", "icao24", "time", "lat", "lon",
                     "baroaltitude", "velocity", "heading",
                     "callsign", "typecode", "registration", "ym"],
        "parquet":  "fact_trajectory_enriched",
        "pk":       ["hk_aircraft", "time"],
        "partitioned": True,
    },
}


def _copy_into(cur, df: pd.DataFrame, stg_table: str, cols: list[str],
               chunk_rows: int = 200_000) -> int:
    projected = df[cols]
    total = 0
    copy_sql = (
        f"COPY {stg_table} ({','.join(cols)}) "
        f"FROM STDIN WITH (FORMAT CSV, NULL '\\N')"
    )
    with cur.copy(copy_sql) as copy:
        for start in range(0, len(projected), chunk_rows):
            buf = io.StringIO()
            chunk = projected.iloc[start:start + chunk_rows]
            chunk.to_csv(buf, index=False, header=False, na_rep="\\N")
            copy.write(buf.getvalue())
            total += len(chunk)
    return total


def _read_fact(marts_dir: Path, ym: str) -> pd.DataFrame:
    """Load fact_trajectory_enriched rows for one ym.

    Primary path: partitioned ``fact_trajectory_enriched/ym=YYYY-MM/part.parquet``.
    Fallback: monolithic ``fact_trajectory_enriched/part.parquet`` filtered to
    rows whose epoch ``time`` falls in the requested calendar month.

    If the file carries no ``ym`` column (monolith pre-migration), it is
    materialized from ``--ym`` so the staged INSERT can populate the new
    NOT NULL column.
    """
    base = marts_dir / "fact_trajectory_enriched"
    partitioned = base / f"ym={ym}" / "part.parquet"
    monolith    = base / "part.parquet"

    if partitioned.exists():
        df = pd.read_parquet(partitioned)
    elif monolith.exists():
        df = pd.read_parquet(monolith)
        if "time" in df.columns:
            month_of = (
                pd.to_datetime(df["time"].astype("Int64"), unit="s", utc=True)
                  .dt.strftime("%Y-%m").astype("string")
            )
            df = df.loc[month_of == ym].copy()
    else:
        raise FileNotFoundError(
            f"fact_trajectory_enriched: neither {partitioned} nor {monolith} exists"
        )

    if "ym" not in df.columns or df["ym"].isna().all():
        df["ym"] = pd.array([ym] * len(df), dtype="string")
    else:
        df["ym"] = df["ym"].astype("string").fillna(ym)

    if "icao24" in df.columns:
        df["icao24"] = df["icao24"].astype("string")
    return df


def _read_dim_aircraft(marts_dir: Path) -> pd.DataFrame:
    parquet = marts_dir / "dim_aircraft" / "part.parquet"
    if not parquet.exists():
        raise FileNotFoundError(parquet)
    df = pd.read_parquet(parquet)
    if "icao24" in df.columns:
        df["icao24"] = df["icao24"].astype("string")
    return df


def _load_dim_aircraft(conn, ym: str, df: pd.DataFrame) -> int:
    cols = TABLES["dim_aircraft"]["cols"]
    with conn.cursor() as cur:
        cur.execute(
            "CREATE TEMP TABLE stg_dim_aircraft "
            "(LIKE dim_aircraft INCLUDING DEFAULTS) ON COMMIT DROP"
        )
        _copy_into(cur, df, "stg_dim_aircraft", cols)
        cur.execute(
            """
            INSERT INTO dim_aircraft (hk_aircraft, icao24, registration, typecode)
            SELECT DISTINCT ON (hk_aircraft)
                   hk_aircraft, icao24, registration, typecode
              FROM stg_dim_aircraft
             ORDER BY hk_aircraft,
                      (registration IS NOT NULL) DESC,
                      (typecode     IS NOT NULL) DESC
            ON CONFLICT (hk_aircraft) DO UPDATE SET
                icao24       = EXCLUDED.icao24,
                registration = COALESCE(EXCLUDED.registration, dim_aircraft.registration),
                typecode     = COALESCE(EXCLUDED.typecode,     dim_aircraft.typecode)
            """
        )
        cur.execute("SELECT count(*) FROM dim_aircraft")
        row_count = cur.fetchone()[0]
        watermark.set("sink", "dim_aircraft", ym, row_count, conn)
    return row_count


def _load_fact_trajectory(conn, ym: str, df: pd.DataFrame) -> int:
    cols = TABLES["fact_trajectory_enriched"]["cols"]
    with conn.cursor() as cur:
        cur.execute(
            "CREATE TEMP TABLE stg_fact_trajectory_enriched "
            "(LIKE fact_trajectory_enriched INCLUDING DEFAULTS) ON COMMIT DROP"
        )
        _copy_into(cur, df, "stg_fact_trajectory_enriched", cols)
        cur.execute(
            """
            INSERT INTO fact_trajectory_enriched
                (hk_aircraft, icao24, time, lat, lon, baroaltitude,
                 velocity, heading, callsign, typecode, registration, ym)
            SELECT DISTINCT ON (hk_aircraft, time)
                   hk_aircraft, icao24, time, lat, lon, baroaltitude,
                   velocity, heading, callsign, typecode, registration, ym
              FROM stg_fact_trajectory_enriched
             ORDER BY hk_aircraft, time,
                      (callsign IS NOT NULL) DESC,
                      (typecode IS NOT NULL) DESC
            ON CONFLICT (hk_aircraft, time) DO UPDATE SET
                lat          = EXCLUDED.lat,
                lon          = EXCLUDED.lon,
                baroaltitude = EXCLUDED.baroaltitude,
                velocity     = EXCLUDED.velocity,
                heading      = EXCLUDED.heading,
                callsign     = COALESCE(EXCLUDED.callsign,     fact_trajectory_enriched.callsign),
                typecode     = COALESCE(EXCLUDED.typecode,     fact_trajectory_enriched.typecode),
                registration = COALESCE(EXCLUDED.registration, fact_trajectory_enriched.registration),
                ym           = EXCLUDED.ym
            """
        )
        cur.execute(
            "SELECT count(*) FROM fact_trajectory_enriched WHERE ym = %s",
            (ym,),
        )
        row_count = cur.fetchone()[0]
        watermark.set("sink", "fact_trajectory_enriched", ym, row_count, conn)
    return row_count


def _display(path: Path) -> str:
    """Pretty path for logs: relative to repo root when possible, else absolute."""
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def load_table(conn, table: str, ym: str, marts_dir: Path) -> int:
    """Stage + upsert a single target for one ym. Commits on success."""
    if not _YM_RE.match(ym):
        raise ValueError(f"ym must be YYYY-MM, got: {ym!r}")

    if table == "dim_aircraft":
        parquet = marts_dir / TABLES[table]["parquet"]
        if not parquet.exists():
            print(f"  SKIP {table}: {parquet} not found", flush=True)
            return 0
        print(f"  reading {_display(parquet)} ...",
              end=" ", flush=True)
        t0 = time.perf_counter()
        df = _read_dim_aircraft(marts_dir)
        print(f"{len(df):,} rows in {time.perf_counter() - t0:.1f}s", flush=True)
        t0 = time.perf_counter()
        row_count = _load_dim_aircraft(conn, ym, df)
        conn.commit()
        print(f"  UPSERT -> dim_aircraft (ym={ym}): "
              f"{row_count:,} total rows in {time.perf_counter() - t0:.1f}s",
              flush=True)
        return row_count

    if table == "fact_trajectory_enriched":
        base = marts_dir / "fact_trajectory_enriched"
        partitioned = base / f"ym={ym}" / "part.parquet"
        monolith    = base / "part.parquet"
        if not partitioned.exists() and not monolith.exists():
            print(f"  SKIP {table}: no parquet at {partitioned} or {monolith}",
                  flush=True)
            return 0
        src = partitioned if partitioned.exists() else monolith
        print(f"  reading {_display(src)} ...", end=" ", flush=True)
        t0 = time.perf_counter()
        df = _read_fact(marts_dir, ym)
        print(f"{len(df):,} rows in {time.perf_counter() - t0:.1f}s", flush=True)
        if df.empty:
            print(f"  SKIP {table}: zero rows for ym={ym}", flush=True)
            return 0
        t0 = time.perf_counter()
        row_count = _load_fact_trajectory(conn, ym, df)
        conn.commit()
        print(f"  UPSERT -> fact_trajectory_enriched (ym={ym}): "
              f"{row_count:,} rows in {time.perf_counter() - t0:.1f}s",
              flush=True)
        return row_count

    raise ValueError(f"unknown table: {table!r}")


def main():
    import psycopg

    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dsn",       default=DEFAULT_DSN)
    p.add_argument("--marts-dir", default=str(MARTS_DIR))
    p.add_argument("--ym",        required=True,
                   help="partition month to load, format YYYY-MM")
    p.add_argument("--only",      action="append", choices=list(TABLES),
                   help="load only this table (repeatable)")
    a = p.parse_args()

    if not _YM_RE.match(a.ym):
        p.error(f"--ym must be YYYY-MM, got: {a.ym!r}")

    tables = [t for t in TABLES if (not a.only or t in a.only)]
    print(f"loading ym={a.ym} into {len(tables)} table(s): {tables}",
          flush=True)

    with psycopg.connect(a.dsn, autocommit=False) as conn:
        total = 0
        for name in tables:
            total += load_table(conn, name, a.ym, Path(a.marts_dir))
    print(f"\ntotal rows upserted: {total:,}", flush=True)


if __name__ == "__main__":
    main()
