"""Bulk-load vault marts into Postgres via COPY FROM STDIN.

Target tables are created by etl4/infra/create_tables.sql. This module
TRUNCATEs each target and streams its parquet replacement — mart rebuilds
are full-refresh (see etl4/docs/data_vault_design.md §6).

Loaded tables:
  dim_aircraft               (small, ~110k rows)
  fact_trajectory_enriched   (4M+ rows; the dashboard's map data)
"""
from __future__ import annotations

import argparse
import io
import os
import time
from pathlib import Path

import pandas as pd

ROOT      = Path(__file__).resolve().parents[2]
MARTS_DIR = ROOT / "data" / "gold" / "marts"

DEFAULT_DSN = os.getenv(
    "POSTGRES_DSN",
    "postgresql://etl4:change_me@localhost:5432/etl4",
)

# Column order must match the Postgres DDL in etl4/infra/create_tables.sql.
TABLES = {
    "dim_aircraft": {
        "parquet": "dim_aircraft/part.parquet",
        "cols":    ["hk_aircraft", "icao24", "registration", "typecode"],
    },
    "fact_trajectory_enriched": {
        "parquet": "fact_trajectory_enriched/part.parquet",
        "cols":    ["hk_aircraft", "icao24", "time", "lat", "lon",
                    "baroaltitude", "velocity", "heading",
                    "callsign", "typecode", "registration"],
    },
}


def _copy_df(conn, df: pd.DataFrame, table: str, cols: list[str],
             chunk_rows: int = 200_000) -> int:
    """Stream a DataFrame into Postgres via COPY. Returns row count written."""
    projected = df[cols].copy()
    total = 0
    with conn.cursor() as cur:
        copy_sql = (
            f"COPY {table} ({','.join(cols)}) "
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


def load_table(conn, table: str, cfg: dict, marts_dir: Path) -> int:
    parquet = marts_dir / cfg["parquet"]
    if not parquet.exists():
        print(f"  SKIP {table}: {parquet} not found")
        return 0
    print(f"  reading {parquet.relative_to(ROOT)} ...", end=" ", flush=True)
    t0 = time.perf_counter()
    df = pd.read_parquet(parquet)
    print(f"{len(df):,} rows in {time.perf_counter() - t0:.1f}s")

    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {table}")
    t0 = time.perf_counter()
    n = _copy_df(conn, df, table, cfg["cols"])
    print(f"  COPY -> {table}: {n:,} rows in {time.perf_counter() - t0:.1f}s")
    return n


def main():
    import psycopg

    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dsn",       default=DEFAULT_DSN)
    p.add_argument("--marts-dir", default=str(MARTS_DIR))
    p.add_argument("--only",      action="append", choices=list(TABLES),
                   help="load only this table (repeatable)")
    a = p.parse_args()

    tables = {k: v for k, v in TABLES.items() if (not a.only or k in a.only)}
    print(f"loading {len(tables)} table(s): {list(tables)}")

    with psycopg.connect(a.dsn, autocommit=False) as conn:
        total = 0
        for name, cfg in tables.items():
            total += load_table(conn, name, cfg, Path(a.marts_dir))
        conn.commit()
    print(f"\ntotal rows loaded: {total:,}")


if __name__ == "__main__":
    main()
