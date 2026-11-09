"""Convert Zenodo flightlist CSV.gz files into bronze parquet.

Input:  data/raw/opensky_flightlists/<version>/flightlist_<from>_<to>.csv.gz
Output: data/bronze/flightlists/<version>/flightlist_<from>_<to>.parquet

The Zenodo flightlist schema (OpenSky "Crowdsourced air traffic data",
concept DOI 3737101) is 16 columns, documented in
etl4/docs/data_vault_design.md §4.3.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
RAW_DIR    = ROOT / "data" / "raw" / "opensky_flightlists"
BRONZE_DIR = ROOT / "data" / "bronze" / "flightlists"

# Typed schema. Inference would make icao24 drop leading zeros and callsign
# lose padding; typecode etc. would be guessed badly on nulls.
DTYPES = {
    "callsign":     "string",
    "number":       "string",
    "icao24":       "string",
    "registration": "string",
    "typecode":     "string",
    "origin":       "string",
    "destination":  "string",
}
PARSE_DATES = ["firstseen", "lastseen", "day"]
# latitude_1, longitude_1, altitude_1, latitude_2, longitude_2, altitude_2
# are left to pandas (all float64).


def convert_one(src: Path, dst: Path) -> int:
    """Read one .csv.gz, write a typed parquet. Returns row count."""
    df = pd.read_csv(
        src,
        dtype=DTYPES,
        parse_dates=PARSE_DATES,
        compression="gzip",
    )
    dst.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(dst, index=False)
    return len(df)


def convert_tree(raw_dir: Path = RAW_DIR, bronze_dir: Path = BRONZE_DIR) -> dict:
    """Convert every .csv.gz under raw_dir to parquet under bronze_dir.

    Mirrors the raw_dir tree. Skips files whose parquet already exists AND
    whose source mtime is older than the target mtime (idempotent).
    """
    results = {}
    for src in sorted(raw_dir.rglob("*.csv.gz")):
        rel = src.relative_to(raw_dir)
        dst = bronze_dir / rel.with_suffix("").with_suffix(".parquet")
        if dst.exists() and dst.stat().st_mtime >= src.stat().st_mtime:
            print(f"  SKIP {dst.relative_to(ROOT)} (up to date)")
            results[str(rel)] = ("skipped", int(pd.read_parquet(dst, columns=["icao24"]).shape[0]))
            continue
        print(f"  CONVERT {rel}  ->  {dst.relative_to(ROOT)}")
        n = convert_one(src, dst)
        print(f"    rows: {n:,}")
        results[str(rel)] = ("converted", n)
    return results


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--raw-dir",    default=str(RAW_DIR))
    p.add_argument("--bronze-dir", default=str(BRONZE_DIR))
    a = p.parse_args()
    out = convert_tree(Path(a.raw_dir), Path(a.bronze_dir))
    total = sum(n for _, n in out.values())
    print(f"\n{len(out)} files, {total:,} rows total")


if __name__ == "__main__":
    main()
