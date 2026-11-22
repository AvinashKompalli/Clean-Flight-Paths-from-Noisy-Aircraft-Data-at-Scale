import sys
from pathlib import Path
import pandas as pd

root = Path(sys.argv[1])
files = sorted(root.rglob("*.parquet"))
print(f"{len(files)} parquet files:")
total = 0
for f in files:
    df = pd.read_parquet(f)
    total += len(df)
    print(f"  {f.name:>20}  rows={len(df):>10,}  cols={len(df.columns)}  size={f.stat().st_size/1e6:7.1f} MB")
print(f"\nTOTAL rows: {total:,}")
# Peek at first file
if files:
    df = pd.read_parquet(files[0])
    print(f"\nColumns ({len(df.columns)}): {list(df.columns)}")
    print(f"Unique icao24: {df['icao24'].nunique():,}")
    print(f"Time range: {df['time'].min()} -> {df['time'].max()}")
    print(df.head(3).to_string())
