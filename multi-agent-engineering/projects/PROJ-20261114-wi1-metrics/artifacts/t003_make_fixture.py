"""Build a tiny synthetic bronze + truth fixture for the T-003 e2e cell.

Writes a 20-aircraft x 500-sample clean synthetic dataset to a gitignored
temp dir without going through synthetic.main()'s add_noise step. Truth
parquet carries the monotonic __truth_id; bronze is the same frame (so
inject_noise(N1) in prepare_silver is the only noise applied, keeping
the truth-side join unique).
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

# Resolve etl4 package from this script's parents.
sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from etl4.bench.noise import TRUTH_ID
from etl4.ingest.synthetic import COLUMNS, make_flight


def main() -> int:
    repo_root = Path(__file__).resolve().parents[4]
    out_bronze = repo_root / "data" / "bronze" / "t003_cell" / "date=2026-01-01" / "hour=0"
    out_truth  = repo_root / "data" / "reference" / "synthetic_truth" / "t003_cell.parquet"
    out_bronze.mkdir(parents=True, exist_ok=True)
    out_truth.parent.mkdir(parents=True, exist_ok=True)

    rng = np.random.default_rng(42)
    t0 = int(pd.Timestamp("2026-01-01", tz="UTC").timestamp())
    step = 10
    n_steps = 500
    n_aircraft = 20
    frames = []
    for i in range(n_aircraft):
        icao = f"{0xA00000 + i:06x}"
        frames.append(make_flight(rng, icao, t0, n_steps, step))
    df = pd.concat(frames, ignore_index=True)
    df["hour"] = t0
    df[TRUTH_ID] = np.arange(len(df), dtype=np.int64)
    df["icao24"] = df["icao24"].astype("string")
    cols = COLUMNS + [TRUTH_ID]
    df = df[cols]

    df.to_parquet(out_bronze / "part-0.parquet", index=False)
    df.to_parquet(out_truth, index=False)
    print(f"bronze: {out_bronze / 'part-0.parquet'}")
    print(f"truth:  {out_truth}")
    print(f"rows={len(df)}  flights={df['icao24'].nunique()}  truth_id unique={df[TRUTH_ID].is_unique}")
    print("FIXTURE OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
