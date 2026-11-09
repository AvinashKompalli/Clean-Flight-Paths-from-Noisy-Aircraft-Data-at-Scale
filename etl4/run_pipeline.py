"""End-to-end pipeline runner.

Takes a bronze Parquet source, runs each reconstruction method, appends one
metric row per method to data/eval/runs.csv, and writes one pipeline_runs
row to Postgres. With --write-silver and/or --write-gold, also persists:
  silver = cleaned + event-time-ordered canonical schema (plan.md §6.1)
  gold   = reconstructed per-flight output per method

Usage:
  python etl4/run_pipeline.py --bronze data/bronze/synthetic --source synthetic \
      --truth data/reference/synthetic_truth --write-silver --write-gold
  python etl4/run_pipeline.py --bronze data/bronze/raw_states --source s3
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Allow `python etl4/run_pipeline.py ...` without `pip install -e`.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd

from etl4.bench.run_experiment import run_experiment
from etl4.sink.postgres import write_run

ROOT = Path(__file__).resolve().parents[1]


def _load_parquet(path: Path) -> pd.DataFrame:
    """Load one Parquet file or every Parquet under a directory tree."""
    if path.is_file():
        return pd.read_parquet(path)
    files = sorted(path.rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"no parquet files under {path}")
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--bronze", required=True,
                   help="bronze Parquet dir or file (from screpper/synthetic/replay)")
    p.add_argument("--truth",  default=None,
                   help="ground-truth Parquet (synthetic only)")
    p.add_argument("--source", default="synthetic", choices=["synthetic", "rest", "s3"])
    p.add_argument("--noise",  default="N0", choices=["N0", "N1", "N2", "N3"])
    p.add_argument("--methods", nargs="+",
                   default=["time_gap", "kinematic", "kinematic_smoothing"])
    p.add_argument("--write-silver", action="store_true",
                   help="persist cleaned canonical schema to data/silver/<source>/<noise>/")
    p.add_argument("--write-gold", action="store_true",
                   help="persist reconstructed per-flight output to data/gold/<source>/<noise>/method=<m>/")
    a = p.parse_args()

    silver_dir = ROOT / "data" / "silver" if a.write_silver else None
    gold_dir   = ROOT / "data" / "gold"   if a.write_gold   else None

    bronze = _load_parquet(Path(a.bronze))
    truth  = _load_parquet(Path(a.truth)) if a.truth else None
    print(f"loaded {len(bronze):,} bronze rows "
          f"({bronze['icao24'].nunique() if 'icao24' in bronze.columns else '?'} unique icao24)")

    for method in a.methods:
        print(f"\n--- running method = {method} ---")
        m = run_experiment(bronze, method, truth=truth, noise=a.noise, source=a.source,
                           write_silver_dir=silver_dir, write_gold_dir=gold_dir)
        run_id = write_run({
            **m,
            "source": a.source,
            "method": method,
            "noise":  a.noise,
            "rows_in": len(bronze),
        })
        headline = (
            f"method={method:<22}  rows_out={m['rows_out']:,}  "
            f"flights={m['n_flights']}  deviations={m['n_deviations']}  "
            f"wall={m['wall_s']}s  continuity={m['continuity']:.3f}"
        )
        if "position_rmse_m" in m:
            headline += f"  rmse={m['position_rmse_m']:.1f} m  seg_f1={m.get('seg_f1', 0):.3f}"
        if run_id:
            headline += f"  [pg run_id={run_id}]"
        print(headline)


if __name__ == "__main__":
    main()
