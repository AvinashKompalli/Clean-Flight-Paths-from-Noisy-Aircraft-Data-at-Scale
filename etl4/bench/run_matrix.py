"""Run the full (method x noise) experiment matrix on one dataset.

Writes a tidy results table to data/eval/matrix.csv showing accuracy-vs-cost
across all nine combinations, which is the headline result the project asks for.

Usage:
  python etl4/bench/run_matrix.py --bronze etl4/data/bronze/raw_states \
      --truth etl4/data/reference/synthetic_truth --source synthetic
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pandas as pd

from etl4.bench.run_experiment import run_experiment
from etl4.sink.postgres import write_run

ROOT = Path(__file__).resolve().parents[2]
MATRIX_CSV = ROOT / "data" / "eval" / "matrix.csv"

METHODS = ["time_gap", "kinematic", "kinematic_smoothing"]
NOISE_PROFILES = ["N0", "N1", "N2", "N3"]


def _load(path: Path) -> pd.DataFrame:
    if path.is_file():
        return pd.read_parquet(path)
    files = sorted(path.rglob("*.parquet"))
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--bronze", required=True)
    p.add_argument("--truth",  default=None)
    p.add_argument("--source", default="zenodo", choices=["synthetic", "rest", "zenodo"])
    p.add_argument("--profiles", nargs="+", default=NOISE_PROFILES)
    p.add_argument("--methods",  nargs="+", default=METHODS)
    a = p.parse_args()

    bronze = _load(Path(a.bronze))
    truth  = _load(Path(a.truth)) if a.truth else None
    print(f"loaded {len(bronze):,} bronze rows")

    MATRIX_CSV.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for noise in a.profiles:
        for method in a.methods:
            print(f"\n>>> noise={noise}  method={method}", flush=True)
            m = run_experiment(bronze, method, truth=truth,
                               noise=noise, source=a.source)
            # Postgres sink: writes pipeline_runs row + per-metric
            # eval_results rows. Non-fatal — returns None if DB is down.
            run_id = write_run({
                **m,
                "source": a.source,
                "method": method,
                "noise":  noise,
                "rows_in": len(bronze),
            })
            if run_id:
                print(f"    [postgres run_id={run_id}]", flush=True)
            rows.append({
                "noise":      noise,
                "method":     method,
                "wall_s":     m["wall_s"],
                "flights":    m["n_flights"],
                "deviations": m["n_deviations"],
                "continuity": round(m["continuity"], 4),
                "rows_out":   m["rows_out"],
                "rmse_m":     round(m.get("position_rmse_m", float("nan")), 1),
                "seg_f1":     round(m.get("seg_f1", float("nan")), 3),
                "coverage":   round(m.get("coverage_ratio", float("nan")), 3),
            })
            # persist after every run so partial progress survives a crash / kill
            pd.DataFrame(rows).to_csv(MATRIX_CSV, index=False)
            print(f"    wall={m['wall_s']}s  flights={m['n_flights']}  "
                  f"deviations={m['n_deviations']}  rows_out={m['rows_out']}", flush=True)

    df = pd.DataFrame(rows)
    print(f"\nwrote {MATRIX_CSV}\n")
    print(df.to_string(index=False))


if __name__ == "__main__":
    main()
