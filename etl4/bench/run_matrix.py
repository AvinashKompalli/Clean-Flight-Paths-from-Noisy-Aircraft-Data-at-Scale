"""Run the full (method x noise) experiment matrix on one dataset.

Two-pass medallion-aware runner:

  Pass 1 - prepare silver ONCE per noise profile (project + inject + clean +
           event-time-order), write parquet to data/silver/cleaned_states.
           Cached in-memory as {noise: silver_df} so pass 2 does not re-run
           the noise-dependent stages per method.

  Pass 2 - reconstruct (features + segment + gold write + deviations +
           metrics) for each (noise, method) combo.  Independent, so we
           run them in a ProcessPool over `--workers` (default 4) workers;
           each worker reads the silver parquet the main process wrote.

Writes:
  data/silver/cleaned_states/<source>/<noise>/part.parquet
  data/gold/marts/reconstructions/<source>/<noise>/method=<method>/part.parquet
  Postgres pipeline_runs + eval_results rows (via write_run)
  data/eval/matrix.csv                       (legacy convenience view)

Usage:
  python etl4/bench/run_matrix.py --bronze data/bronze/raw_states --source zenodo
  python etl4/bench/run_matrix.py --bronze ... --workers 1   # force sequential
"""
from __future__ import annotations

import argparse
import itertools
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pandas as pd

from etl4.bench.run_experiment import prepare_silver, reconstruct_from_silver
from etl4.sink.postgres import write_run

ROOT = Path(__file__).resolve().parents[2]

SILVER_DIR = ROOT / "data" / "silver" / "cleaned_states"
GOLD_DIR   = ROOT / "data" / "gold"   / "marts" / "reconstructions"
MATRIX_CSV = ROOT / "data" / "eval"   / "matrix.csv"

METHODS = ["time_gap", "kinematic", "kinematic_smoothing"]
NOISE_PROFILES = ["N0", "N1", "N2", "N3"]


def _load(path: Path) -> pd.DataFrame:
    if path.is_file():
        return pd.read_parquet(path)
    files = sorted(path.rglob("*.parquet"))
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


def _silver_path(source: str, noise: str) -> Path:
    return SILVER_DIR / source / noise / "part.parquet"


def _worker(args: tuple) -> dict:
    """ProcessPool entry: read silver parquet, run method, return metrics."""
    silver_path, truth_path, method, noise, source, rows_in = args
    silver = pd.read_parquet(silver_path)
    truth  = pd.read_parquet(truth_path) if truth_path else None
    return reconstruct_from_silver(
        silver, method, noise, source,
        truth=truth, rows_in=rows_in,
        write_gold_dir=GOLD_DIR,
    )


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--bronze", required=True)
    p.add_argument("--truth",  default=None)
    p.add_argument("--source", default="zenodo",
                   choices=["synthetic", "rest", "zenodo", "s3"])
    p.add_argument("--profiles", nargs="+", default=NOISE_PROFILES)
    p.add_argument("--methods",  nargs="+", default=METHODS)
    p.add_argument("--workers",  type=int, default=4,
                   help="parallel workers for pass 2 reconstruction")
    a = p.parse_args()

    bronze = _load(Path(a.bronze))
    rows_in = int(len(bronze))
    print(f"loaded {rows_in:,} bronze rows", flush=True)

    # ---- Pass 1: silver cache, one write per noise profile ----
    print(f"\n[pass 1] prepare silver for {len(a.profiles)} noise profile(s)",
          flush=True)
    t1 = time.perf_counter()
    for noise in a.profiles:
        tn = time.perf_counter()
        silver, _rejected = prepare_silver(
            bronze, noise, a.source, write_silver_dir=SILVER_DIR,
        )
        print(f"  silver  noise={noise}  rows={len(silver):,}  "
              f"wall={time.perf_counter() - tn:.1f}s  -> "
              f"{_silver_path(a.source, noise).relative_to(ROOT)}", flush=True)
        del silver
    print(f"[pass 1] done in {time.perf_counter() - t1:.1f}s", flush=True)
    del bronze  # free before parallel reconstruction

    # ---- Pass 2: reconstruct every (noise, method) combo ----
    tasks = [
        (
            str(_silver_path(a.source, noise)),
            a.truth,
            method,
            noise,
            a.source,
            rows_in,
        )
        for noise, method in itertools.product(a.profiles, a.methods)
    ]
    workers = max(1, min(a.workers, len(tasks)))
    print(f"\n[pass 2] {len(tasks)} reconstruct tasks, workers={workers}",
          flush=True)
    t2 = time.perf_counter()

    rows = []

    def _record(m: dict):
        """Write Postgres run + append matrix.csv row."""
        run_id = write_run({
            **m,
            "source":  a.source,
            "method":  m["method"],
            "noise":   m["noise"],
            "rows_in": rows_in,
        })
        if run_id:
            print(f"    [postgres run_id={run_id}]", flush=True)
        rows.append({
            "noise":      m["noise"],
            "method":     m["method"],
            "wall_s":     m["wall_s"],
            "flights":    m["n_flights"],
            "deviations": m["n_deviations"],
            "continuity": round(m["continuity"], 4),
            "rows_out":   m["rows_out"],
            "rmse_m":     round(m.get("position_rmse_m", float("nan")), 1),
            "seg_f1":     round(m.get("seg_f1", float("nan")), 3),
            "coverage":   round(m.get("coverage_ratio", float("nan")), 3),
        })
        MATRIX_CSV.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(rows).to_csv(MATRIX_CSV, index=False)

    if workers == 1:
        for t in tasks:
            print(f"  >>> noise={t[3]}  method={t[2]}", flush=True)
            m = _worker(t)
            _record(m)
            print(f"      wall={m['wall_s']}s  flights={m['n_flights']}  "
                  f"rows_out={m['rows_out']}", flush=True)
    else:
        with ProcessPoolExecutor(max_workers=workers) as ex:
            futs = {ex.submit(_worker, t): t for t in tasks}
            for fut in as_completed(futs):
                t = futs[fut]
                m = fut.result()
                print(f"  done  noise={t[3]:>3}  method={t[2]:<20}  "
                      f"wall={m['wall_s']}s  flights={m['n_flights']}",
                      flush=True)
                _record(m)

    print(f"[pass 2] done in {time.perf_counter() - t2:.1f}s", flush=True)

    df = pd.DataFrame(rows).sort_values(["noise", "method"]).reset_index(drop=True)
    print(f"\nwrote {MATRIX_CSV.relative_to(ROOT)}\n")
    print(df.to_string(index=False))


if __name__ == "__main__":
    main()
