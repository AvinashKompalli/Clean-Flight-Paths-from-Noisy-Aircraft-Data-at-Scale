"""Run one experiment: (dataset × method × noise profile) → metrics.

Writes a row to data/eval/runs.csv for each invocation. The pipeline runner
(etl4/run_pipeline.py) wraps this; use it directly to compare methods.
"""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import pandas as pd

from etl4.clean import clean, event_time_order, kinematic_features
from etl4.clean.clean import project_raw
from etl4.reconstruct import (
    segment_time_gap, segment_kinematic, smooth_and_segment,
    detect_deviations, correct_trajectory,
)
from etl4.evaluate import (
    position_rmse_m, segmentation_prf, boundary_time_error_s,
    coverage_ratio, continuity_ratio,
)
from etl4.bench.noise import inject_noise

ROOT = Path(__file__).resolve().parents[2]
RUNS_CSV = ROOT / "data" / "eval" / "runs.csv"

METHODS = {
    "time_gap":            segment_time_gap,
    "kinematic":           segment_kinematic,
    "kinematic_smoothing": smooth_and_segment,
}


def prepare_silver(bronze: pd.DataFrame, noise: str, source: str,
                   write_silver_dir: Path | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Noise-dependent, method-independent stages. Returns (silver_df, rejected).

    Safe to memoize by `noise` across the method loop, so project+noise+clean+order
    runs once per noise profile instead of once per (noise, method) combo.
    """
    df = project_raw(bronze, source=source)
    if noise != "N0":
        df = inject_noise(df, noise)
    df, rejected = clean(df)
    df = event_time_order(df)
    if write_silver_dir is not None:
        silver_path = Path(write_silver_dir) / source / noise / "part.parquet"
        silver_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(silver_path, index=False)
    return df, rejected


def reconstruct_from_silver(silver: pd.DataFrame, method: str, noise: str, source: str,
                            truth: pd.DataFrame | None = None,
                            rejected: pd.DataFrame | None = None,
                            rows_in: int | None = None,
                            expected_step_s: float = 10.0,
                            write_gold_dir: Path | None = None) -> dict:
    """Method-dependent stages: features -> segment -> gold -> deviations -> metrics."""
    t0 = time.perf_counter()
    df = kinematic_features(silver)
    fn = METHODS[method]
    df = fn(df)
    if write_gold_dir is not None:
        gold_path = Path(write_gold_dir) / source / noise / f"method={method}" / "part.parquet"
        gold_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(gold_path, index=False)
    deviations = detect_deviations(df, use_kalman_residual=(method == "kinematic_smoothing"))
    if method == "kinematic_smoothing":
        df = correct_trajectory(df)
    metrics = {
        "method":            method,
        "noise":             noise,
        "source":            source,
        "rows_in":           int(rows_in) if rows_in is not None else int(len(silver)),
        "rows_out":          int(len(df)),
        "rejected":          int(len(rejected)) if rejected is not None else 0,
        "duplicates":        int(df.attrs.get("duplicates_dropped", 0)) if hasattr(df, "attrs") else 0,
        "n_flights":         int(df["flight_id"].nunique()) if "flight_id" in df.columns else 0,
        "n_deviations":      int(len(deviations)),
        "continuity":        continuity_ratio(df, expected_step_s),
        "wall_s":            round(time.perf_counter() - t0, 3),
        "stage_times":       {},
    }
    if truth is not None and not truth.empty:
        metrics["position_rmse_m"] = position_rmse_m(df, truth)
        metrics["coverage_ratio"]  = coverage_ratio(df, truth)
        if "flight_id" in truth.columns:
            pred_b  = _flight_boundaries(df)
            truth_b = _flight_boundaries(truth)
            prf = segmentation_prf(pred_b, truth_b)
            metrics["seg_precision"] = prf["precision"]
            metrics["seg_recall"]    = prf["recall"]
            metrics["seg_f1"]        = prf["f1"]
            metrics["boundary_err_s"] = boundary_time_error_s(pred_b, truth_b)
    _append_run(metrics)
    return metrics


def run_experiment(bronze: pd.DataFrame, method: str, truth: pd.DataFrame | None = None,
                   noise: str = "N0", source: str = "synthetic",
                   expected_step_s: float = 10.0,
                   write_silver_dir: Path | None = None,
                   write_gold_dir: Path | None = None) -> dict:
    """Compose prepare_silver + reconstruct_from_silver as a single call.

    Kept as the stable public API; the matrix runner calls the two halves
    directly so it can memoize silver across the method loop.
    """
    silver, rejected = prepare_silver(bronze, noise, source,
                                      write_silver_dir=write_silver_dir)
    return reconstruct_from_silver(
        silver, method, noise, source,
        truth=truth, rejected=rejected, rows_in=len(bronze),
        expected_step_s=expected_step_s, write_gold_dir=write_gold_dir,
    )


def _flight_boundaries(df: pd.DataFrame) -> list:
    """Return (icao24, start_time) for each flight segment — these are the
    boundaries. Scoping to icao24 keeps boundaries on different aircraft from
    cross-matching in `segmentation_prf`."""
    if "flight_id" not in df.columns or df.empty:
        return []
    starts = df.groupby(["flight_id", "icao24"], sort=False)["time"].min().reset_index()
    return list(zip(starts["icao24"].astype(str), starts["time"]))


def _append_run(row: dict) -> None:
    RUNS_CSV.parent.mkdir(parents=True, exist_ok=True)
    flat = {**row, "stage_times": json.dumps(row.get("stage_times", {}))}
    pd.DataFrame([flat]).to_csv(
        RUNS_CSV, mode="a", header=not RUNS_CSV.exists(), index=False,
    )


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--bronze", required=True, help="path to a bronze Parquet dir or file")
    p.add_argument("--truth",  default=None, help="ground-truth Parquet (synthetic only)")
    p.add_argument("--method", required=True, choices=list(METHODS))
    p.add_argument("--noise",  default="N0", choices=list({"N0", "N1", "N2", "N3"}))
    p.add_argument("--source", default="synthetic", choices=["synthetic", "rest", "s3"])
    a = p.parse_args()

    bronze_path = Path(a.bronze)
    bronze = pd.read_parquet(bronze_path)
    truth  = pd.read_parquet(a.truth) if a.truth else None

    metrics = run_experiment(bronze, a.method, truth=truth, noise=a.noise, source=a.source)
    print(json.dumps(metrics, indent=2, default=str))


if __name__ == "__main__":
    main()
