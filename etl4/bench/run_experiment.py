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


def run_experiment(bronze: pd.DataFrame, method: str, truth: pd.DataFrame | None = None,
                   noise: str = "N0", source: str = "synthetic",
                   expected_step_s: float = 10.0) -> dict:
    """Run one method end-to-end. Returns a dict of metrics + wall-clock cost."""
    t0 = time.perf_counter()
    stage_times = {}

    def stage(label):
        now = time.perf_counter()
        stage_times[label] = now - t0 - sum(stage_times.values())

    df = project_raw(bronze, source=source)
    stage("project")

    if noise != "N0":
        df = inject_noise(df, noise)
    stage("noise")

    df, rejected = clean(df)
    stage("clean")

    df = event_time_order(df)
    stage("order")

    df = kinematic_features(df)
    stage("features")

    fn = METHODS[method]
    df = fn(df)
    stage("segment")

    deviations = detect_deviations(df, use_kalman_residual=(method == "kinematic_smoothing"))
    stage("deviations")

    if method == "kinematic_smoothing":
        df = correct_trajectory(df)
    stage("correction")

    metrics = {
        "method":            method,
        "noise":             noise,
        "source":            source,
        "rows_in":           int(len(bronze)),
        "rows_out":          int(len(df)),
        "rejected":          int(len(rejected)),
        "duplicates":        int(df.attrs.get("duplicates_dropped", 0)) if hasattr(df, "attrs") else 0,
        "n_flights":         int(df["flight_id"].nunique()) if "flight_id" in df.columns else 0,
        "n_deviations":      int(len(deviations)),
        "continuity":        continuity_ratio(df, expected_step_s),
        "wall_s":            round(time.perf_counter() - t0, 3),
        "stage_times":       {k: round(v, 3) for k, v in stage_times.items()},
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


def _flight_boundaries(df: pd.DataFrame) -> list:
    """Return the first time of each flight segment — these are the boundaries."""
    if "flight_id" not in df.columns:
        return []
    return df.groupby("flight_id")["time"].min().tolist()


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
