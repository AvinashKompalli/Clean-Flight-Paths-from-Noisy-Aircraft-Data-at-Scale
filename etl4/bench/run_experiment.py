"""Run one experiment: (dataset x method x noise profile) -> metrics.

Writes a row to data/eval/runs.csv for each invocation. The pipeline runner
(etl4/run_pipeline.py) wraps this; use it directly to compare methods.

Threads a `NoiseTruth` sidecar from `inject_noise` through to the metric
functions so the synthetic path produces the full feature-metric matrix
the course PDF section 3 requires (`position_rmse_m`, `seg_precision`,
`seg_recall`, `seg_f1`, `coverage_ratio`, `boundary_err_s`, plus the
non-persisted deviation scores). ADR 2026-11-14 is the contract.
"""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import numpy as np
import pandas as pd

from etl4.clean import clean, event_time_order, kinematic_features
from etl4.clean.clean import project_raw
from etl4.reconstruct import (
    segment_time_gap, segment_kinematic, smooth_and_segment,
    detect_deviations, correct_trajectory,
)
from etl4.evaluate import (
    position_rmse_m, segmentation_prf1, boundary_time_error_s,
    coverage_ratio, continuity_ratio, deviation_prf1,
)
from etl4.evaluate.metrics import BOUNDARY_TOLERANCE_S, TRUTH_ID
from etl4.bench.noise import inject_noise, NoiseTruth, _infer_boundaries

ROOT = Path(__file__).resolve().parents[2]
RUNS_CSV = ROOT / "data" / "eval" / "runs.csv"

METHODS = {
    "time_gap":            segment_time_gap,
    "kinematic":           segment_kinematic,
    "kinematic_smoothing": smooth_and_segment,
}


def prepare_silver(
    bronze: pd.DataFrame,
    noise: str,
    source: str,
    write_silver_dir: Path | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, NoiseTruth | None]:
    """Noise-dependent, method-independent stages.

    Returns `(silver_df, rejected, noise_truth)`. `noise_truth` is `None`
    only for N0 (`inject_noise` is skipped). It is the in-memory sidecar
    downstream metrics score against; the ADR forbids persisting it to
    disk, but it pickles cleanly across the matrix ProcessPool so pass-2
    workers receive the real `jumped_ids` / `shuffled_ids` /
    `duplicated_ids` from pass 1. When absent (N0, or a caller that does
    not thread it through), `reconstruct_from_silver` synthesises a
    minimal survivor-only proxy from the truth frame and marks
    deviation scoring as not applicable.

    Safe to memoise by `noise` across the method loop: project + noise +
    clean + order runs once per noise profile instead of per (noise,
    method) combo.
    """
    df = project_raw(bronze, source=source)
    noise_truth: NoiseTruth | None = None
    if noise != "N0":
        df, noise_truth = inject_noise(df, noise)
    df, rejected = clean(df)
    df = event_time_order(df)
    if write_silver_dir is not None:
        silver_path = Path(write_silver_dir) / source / noise / "part.parquet"
        silver_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(silver_path, index=False)
    return df, rejected, noise_truth


def _synthesise_noise_truth(truth_df: pd.DataFrame, silver_df: pd.DataFrame,
                            noise: str) -> NoiseTruth | None:
    """Rebuild a minimal `NoiseTruth` from a truth frame plus the
    post-noise silver when the real sidecar was not supplied (e.g. N0
    which skips `inject_noise`, or a caller that does not thread it
    through). `jumped_ids` stays empty here; the deviation scorer is
    gated off by `from_proxy` in the caller so this proxy never emits
    a fabricated zero to Postgres."""
    if truth_df is None or TRUTH_ID not in truth_df.columns:
        return None
    original_ids = frozenset(
        int(x) for x in truth_df[TRUTH_ID].dropna().astype("int64").unique()
    )
    if not original_ids:
        return None
    surviving = (
        frozenset(int(x) for x in silver_df[TRUTH_ID].dropna().astype("int64").unique())
        if TRUTH_ID in silver_df.columns else frozenset()
    )
    dropped = original_ids - surviving
    return NoiseTruth(
        profile=noise,
        seed=42,
        original_row_count=len(original_ids),
        original_ids=original_ids,
        original_coords=pd.DataFrame(
            columns=[TRUTH_ID, "lat", "lon", "baroaltitude"]),
        dropped_ids=dropped,
        duplicated_ids=frozenset(),
        shuffled_ids=frozenset(),
        jumped_ids=frozenset(),
        true_boundaries=_infer_boundaries(truth_df),
    )


def _mask_from_events(df: pd.DataFrame, events: pd.DataFrame) -> pd.Series:
    """Collapse the event table (one row per contiguous flagged run) into
    a per-row boolean mask aligned with `df.index`. Used only to score
    `deviation_prf1`; detection itself still lives in
    `reconstruct/deviations.py`."""
    mask = pd.Series(False, index=df.index)
    if events is None or events.empty or "flight_id" not in df.columns:
        return mask
    for ev in events.itertuples(index=False):
        hit = (
            (df["flight_id"] == ev.flight_id)
            & (df["time"] >= ev.start_time)
            & (df["time"] <= ev.end_time)
        )
        mask = mask | hit
    return mask


def reconstruct_from_silver(silver: pd.DataFrame, method: str, noise: str, source: str,
                            truth: pd.DataFrame | None = None,
                            rejected: pd.DataFrame | None = None,
                            rows_in: int | None = None,
                            expected_step_s: float = 10.0,
                            write_gold_dir: Path | None = None,
                            noise_truth: NoiseTruth | None = None) -> dict:
    """Method-dependent stages: features -> segment -> gold -> deviations -> metrics.

    `noise_truth` is the in-process sidecar built by `prepare_silver`.
    When absent (matrix process-pool path), a proxy is synthesised from
    `truth` + `silver`; `jumped_ids` is empty in that path so the
    deviation PRF effectively zeroes out, but the survivor-based
    coverage / RMSE / segmentation metrics still populate.
    """
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
    metrics: dict = {
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

    # Ground-truth-dependent metrics only on the synthetic path. Real
    # sources have no __truth_id, so we leave the keys unset and the
    # sink + CSV writers render them as NULL / nan.
    # Enter whenever EITHER a truth DataFrame with __truth_id is supplied
    # OR an in-memory NoiseTruth sidecar carries original_ids. The four
    # sidecar-only scorers (coverage_ratio, segmentation_prf1,
    # boundary_time_error_s, deviation_prf1) populate from NoiseTruth
    # alone; position_rmse_m still needs the truth DataFrame because the
    # NoiseTruth.original_coords field only captures pre-jump coords for
    # the jumped_ids subset, not every surviving id.
    has_truth_df = (
        truth is not None and not truth.empty and TRUTH_ID in truth.columns
    )
    has_sidecar = (
        noise_truth is not None and bool(noise_truth.original_ids)
    )
    if source == "synthetic" and (has_truth_df or has_sidecar):
        # from_proxy flags cells where NoiseTruth was reconstructed from
        # truth + silver (matrix path without an in-memory sidecar, or
        # N0 which skips inject_noise). Such a proxy has jumped_ids=empty
        # so scoring deviation_prf1 would emit a fabricated zero; skip
        # the metric in that case rather than persist noise.
        from_proxy = noise_truth is None
        if from_proxy and has_truth_df:
            noise_truth = _synthesise_noise_truth(truth, df, noise)
        if noise_truth is not None:
            if has_truth_df:
                rmse = position_rmse_m(df, noise_truth, truth)
                if rmse is not None and not (isinstance(rmse, float) and np.isnan(rmse)):
                    metrics["position_rmse_m"] = float(rmse)
            metrics["coverage_ratio"] = float(coverage_ratio(df, noise_truth))
            pred_b = _flight_boundaries(df)
            true_b = noise_truth.true_boundaries
            prf = segmentation_prf1(pred_b, true_b, tolerance_s=BOUNDARY_TOLERANCE_S)
            metrics.update(prf)
            berr = boundary_time_error_s(pred_b, true_b, tolerance_s=BOUNDARY_TOLERANCE_S)
            if berr is not None:
                metrics["boundary_err_s"] = float(berr)
            if not from_proxy and TRUTH_ID in df.columns:
                flagged_mask = _mask_from_events(df, deviations)
                dprf = deviation_prf1(flagged_mask, df[TRUTH_ID], noise_truth)
                metrics.update(dprf)
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
    # Retain the pre-inject-noise projection as truth_df when the caller
    # does not pass one explicitly. On synthetic bronze this frame still
    # carries synthetic.add_noise artifacts (gaps / dups / small jumps),
    # so RMSE will understate relative to a true clean parquet, but the
    # ADR-mandated __truth_id join still works and the full metric set
    # populates on the --truth-less code path (run_pipeline.py, pytest
    # test_run_experiment_time_gap_end_to_end).
    if truth is None and source == "synthetic" and TRUTH_ID in bronze.columns:
        projected = project_raw(bronze, source=source)
        keep = [c for c in ("icao24", "time", "lat", "lon", "baroaltitude", TRUTH_ID)
                if c in projected.columns]
        truth = (
            projected[keep]
            .dropna(subset=[TRUTH_ID])
            .drop_duplicates(subset=[TRUTH_ID])
            .reset_index(drop=True)
        )
    silver, rejected, noise_truth = prepare_silver(
        bronze, noise, source, write_silver_dir=write_silver_dir,
    )
    return reconstruct_from_silver(
        silver, method, noise, source,
        truth=truth, rejected=rejected, rows_in=len(bronze),
        expected_step_s=expected_step_s, write_gold_dir=write_gold_dir,
        noise_truth=noise_truth,
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
