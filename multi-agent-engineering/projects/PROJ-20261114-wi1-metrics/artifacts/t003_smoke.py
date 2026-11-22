"""T-003 handcrafted smoke tests.

Exercises the acceptance criteria 3 and 4 for the four new metric
functions. Prints a single PASS / FAIL line per criterion; exits
non-zero if anything fails. Standalone -- does not touch Postgres or
the pipeline runner.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from etl4.bench.noise import NoiseTruth
from etl4.evaluate.metrics import (
    boundary_time_error_s,
    coverage_ratio,
    deviation_prf1,
    position_rmse_m,
    segmentation_prf1,
)

TRUTH_ID = "__truth_id"


def _check(cond: bool, label: str, detail: str = "") -> int:
    tag = "PASS" if cond else "FAIL"
    print(f"[{tag}] {label}" + (f" -- {detail}" if detail else ""), flush=True)
    return 0 if cond else 1


def segmentation_identical() -> int:
    r = segmentation_prf1(
        [("A", 100), ("B", 200)],
        [("A", 100), ("B", 200)],
        tolerance_s=5.0,
    )
    return _check(
        r["seg_precision"] == 1.0 and r["seg_recall"] == 1.0 and r["seg_f1"] == 1.0,
        "segmentation_prf1 identical lists -> P=R=F1=1.0",
        f"got P={r['seg_precision']:.3f} R={r['seg_recall']:.3f} F1={r['seg_f1']:.3f}",
    )


def segmentation_disjoint() -> int:
    r = segmentation_prf1(
        [("A", 100), ("A", 200)],
        [("Z", 999), ("Z", 1500)],
        tolerance_s=5.0,
    )
    return _check(
        r["seg_precision"] == 0.0 and r["seg_recall"] == 0.0 and r["seg_f1"] == 0.0,
        "segmentation_prf1 disjoint -> P=R=F1=0.0",
        f"got P={r['seg_precision']:.3f} R={r['seg_recall']:.3f} F1={r['seg_f1']:.3f}",
    )


def segmentation_half_precision() -> int:
    # 4 predicted, 2 of them are within tol=5 of a truth boundary,
    # 2 of them are >> 5 off from any truth. precision = 2/4 = 0.5.
    r = segmentation_prf1(
        predicted_boundaries=[("A", 100), ("A", 200), ("A", 500), ("A", 600)],
        true_boundaries=[("A", 100), ("A", 200), ("A", 300), ("A", 400)],
        tolerance_s=5.0,
    )
    return _check(
        r["seg_precision"] == 0.5,
        "segmentation_prf1 half-match predicted -> precision=0.5",
        f"got P={r['seg_precision']:.3f} R={r['seg_recall']:.3f} F1={r['seg_f1']:.3f}",
    )


def position_rmse_zero_identical() -> int:
    truth_df = pd.DataFrame({
        TRUTH_ID: [0, 1, 2],
        "lat":    [13.0, 13.001, 13.002],
        "lon":    [77.0, 77.001, 77.002],
        "baroaltitude": [10000.0, 10000.0, 10000.0],
    })
    pred = truth_df.copy()
    truth = NoiseTruth(
        profile="N0", seed=42, original_row_count=3,
        original_ids=frozenset({0, 1, 2}),
        original_coords=pd.DataFrame(columns=[TRUTH_ID, "lat", "lon", "baroaltitude"]),
        dropped_ids=frozenset(), duplicated_ids=frozenset(),
        shuffled_ids=frozenset(), jumped_ids=frozenset(),
        true_boundaries=[],
    )
    rmse = position_rmse_m(pred, truth, truth_df)
    ok = rmse is not None and abs(rmse) < 1e-6
    return _check(ok, "position_rmse_m identical frames -> 0.0",
                  f"got {rmse}")


def position_rmse_positive_on_offset() -> int:
    truth_df = pd.DataFrame({
        TRUTH_ID: [0, 1, 2],
        "lat":    [13.0, 13.001, 13.002],
        "lon":    [77.0, 77.001, 77.002],
        "baroaltitude": [10000.0, 10000.0, 10000.0],
    })
    pred = truth_df.copy()
    pred["lat"] = pred["lat"] + 1.0  # 1 degree lat ~ 111 km
    truth = NoiseTruth(
        profile="N0", seed=42, original_row_count=3,
        original_ids=frozenset({0, 1, 2}),
        original_coords=pd.DataFrame(columns=[TRUTH_ID, "lat", "lon", "baroaltitude"]),
        dropped_ids=frozenset(), duplicated_ids=frozenset(),
        shuffled_ids=frozenset(), jumped_ids=frozenset(),
        true_boundaries=[],
    )
    rmse = position_rmse_m(pred, truth, truth_df)
    ok = rmse is not None and rmse > 100_000.0  # order of 111 km
    return _check(ok, "position_rmse_m +1 deg lat offset -> positive (~111km)",
                  f"got {rmse:.1f}" if rmse is not None else "got None")


def boundary_time_error_contract() -> int:
    err_match = boundary_time_error_s(
        [("A", 102), ("B", 207)],
        [("A", 100), ("B", 200)],
        tolerance_s=10.0,
    )
    err_none = boundary_time_error_s(
        [("A", 102)], [("Z", 999)], tolerance_s=10.0,
    )
    ok = (
        err_match is not None
        and abs(err_match - (2.0 + 7.0) / 2.0) < 1e-6
        and err_none is None
    )
    return _check(
        ok,
        "boundary_time_error_s matched mean + None on no match",
        f"matched={err_match} nomatch={err_none}",
    )


def coverage_ratio_bounds() -> int:
    truth = NoiseTruth(
        profile="N1", seed=42, original_row_count=10,
        original_ids=frozenset(range(10)),
        original_coords=pd.DataFrame(columns=[TRUTH_ID, "lat", "lon", "baroaltitude"]),
        dropped_ids=frozenset({3, 7}),
        duplicated_ids=frozenset(), shuffled_ids=frozenset(), jumped_ids=frozenset(),
        true_boundaries=[],
    )
    pred = pd.DataFrame({TRUTH_ID: [0, 1, 2, 4, 5, 6, 8, 9]})
    ratio = coverage_ratio(pred, truth)
    ok = abs(ratio - 0.8) < 1e-9
    return _check(ok, "coverage_ratio = 8/10 = 0.8", f"got {ratio}")


def deviation_prf1_shapes() -> int:
    truth = NoiseTruth(
        profile="N1", seed=42, original_row_count=5,
        original_ids=frozenset(range(5)),
        original_coords=pd.DataFrame(columns=[TRUTH_ID, "lat", "lon", "baroaltitude"]),
        dropped_ids=frozenset(), duplicated_ids=frozenset(),
        shuffled_ids=frozenset(),
        jumped_ids=frozenset({1, 3}),
        true_boundaries=[],
    )
    # flag ids 1 (TP) and 4 (FP). id 3 is jumped but not flagged (FN).
    mask = pd.Series([False, True, False, False, True])
    ids  = pd.Series([0, 1, 2, 3, 4])
    r = deviation_prf1(mask, ids, truth)
    tp, fp, fn = 1, 1, 1
    p = tp / (tp + fp)
    rcl = tp / (tp + fn)
    f1 = 2 * p * rcl / (p + rcl)
    ok = (
        abs(r["dev_precision"] - p) < 1e-9
        and abs(r["dev_recall"] - rcl) < 1e-9
        and abs(r["dev_f1"] - f1) < 1e-9
    )
    return _check(ok, "deviation_prf1 TP=1 FP=1 FN=1 -> expected P/R/F1",
                  f"got {r}")


def main() -> int:
    rc = 0
    rc |= segmentation_identical()
    rc |= segmentation_disjoint()
    rc |= segmentation_half_precision()
    rc |= position_rmse_zero_identical()
    rc |= position_rmse_positive_on_offset()
    rc |= boundary_time_error_contract()
    rc |= coverage_ratio_bounds()
    rc |= deviation_prf1_shapes()
    print("SMOKE OK" if rc == 0 else "SMOKE FAILED", flush=True)
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
