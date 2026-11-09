"""Trajectory and segmentation quality metrics.

All functions take plain pandas DataFrames and return scalar floats / dicts.
Keep them pure: no I/O, no side effects, no config lookups.
"""
from __future__ import annotations

from typing import Iterable

import numpy as np
import pandas as pd

from etl4.reconstruct.reference import _haversine_m

BOUNDARY_TOLERANCE_S = 60.0


def position_rmse_m(predicted: pd.DataFrame, truth: pd.DataFrame,
                    on: tuple = ("icao24", "time")) -> float:
    """Root-mean-square distance (metres) between predicted and truth positions,
    joined on (icao24, time)."""
    # Kalman smoothing writes smoothed_lat/smoothed_lon alongside the raw
    # lat/lon so the audit trail is preserved; score the smoothed track when
    # it is present, otherwise fall back to raw.
    if {"smoothed_lat", "smoothed_lon"}.issubset(predicted.columns):
        pred_cols = ["smoothed_lat", "smoothed_lon"]
        pred_use = predicted[list(on) + pred_cols].rename(
            columns={"smoothed_lat": "lat", "smoothed_lon": "lon"})
    else:
        pred_use = predicted[list(on) + ["lat", "lon"]]
    join = pred_use.merge(truth[list(on) + ["lat", "lon"]],
                          on=list(on), suffixes=("_p", "_t"))
    if join.empty:
        return float("nan")
    d = _haversine_m(join["lat_p"], join["lon_p"], join["lat_t"], join["lon_t"])
    return float(np.sqrt(np.mean(d ** 2)))


def segmentation_prf(predicted_boundaries: Iterable, truth_boundaries: Iterable,
                     tolerance_s: float = BOUNDARY_TOLERANCE_S) -> dict:
    """Precision / recall / F1 of predicted segmentation boundary timestamps.
    Each boundary is an (icao24, time) tuple; a scalar time is treated as a
    scoped-to-nobody boundary and will only match a scalar truth boundary.
    A boundary is a TP if its icao24 matches and its time is within ±tol."""
    def _key(b):
        return b if isinstance(b, tuple) else (None, b)

    predicted = sorted((_key(b) for b in predicted_boundaries), key=lambda x: x[1])
    truth     = sorted((_key(b) for b in truth_boundaries),     key=lambda x: x[1])
    tp = 0
    matched_truth = set()
    for p_icao, p_t in predicted:
        for i, (t_icao, t_t) in enumerate(truth):
            if i in matched_truth:
                continue
            if p_icao == t_icao and abs(p_t - t_t) <= tolerance_s:
                tp += 1
                matched_truth.add(i)
                break
    fp = len(predicted) - tp
    fn = len(truth) - tp
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall    = tp / (tp + fn) if (tp + fn) else 0.0
    f1        = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
    return {"precision": precision, "recall": recall, "f1": f1,
            "tp": tp, "fp": fp, "fn": fn}


def boundary_time_error_s(predicted_boundaries: Iterable,
                          truth_boundaries: Iterable) -> float:
    """Mean |t_predicted − t_true| for the greedy nearest-neighbour matching.
    Accepts scalar times or (icao24, time) tuples; the icao24 is ignored here."""
    def _t(b):
        return b[1] if isinstance(b, tuple) else b

    pred_ts  = [_t(b) for b in predicted_boundaries]
    truth_ts = [_t(b) for b in truth_boundaries]
    if not pred_ts or not truth_ts:
        return float("nan")
    truth_arr = np.sort(np.asarray(truth_ts, dtype=float))
    errors = [float(np.min(np.abs(truth_arr - p))) for p in pred_ts]
    return float(np.mean(errors))


def coverage_ratio(predicted: pd.DataFrame, truth: pd.DataFrame) -> float:
    """Reconstructed-track length / truth-track length (per flight, averaged)."""
    def track_len(df):
        if len(df) < 2:
            return 0.0
        d = _haversine_m(df["lat"].to_numpy()[:-1], df["lon"].to_numpy()[:-1],
                         df["lat"].to_numpy()[1:],  df["lon"].to_numpy()[1:])
        return float(np.nansum(d))

    truth_grp = truth.groupby("icao24")
    pred_grp  = predicted.groupby("icao24")
    ratios = []
    for icao, tdf in truth_grp:
        if icao in pred_grp.groups:
            tl = track_len(tdf)
            pl = track_len(pred_grp.get_group(icao))
            if tl > 0:
                ratios.append(pl / tl)
    return float(np.mean(ratios)) if ratios else float("nan")


def continuity_ratio(df: pd.DataFrame, expected_step_s: float) -> float:
    """1 − (unexpected gaps) / (expected points − 1), per flight averaged.
    An unexpected gap is dt > 1.5 × expected_step_s."""
    if df.empty or "flight_id" not in df.columns:
        return float("nan")
    ratios = []
    for _, g in df.groupby("flight_id", sort=False):
        if len(g) < 2:
            continue
        dt = g["time"].diff().dropna()
        bad = (dt > 1.5 * expected_step_s).sum()
        ratios.append(1.0 - bad / max(len(g) - 1, 1))
    return float(np.mean(ratios)) if ratios else float("nan")


def deviation_prf(predicted_events: pd.DataFrame,
                  truth_events: pd.DataFrame,
                  tolerance_s: float = BOUNDARY_TOLERANCE_S) -> dict:
    """P/R/F1 of deviation events, matched by overlapping time windows."""
    if truth_events.empty and predicted_events.empty:
        return {"precision": 1.0, "recall": 1.0, "f1": 1.0,
                "tp": 0, "fp": 0, "fn": 0}
    tp = 0
    matched_truth = set()
    for _, p in predicted_events.iterrows():
        for ti, t in truth_events.iterrows():
            if ti in matched_truth:
                continue
            overlap = (p["start_time"] <= t["end_time"] + tolerance_s and
                       p["end_time"]   >= t["start_time"] - tolerance_s and
                       p["icao24"] == t["icao24"])
            if overlap:
                tp += 1
                matched_truth.add(ti)
                break
    fp = len(predicted_events) - tp
    fn = len(truth_events)     - tp
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall    = tp / (tp + fn) if (tp + fn) else 0.0
    f1        = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
    return {"precision": precision, "recall": recall, "f1": f1,
            "tp": tp, "fp": fp, "fn": fn}
