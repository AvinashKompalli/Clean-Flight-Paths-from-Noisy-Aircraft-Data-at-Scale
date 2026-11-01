"""Detect significant deviations from a reference path.

Emits one row per deviation event (contiguous run of flagged observations):
  (icao24, flight_id, start_time, end_time, type, max_distance, peak_residual)
"""
from __future__ import annotations

import pandas as pd

from .reference import build_reference_for_flight, cross_track_distance_m

CROSS_TRACK_FLAG_M      = 5_000.0   # 5 km
ALTITUDE_FLAG_M         = 300.0
MIN_EVENT_DURATION_S    = 60.0


def detect_deviations(df: pd.DataFrame, use_kalman_residual: bool = True) -> pd.DataFrame:
    """Expects df to carry `flight_id`. If kalman_residual column exists and
    `use_kalman_residual` is True, residual-based events are also emitted."""
    if df.empty or "flight_id" not in df.columns:
        return pd.DataFrame(columns=[
            "icao24", "flight_id", "start_time", "end_time",
            "type", "max_distance", "peak_residual",
        ])

    events = []
    for fid, g in df.groupby("flight_id", sort=False):
        ref = build_reference_for_flight(g)
        if ref.empty:
            continue
        # Vectorised cross-track for the whole flight group in one call
        xtd_arr = cross_track_distance_m(
            g["lat"].to_numpy(dtype=float),
            g["lon"].to_numpy(dtype=float),
            ref["lat"].to_numpy(),
            ref["lon"].to_numpy(),
        )
        xtd = pd.Series(xtd_arr, index=g.index)
        events += _runs(g, (xtd > CROSS_TRACK_FLAG_M).to_numpy(),
                        type_="cross_track", measure=xtd)

        if use_kalman_residual and "kalman_residual" in g.columns:
            res = g["kalman_residual"].fillna(0.0)
            threshold = 3 * res.std() if res.std() > 0 else 100.0
            events += _runs(g, (res > threshold).to_numpy(),
                            type_="residual", measure=res)

    cols = ["icao24", "flight_id", "start_time", "end_time",
            "type", "max_distance", "peak_residual"]
    return pd.DataFrame(events, columns=cols) if events else pd.DataFrame(columns=cols)


def _runs(flight_df: pd.DataFrame, flag_mask, type_: str, measure) -> list:
    """Collapse a boolean mask over flight_df rows into contiguous events."""
    if not flag_mask.any():
        return []
    runs = []
    in_run = False
    start_idx = 0
    for i, f in enumerate(flag_mask):
        if f and not in_run:
            start_idx = i
            in_run = True
        elif not f and in_run:
            runs.append((start_idx, i - 1))
            in_run = False
    if in_run:
        runs.append((start_idx, len(flag_mask) - 1))

    out = []
    for a, b in runs:
        slc = flight_df.iloc[a:b + 1]
        if slc["time"].iloc[-1] - slc["time"].iloc[0] < MIN_EVENT_DURATION_S:
            continue
        peak = float(measure.iloc[a:b + 1].max())
        out.append({
            "icao24":        slc["icao24"].iloc[0],
            "flight_id":     slc["flight_id"].iloc[0],
            "start_time":    int(slc["time"].iloc[0]),
            "end_time":      int(slc["time"].iloc[-1]),
            "type":          type_,
            "max_distance":  peak if type_ == "cross_track" else float("nan"),
            "peak_residual": peak if type_ == "residual"    else float("nan"),
        })
    return out
