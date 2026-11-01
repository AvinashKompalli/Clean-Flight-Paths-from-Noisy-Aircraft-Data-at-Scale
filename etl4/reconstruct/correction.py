"""Candidate path correction.

Replaces noisy observations (large Kalman residual) with the filter's
smoothed position, and linearly interpolates small gaps.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

GAP_INTERP_MAX_S = 120.0


def correct_trajectory(df: pd.DataFrame, residual_sigma: float = 3.0) -> pd.DataFrame:
    """Expects columns: lat, lon, smoothed_lat, smoothed_lon, kalman_residual, dt.
    Returns df with corrected_lat, corrected_lon, corrected flag."""
    if df.empty:
        for c in ("corrected_lat", "corrected_lon", "corrected"):
            df[c] = pd.NA
        return df

    out = df.copy()
    out["corrected_lat"] = out["lat"].astype(float)
    out["corrected_lon"] = out["lon"].astype(float)
    out["corrected"]     = False

    if "kalman_residual" in out.columns and "smoothed_lat" in out.columns:
        res = out["kalman_residual"].fillna(0.0)
        threshold = residual_sigma * res.std() if res.std() > 0 else np.inf
        bad = res > threshold
        out.loc[bad, "corrected_lat"] = out.loc[bad, "smoothed_lat"]
        out.loc[bad, "corrected_lon"] = out.loc[bad, "smoothed_lon"]
        out.loc[bad, "corrected"] = True

    # linear interpolation of short gaps per flight
    if "flight_id" in out.columns and "dt" in out.columns:
        for fid, g in out.groupby("flight_id", sort=False):
            short_gap = g["dt"].between(0.001, GAP_INTERP_MAX_S)
            if short_gap.any():
                lat_i = g["corrected_lat"].interpolate(method="linear", limit_area="inside")
                lon_i = g["corrected_lon"].interpolate(method="linear", limit_area="inside")
                out.loc[g.index, "corrected_lat"] = lat_i
                out.loc[g.index, "corrected_lon"] = lon_i

    return out
