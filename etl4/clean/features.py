"""Event-time ordering and kinematic feature extraction.

Features added per record (grouped by icao24, in time order):
  dt         seconds since previous observation for this aircraft
  dist_m     haversine distance from previous observation
  speed_est  position-derived speed  (dist_m / dt)
  accel      change in broadcast velocity per second
  turn_rate  change in heading per second, wrapped to +/-180 deg
  alt_rate   change in baroaltitude per second
"""
from __future__ import annotations

import numpy as np
import pandas as pd

R_EARTH_M = 6_371_000.0


def event_time_order(df: pd.DataFrame) -> pd.DataFrame:
    """Return df sorted by (icao24, time) with monotonic index."""
    return (
        df.sort_values(["icao24", "time"], kind="stable")
          .reset_index(drop=True)
    )


def _haversine_m(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(np.radians, (lat1, lon1, lat2, lon2))
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    return 2 * R_EARTH_M * np.arcsin(np.sqrt(a))


def _wrap_deg(x):
    """Wrap heading differences into [-180, 180]."""
    return (x + 180.0) % 360.0 - 180.0


def kinematic_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add dt, dist_m, speed_est, accel, turn_rate, alt_rate columns.
    Assumes df is already event-time ordered (per-aircraft)."""
    if df.empty:
        for c in ("dt", "dist_m", "speed_est", "accel", "turn_rate", "alt_rate"):
            df[c] = np.nan
        return df

    g = df.groupby("icao24", sort=False)
    dt   = g["time"].diff()
    df["dt"] = dt

    prev_lat = g["lat"].shift(1)
    prev_lon = g["lon"].shift(1)
    df["dist_m"] = _haversine_m(prev_lat, prev_lon, df["lat"], df["lon"])

    with np.errstate(divide="ignore", invalid="ignore"):
        df["speed_est"] = np.where(dt > 0, df["dist_m"] / dt, np.nan)
        df["accel"]     = np.where(dt > 0, g["velocity"].diff() / dt, np.nan)
        heading_diff    = _wrap_deg(g["heading"].diff())
        df["turn_rate"] = np.where(dt > 0, heading_diff / dt, np.nan)
        df["alt_rate"]  = np.where(dt > 0, g["baroaltitude"].diff() / dt, np.nan)

    return df
