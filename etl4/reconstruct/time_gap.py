"""Method A: Time-gap baseline.

A new flight begins whenever the time gap between consecutive observations
for the same aircraft exceeds GAP_THRESHOLD_S (default 30 minutes).
"""
from __future__ import annotations

import pandas as pd

GAP_THRESHOLD_S = 30 * 60   # 30 minutes


def segment_time_gap(df: pd.DataFrame, gap_s: float = GAP_THRESHOLD_S) -> pd.DataFrame:
    """Add a `flight_id` column assigning each row to a flight segment.
    Expects df already event-time ordered."""
    if df.empty:
        df["flight_id"] = pd.Series(dtype="string")
        return df

    dt = df.groupby("icao24", sort=False)["time"].diff()
    new_flight = ((dt > gap_s) | dt.isna()).fillna(True).astype(bool)
    seq = new_flight.groupby(df["icao24"]).cumsum().fillna(0).astype("int64")
    df = df.copy()
    df["flight_id"] = df["icao24"].astype(str) + "_" + seq.astype(str)
    return df
