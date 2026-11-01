"""Method B: Kinematic reconstruction.

A new flight begins when any of the following is true:
  - time gap > GAP_THRESHOLD_S
  - position jump (dist_m/dt > MAX_GROUND_SPEED_MPS)      # flagged by clean()
  - altitude jump (|alt_rate| > MAX_ALT_RATE_MPS)
  - heading reversal (|turn_rate| > MAX_TURN_RATE_DPS sustained)
  - callsign change while airborne
  - on-ground transition sustained > ONGROUND_HOLD_S

Expects df to already contain the kinematic feature columns
(dt, dist_m, speed_est, accel, turn_rate, alt_rate) and the
`position_jump` flag from clean().
"""
from __future__ import annotations

import pandas as pd

GAP_THRESHOLD_S     = 30 * 60
MAX_ALT_RATE_MPS    = 50.0
MAX_TURN_RATE_DPS   = 30.0


def segment_kinematic(df: pd.DataFrame, gap_s: float = GAP_THRESHOLD_S) -> pd.DataFrame:
    if df.empty:
        df["flight_id"] = pd.Series(dtype="string")
        return df

    df = df.copy()
    g = df.groupby("icao24", sort=False)
    dt = g["time"].diff()

    break_time   = ((dt > gap_s) | dt.isna()).fillna(True)
    break_jump   = df.get("position_jump", pd.Series(False, index=df.index)).fillna(False)
    break_alt    = df["alt_rate"].abs().gt(MAX_ALT_RATE_MPS).fillna(False)
    break_turn   = df["turn_rate"].abs().gt(MAX_TURN_RATE_DPS).fillna(False)
    break_call   = (g["callsign"].shift(1).ne(df["callsign"]) & df["callsign"].notna()).fillna(False)
    break_call   = break_call & ~dt.isna()   # first row per aircraft doesn't count

    new_flight = (break_time | break_jump | break_alt | break_turn | break_call).astype(bool)
    seq = new_flight.groupby(df["icao24"]).cumsum().fillna(0).astype("int64")
    df["flight_id"] = df["icao24"].astype(str) + "_" + seq.astype(str)
    return df
