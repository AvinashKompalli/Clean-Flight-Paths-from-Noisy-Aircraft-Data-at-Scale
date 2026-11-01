"""Project raw ingest records to the pipeline schema, deduplicate, validate,
and range-check.

Inputs are intentionally heterogeneous — the ingest layer keeps everything
raw. project_raw() handles the naming/shape differences; clean() then does
the quality work.

project_raw()  : OpenSky REST / S3 CSV native fields -> pipeline COLUMNS
clean()        : (projected_df) -> (clean_df, rejected_df)
"""
from __future__ import annotations

import pandas as pd

# OpenSky REST field name  ->  pipeline COLUMNS name
_REST_TO_PIPELINE = {
    "snapshot_time":  "time",
    "icao24":         "icao24",
    "latitude":       "lat",
    "longitude":      "lon",
    "velocity":       "velocity",
    "true_track":     "heading",
    "vertical_rate":  "vertrate",
    "callsign":       "callsign",
    "on_ground":      "onground",
    "spi":            "spi",
    "squawk":         "squawk",
    "baro_altitude":  "baroaltitude",
    "geo_altitude":   "geoaltitude",
    "time_position":  "lastposupdate",
    "last_contact":   "lastcontact",
}

# OpenSky S3 CSV columns use pipeline names already (lat, lon, etc.) —
# only rename the few that differ.
_S3_TO_PIPELINE = {
    # Historical CSVs sometimes name the primary timestamp "time" already;
    # if an alternate name appears, map it here.
}


def project_raw(df: pd.DataFrame, source: str) -> pd.DataFrame:
    """Rename / add missing columns so df conforms to the pipeline schema.

    source ∈ {"rest", "s3", "synthetic"}. Fields the source does not provide
    are filled with pd.NA; `alert` defaults to False for REST (not exposed).
    """
    df = df.copy()
    if source == "rest":
        df = df.rename(columns=_REST_TO_PIPELINE)
        if "alert" not in df.columns:
            df["alert"] = False
    elif source == "s3":
        df = df.rename(columns=_S3_TO_PIPELINE)
    # synthetic already matches

    required = [
        "time", "icao24", "lat", "lon", "velocity", "heading", "vertrate",
        "callsign", "onground", "alert", "spi", "squawk", "baroaltitude",
        "geoaltitude", "lastposupdate", "lastcontact",
    ]
    for col in required:
        if col not in df.columns:
            df[col] = pd.NA
    if "hour" not in df.columns:
        df["hour"] = (df["time"].astype("Int64") // 3600) * 3600
    return df

# physically plausible bounds
MAX_GROUND_SPEED_MPS = 340.0     # Mach 1 at sea level; aircraft stay below this
MIN_BARO_ALT_M       = -500.0
MAX_BARO_ALT_M       = 15000.0

DEDUP_KEYS = ["icao24", "time", "lat", "lon"]


def clean(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    rejected = []

    # 1. drop rows with null icao24 or null position
    null_icao = df["icao24"].isna()
    null_pos  = df["lat"].isna() | df["lon"].isna()
    bad = df[null_icao | null_pos].assign(reason="null_key_or_position")
    rejected.append(bad)
    df = df[~(null_icao | null_pos)]

    # 2. range checks on lat/lon/alt
    bad_lat = ~df["lat"].between(-90, 90)
    bad_lon = ~df["lon"].between(-180, 180)
    bad_alt = df["baroaltitude"].notna() & (
        (df["baroaltitude"] < MIN_BARO_ALT_M) | (df["baroaltitude"] > MAX_BARO_ALT_M)
    )
    bad = df[bad_lat | bad_lon | bad_alt].assign(reason="out_of_range")
    rejected.append(bad)
    df = df[~(bad_lat | bad_lon | bad_alt)]

    # 3. normalise text columns
    for col in ("callsign", "squawk"):
        if col in df.columns:
            df[col] = (
                df[col].astype("string").str.strip().replace("", pd.NA)
            )

    # 4. drop exact duplicates
    before = len(df)
    df = df.drop_duplicates(subset=DEDUP_KEYS, keep="first")
    duplicates_dropped = before - len(df)

    # 5. flag position jumps (not dropped — Method B uses them as segmentation signal)
    df = _flag_position_jumps(df)

    rejected_df = pd.concat(rejected, ignore_index=True) if rejected else pd.DataFrame()
    df = df.reset_index(drop=True)
    df.attrs["duplicates_dropped"] = duplicates_dropped
    return df, rejected_df


def _flag_position_jumps(df: pd.DataFrame) -> pd.DataFrame:
    """Mark rows where straight-line distance to the previous point exceeds
    MAX_GROUND_SPEED_MPS × dt. Does NOT drop them — Method B needs them."""
    if df.empty:
        df["position_jump"] = False
        return df

    df = df.sort_values(["icao24", "time"]).copy()
    g = df.groupby("icao24", sort=False)
    dt   = g["time"].diff()
    dlat = g["lat"].diff()
    dlon = g["lon"].diff()
    # cheap flat-Earth distance in metres (sufficient for jump detection)
    import numpy as np
    mean_lat_rad = np.radians(df["lat"].to_numpy())
    dist_m = np.sqrt(
        (dlat.to_numpy() * 111_000.0) ** 2 +
        (dlon.to_numpy() * 111_000.0 * np.cos(mean_lat_rad)) ** 2
    )
    with np.errstate(divide="ignore", invalid="ignore"):
        implied = np.where(dt.to_numpy() > 0, dist_m / dt.to_numpy(), 0.0)
    df["position_jump"] = implied > MAX_GROUND_SPEED_MPS
    return df
