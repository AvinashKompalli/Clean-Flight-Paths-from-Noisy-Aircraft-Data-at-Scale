"""Daily incremental ingest - one OpenSky REST poll per run.

Writes one parquet file per invocation under
`data/bronze/raw_states/date=YYYY-MM-DD/hour=HH/part-<unix>.parquet`,
matching the schema the vault builder already consumes. Each run spends
exactly one OAuth credit (of your 4000/day quota) and captures a global
snapshot of every airborne aircraft at that moment (~10-25k records).

The resulting bronze partitions accrete over time; the nightly vault
rebuild picks them up and the marts + Postgres sink refresh.

Usage:
  python -m etl4.ingest.daily_batch                     # now, UTC
  python -m etl4.ingest.daily_batch --date 2026-11-12   # force partition
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

from etl4.ingest.live_producer import OPENSKY_URL, _TokenCache, fetch_states
from etl4.orchestration.ingest_metrics import ingest_run

ROOT = Path(__file__).resolve().parents[2]
BRONZE = ROOT / "data" / "bronze" / "raw_states"

# OpenSky REST field -> bronze schema column
_REST_TO_BRONZE = {
    "icao24":        "icao24",
    "callsign":      "callsign",
    "longitude":     "lon",
    "latitude":      "lat",
    "baro_altitude": "baroaltitude",
    "geo_altitude":  "geoaltitude",
    "on_ground":     "onground",
    "velocity":      "velocity",
    "true_track":    "heading",
    "vertical_rate": "vertrate",
    "last_contact":  "lastcontact",
    "time_position": "lastposupdate",
    "squawk":        "squawk",
    "spi":           "spi",
}

_BRONZE_COLS = ["time", "icao24", "lat", "lon", "velocity", "heading",
                "vertrate", "callsign", "onground", "alert", "spi", "squawk",
                "baroaltitude", "geoaltitude", "lastposupdate", "lastcontact",
                "hour"]


def poll_once(session) -> pd.DataFrame:
    """One REST poll. Returns a DataFrame in the bronze schema."""
    load_dotenv(ROOT / "etl4" / ".env")
    cid, sec = os.getenv("OPENSKY_CLIENT_ID"), os.getenv("OPENSKY_CLIENT_SECRET")
    tc = _TokenCache(cid, sec) if cid and sec else None
    snapshot_ts, rows = fetch_states(session, token_cache=tc)
    if not rows:
        return pd.DataFrame(columns=_BRONZE_COLS)

    raw = pd.DataFrame(rows, columns=[
        "icao24", "callsign", "origin_country", "time_position", "last_contact",
        "longitude", "latitude", "baro_altitude", "on_ground", "velocity",
        "true_track", "vertical_rate", "sensors", "geo_altitude", "squawk",
        "spi", "position_source",
    ])
    df = raw.rename(columns=_REST_TO_BRONZE).copy()
    df["icao24"] = df["icao24"].astype("string").str.lower()
    df["callsign"] = df["callsign"].astype("string").str.strip()
    df["time"] = snapshot_ts
    df["alert"] = pd.NA
    df["hour"] = datetime.fromtimestamp(snapshot_ts, timezone.utc).hour
    return df[_BRONZE_COLS]


def write_partition(df: pd.DataFrame, date: str, snapshot_ts: int) -> tuple[Path, int]:
    hour = datetime.fromtimestamp(snapshot_ts, timezone.utc).hour
    out = BRONZE / f"date={date}" / f"hour={hour}" / f"part-{snapshot_ts}.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, index=False)
    return out, out.stat().st_size


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--date", default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                   help="partition date (default: today UTC)")
    a = p.parse_args()

    with ingest_run("daily_batch", "batch",
                    extras={"date": a.date, "credits_used": 1}) as rec:
        with requests.Session() as session:
            df = poll_once(session)
        if df.empty:
            print("[daily_batch] empty poll (OpenSky returned no states)", flush=True)
            rec["rows_out"] = 0
            rec["bytes_out"] = 0
            return
        snapshot_ts = int(df["time"].iloc[0])
        path, nbytes = write_partition(df, a.date, snapshot_ts)
        rec["rows_out"]  = len(df)
        rec["bytes_out"] = nbytes
        print(f"[daily_batch] wrote {path}  rows={len(df):,}  bytes={nbytes:,}",
              flush=True)


if __name__ == "__main__":
    main()
