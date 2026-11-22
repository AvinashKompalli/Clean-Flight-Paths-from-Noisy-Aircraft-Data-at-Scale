"""Export one day of OpenSky state vectors to bronze Parquet via Trino.

Rules: filter every query by hour; export each day once (resume by skipping
hours whose Parquet already exists). Uses Trino.history, not Rebuild.
"""
import argparse
import os
from pathlib import Path

import pandas as pd
import yaml
from dotenv import load_dotenv

load_dotenv()
ROOT = Path(__file__).resolve().parents[1]
CFG = yaml.safe_load((ROOT / "config" / "config.yaml").read_text())

# Columns in the canonical schema (see ingest/synthetic.py).
PIPELINE_COLUMNS = [
    "time", "icao24", "lat", "lon", "velocity", "heading", "vertrate",
    "callsign", "onground", "alert", "spi", "squawk", "baroaltitude",
    "geoaltitude", "lastposupdate", "lastcontact", "hour",
]


def fetch_hour(trino, start, stop, bounds):
    """One hour from Trino, with columns renamed to the pipeline schema."""
    df = trino.history(start=start, stop=stop, bounds=bounds)
    if df is None or df.empty:
        return pd.DataFrame(columns=PIPELINE_COLUMNS)
    # pyopensky returns `latitude`/`longitude`/`baroaltitude`; normalise.
    rename = {"latitude": "lat", "longitude": "lon"}
    df = df.rename(columns=rename)
    missing = [c for c in PIPELINE_COLUMNS if c not in df.columns and c != "hour"]
    for c in missing:
        df[c] = pd.NA
    return df


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True, help="YYYY-MM-DD")
    p.add_argument("--hours", type=int, default=24)
    p.add_argument("--out", default=str(ROOT / CFG["paths"]["bronze_raw_states"]))
    a = p.parse_args()

    if os.getenv("SOURCE", "synthetic") != "trino":
        raise SystemExit("Set SOURCE=trino in .env first. Needs approved access.")

    from pyopensky.trino import Trino
    trino = Trino()

    bounds = CFG["ingest"]["bounds"]
    for h in range(a.hours):
        out = Path(a.out) / f"date={a.date}" / f"hour={h}" / "part-0.parquet"
        if out.exists():
            print(f"skip hour {h}: exists")
            continue
        start = pd.Timestamp(a.date, tz="UTC") + pd.Timedelta(hours=h)
        stop = start + pd.Timedelta(hours=1)

        df = fetch_hour(trino, start, stop, bounds)
        rows = len(df)
        unique = df["icao24"].nunique() if rows else 0
        print(f"hour {h:2d}: rows={rows:>10,}  icao24={unique:>6,}")

        if rows:
            df["hour"] = int(start.timestamp())
            out.parent.mkdir(parents=True, exist_ok=True)
            df[PIPELINE_COLUMNS].to_parquet(out, index=False)


if __name__ == "__main__":
    main()
