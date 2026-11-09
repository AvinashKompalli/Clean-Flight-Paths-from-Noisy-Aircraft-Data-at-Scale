"""Build the Data Vault 2.0 raw vault from bronze.

Reads:
  data/bronze/flightlists/<version>/*.parquet       Zenodo flightlists
  data/bronze/raw_states/date=*/hour=*/part.parquet S3 state vectors
  data/bronze/synthetic/date=*/hour=*/part.parquet  Synthetic (opt-in)

Writes (insert-only; duplicate business keys are deduped):
  data/silver/vault/hub_aircraft/
  data/silver/vault/hub_airport/
  data/silver/vault/hub_flight/
  data/silver/vault/link_flight_aircraft/
  data/silver/vault/link_flight_route/
  data/silver/vault/sat_aircraft_identity/
  data/silver/vault/sat_flight_summary/
  data/silver/vault/sat_state_observation/

Schema: see etl4/docs/data_vault_design.md.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from etl4.vault.schema import hash_series

ROOT = Path(__file__).resolve().parents[2]
VAULT_DIR         = ROOT / "data" / "silver" / "vault"
FLIGHTLIST_BRONZE = ROOT / "data" / "bronze" / "flightlists"
STATES_BRONZE     = ROOT / "data" / "bronze" / "raw_states"


def _load_parquet_tree(path: Path) -> pd.DataFrame:
    """Read every parquet under a directory and concat (§17.3 pattern)."""
    if path.is_file():
        return pd.read_parquet(path)
    files = sorted(path.rglob("*.parquet"))
    if not files:
        return pd.DataFrame()
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


def _write(df: pd.DataFrame, name: str, out_dir: Path) -> int:
    """Write one vault table as a single parquet. Returns row count."""
    dst = out_dir / name / "part.parquet"
    dst.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(dst, index=False)
    return len(df)


# --------------------------------------------------------------------------
# Hubs
# --------------------------------------------------------------------------
def build_hub_aircraft(flightlist: pd.DataFrame, states: pd.DataFrame,
                       load_dt: datetime) -> pd.DataFrame:
    sources = []
    if not flightlist.empty:
        sources.append(
            pd.DataFrame({"icao24": flightlist["icao24"], "rec_src": "opensky_zenodo"})
        )
    if not states.empty:
        sources.append(
            pd.DataFrame({"icao24": states["icao24"], "rec_src": "opensky_s3"})
        )
    if not sources:
        return pd.DataFrame(columns=["hk_aircraft", "icao24", "load_dt", "rec_src"])
    stacked = pd.concat(sources, ignore_index=True)
    stacked = stacked.dropna(subset=["icao24"])
    stacked["icao24"] = stacked["icao24"].astype("string").str.strip().str.upper()
    # First-seen rec_src wins on duplicates (keeps whichever source observed it first in the stack).
    stacked = stacked.drop_duplicates(subset=["icao24"], keep="first")
    stacked["hk_aircraft"] = hash_series(stacked["icao24"])
    stacked["load_dt"] = load_dt
    return stacked[["hk_aircraft", "icao24", "load_dt", "rec_src"]]


def build_hub_airport(flightlist: pd.DataFrame, load_dt: datetime) -> pd.DataFrame:
    if flightlist.empty:
        return pd.DataFrame(columns=["hk_airport", "icao_code", "load_dt", "rec_src"])
    codes = pd.concat([flightlist["origin"], flightlist["destination"]], ignore_index=True)
    codes = codes.dropna()
    codes = codes.astype("string").str.strip().str.upper()
    codes = codes[codes != ""].drop_duplicates().reset_index(drop=True)
    out = pd.DataFrame({"icao_code": codes})
    out["hk_airport"] = hash_series(out["icao_code"])
    out["load_dt"] = load_dt
    out["rec_src"] = "opensky_zenodo"
    return out[["hk_airport", "icao_code", "load_dt", "rec_src"]]


def build_hub_flight(flightlist: pd.DataFrame, load_dt: datetime) -> pd.DataFrame:
    if flightlist.empty:
        return pd.DataFrame(columns=["hk_flight", "callsign", "day", "load_dt", "rec_src"])
    df = flightlist[["callsign", "day"]].copy()
    df = df.dropna(subset=["callsign", "day"])
    df["callsign"] = df["callsign"].astype("string").str.strip().str.upper()
    df["day"] = pd.to_datetime(df["day"], utc=True).dt.date
    df = df.drop_duplicates(subset=["callsign", "day"], keep="first").reset_index(drop=True)
    df["hk_flight"] = hash_series(df["callsign"], df["day"].astype(str))
    df["load_dt"] = load_dt
    df["rec_src"] = "opensky_zenodo"
    return df[["hk_flight", "callsign", "day", "load_dt", "rec_src"]]


# --------------------------------------------------------------------------
# Links
# --------------------------------------------------------------------------
def build_link_flight_aircraft(flightlist: pd.DataFrame, load_dt: datetime) -> pd.DataFrame:
    if flightlist.empty:
        return pd.DataFrame(columns=["hk_flight_aircraft", "hk_flight", "hk_aircraft", "load_dt", "rec_src"])
    df = flightlist[["callsign", "day", "icao24"]].copy()
    df = df.dropna(subset=["callsign", "day", "icao24"])
    df["callsign"] = df["callsign"].astype("string").str.strip().str.upper()
    df["day"]      = pd.to_datetime(df["day"], utc=True).dt.date
    df["icao24"]   = df["icao24"].astype("string").str.strip().str.upper()
    df = df.drop_duplicates().reset_index(drop=True)
    df["hk_flight"]   = hash_series(df["callsign"], df["day"].astype(str))
    df["hk_aircraft"] = hash_series(df["icao24"])
    df["hk_flight_aircraft"] = hash_series(df["hk_flight"], df["hk_aircraft"])
    df["load_dt"] = load_dt
    df["rec_src"] = "opensky_zenodo"
    return df[["hk_flight_aircraft", "hk_flight", "hk_aircraft", "load_dt", "rec_src"]]


def build_link_flight_route(flightlist: pd.DataFrame, load_dt: datetime) -> pd.DataFrame:
    cols = ["hk_flight_route", "hk_flight", "hk_airport_origin",
            "hk_airport_destination", "load_dt", "rec_src"]
    if flightlist.empty:
        return pd.DataFrame(columns=cols)
    df = flightlist[["callsign", "day", "origin", "destination"]].copy()
    df = df.dropna(subset=["callsign", "day"])
    df["callsign"]    = df["callsign"].astype("string").str.strip().str.upper()
    df["day"]         = pd.to_datetime(df["day"], utc=True).dt.date
    df["origin"]      = df["origin"].astype("string").str.strip().str.upper().fillna("")
    df["destination"] = df["destination"].astype("string").str.strip().str.upper().fillna("")
    df = df.drop_duplicates().reset_index(drop=True)
    df["hk_flight"]              = hash_series(df["callsign"], df["day"].astype(str))
    df["hk_airport_origin"]      = hash_series(df["origin"])
    df["hk_airport_destination"] = hash_series(df["destination"])
    df["hk_flight_route"] = hash_series(
        df["hk_flight"], df["hk_airport_origin"], df["hk_airport_destination"],
    )
    df["load_dt"] = load_dt
    df["rec_src"] = "opensky_zenodo"
    # null-out the airport hashes where the source code was missing, so FKs are honest.
    df.loc[df["origin"]      == "", "hk_airport_origin"]      = pd.NA
    df.loc[df["destination"] == "", "hk_airport_destination"] = pd.NA
    return df[cols]


# --------------------------------------------------------------------------
# Satellites
# --------------------------------------------------------------------------
def build_sat_aircraft_identity(flightlist: pd.DataFrame, load_dt: datetime) -> pd.DataFrame:
    cols = ["hk_aircraft", "load_dt", "rec_src", "registration", "typecode"]
    if flightlist.empty:
        return pd.DataFrame(columns=cols)
    df = flightlist[["icao24", "registration", "typecode"]].copy()
    df = df.dropna(subset=["icao24"])
    df["icao24"]       = df["icao24"].astype("string").str.strip().str.upper()
    df["registration"] = df["registration"].astype("string").fillna("")
    df["typecode"]     = df["typecode"].astype("string").fillna("")
    # One row per (icao24, registration, typecode); prefer non-empty attributes.
    df["_score"] = (df["registration"] != "").astype(int) + (df["typecode"] != "").astype(int)
    df = (df.sort_values("_score", ascending=False)
            .drop_duplicates(subset=["icao24"], keep="first")
            .drop(columns="_score")
            .reset_index(drop=True))
    df["hk_aircraft"] = hash_series(df["icao24"])
    df["load_dt"] = load_dt
    df["rec_src"] = "opensky_zenodo"
    df.loc[df["registration"] == "", "registration"] = pd.NA
    df.loc[df["typecode"]     == "", "typecode"]     = pd.NA
    return df[cols]


def build_sat_flight_summary(flightlist: pd.DataFrame, load_dt: datetime) -> pd.DataFrame:
    cols = ["hk_flight", "load_dt", "rec_src", "firstseen", "lastseen",
            "duration_s", "latitude_1", "longitude_1", "altitude_1",
            "latitude_2", "longitude_2", "altitude_2"]
    if flightlist.empty:
        return pd.DataFrame(columns=cols)
    df = flightlist.copy()
    df = df.dropna(subset=["callsign", "day"])
    df["callsign"] = df["callsign"].astype("string").str.strip().str.upper()
    day = pd.to_datetime(df["day"], utc=True).dt.date
    firstseen = pd.to_datetime(df["firstseen"], utc=True)
    lastseen  = pd.to_datetime(df["lastseen"],  utc=True)
    df["hk_flight"] = hash_series(df["callsign"], day.astype(str))
    df["firstseen"] = firstseen
    df["lastseen"]  = lastseen
    df["duration_s"] = (lastseen - firstseen).dt.total_seconds()
    df["load_dt"] = load_dt
    df["rec_src"] = "opensky_zenodo"
    # One row per flight; earlier rows may have been dups of the same business key.
    df = df.drop_duplicates(subset=["hk_flight"], keep="first").reset_index(drop=True)
    return df[cols]


def build_sat_state_observation(states: pd.DataFrame, load_dt: datetime) -> pd.DataFrame:
    cols = ["hk_aircraft", "time", "load_dt", "rec_src",
            "lat", "lon", "baroaltitude", "velocity", "heading",
            "vertrate", "callsign", "onground"]
    if states.empty:
        return pd.DataFrame(columns=cols)
    df = states.copy()
    df = df.dropna(subset=["icao24", "time"])
    df["icao24"] = df["icao24"].astype("string").str.strip().str.upper()
    df["hk_aircraft"] = hash_series(df["icao24"])
    df["time"] = df["time"].astype("Int64")
    df["load_dt"] = load_dt
    df["rec_src"] = "opensky_s3"
    # Keep only PK + observation columns; drop duplicates on (hk_aircraft, time).
    df = df.drop_duplicates(subset=["hk_aircraft", "time"], keep="first")
    return df[cols].reset_index(drop=True)


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------
def build_raw_vault(flightlist_dir: Path = FLIGHTLIST_BRONZE,
                    states_dir: Path = STATES_BRONZE,
                    out_dir: Path = VAULT_DIR,
                    include_states: bool = True) -> dict:
    """Read bronze, write every raw-vault table. Returns row counts."""
    load_dt = datetime.now(timezone.utc)

    print("loading bronze ...")
    flightlist = _load_parquet_tree(flightlist_dir)
    print(f"  flightlist rows: {len(flightlist):,}")
    states = _load_parquet_tree(states_dir) if include_states else pd.DataFrame()
    print(f"  state-vector rows: {len(states):,}")

    counts = {}
    print("\nbuilding hubs ...")
    counts["hub_aircraft"]   = _write(build_hub_aircraft(flightlist, states, load_dt), "hub_aircraft", out_dir)
    counts["hub_airport"]    = _write(build_hub_airport(flightlist, load_dt), "hub_airport", out_dir)
    counts["hub_flight"]     = _write(build_hub_flight(flightlist, load_dt), "hub_flight", out_dir)
    print(f"  hub_aircraft={counts['hub_aircraft']:,}  "
          f"hub_airport={counts['hub_airport']:,}  "
          f"hub_flight={counts['hub_flight']:,}")

    print("\nbuilding links ...")
    counts["link_flight_aircraft"] = _write(build_link_flight_aircraft(flightlist, load_dt), "link_flight_aircraft", out_dir)
    counts["link_flight_route"]    = _write(build_link_flight_route(flightlist, load_dt),    "link_flight_route",    out_dir)
    print(f"  link_flight_aircraft={counts['link_flight_aircraft']:,}  "
          f"link_flight_route={counts['link_flight_route']:,}")

    print("\nbuilding satellites ...")
    counts["sat_aircraft_identity"] = _write(build_sat_aircraft_identity(flightlist, load_dt), "sat_aircraft_identity", out_dir)
    counts["sat_flight_summary"]    = _write(build_sat_flight_summary(flightlist, load_dt),    "sat_flight_summary",    out_dir)
    counts["sat_state_observation"] = _write(build_sat_state_observation(states, load_dt),     "sat_state_observation", out_dir)
    print(f"  sat_aircraft_identity={counts['sat_aircraft_identity']:,}  "
          f"sat_flight_summary={counts['sat_flight_summary']:,}  "
          f"sat_state_observation={counts['sat_state_observation']:,}")

    return counts


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--flightlist-dir", default=str(FLIGHTLIST_BRONZE))
    p.add_argument("--states-dir",     default=str(STATES_BRONZE))
    p.add_argument("--out",            default=str(VAULT_DIR))
    p.add_argument("--no-states",      action="store_true",
                   help="skip the state-vector satellite (fast dev loop)")
    a = p.parse_args()
    build_raw_vault(
        Path(a.flightlist_dir), Path(a.states_dir), Path(a.out),
        include_states=not a.no_states,
    )


if __name__ == "__main__":
    main()
