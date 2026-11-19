"""Build the Data Vault 2.0 raw vault from bronze.

Reads:
  data/bronze/flightlists/<version>/*.parquet       Zenodo flightlists
  data/bronze/raw_states/date=*/hour=*/part.parquet S3 state vectors
  data/bronze/synthetic/date=*/hour=*/part.parquet  Synthetic (opt-in)

Writes partitioned (one part.parquet per ym=YYYY-MM):
  data/silver/vault/hub_flight/ym=YYYY-MM/part.parquet
  data/silver/vault/link_flight_aircraft/ym=YYYY-MM/part.parquet
  data/silver/vault/link_flight_route/ym=YYYY-MM/part.parquet
  data/silver/vault/sat_flight_summary/ym=YYYY-MM/part.parquet
  data/silver/vault/sat_state_observation/ym=YYYY-MM/part.parquet

Writes non-partitioned (globally unique entities, compacted):
  data/silver/vault/hub_aircraft/part.parquet
  data/silver/vault/hub_airport/part.parquet
  data/silver/vault/sat_aircraft_identity/part.parquet

Incremental contract (ADR 2026-11-13, IMPR-01):
  * `watermark.get('vault', 'hub_flight')` returns the last fully-processed
    ym. Bronze files whose ym <= last_ym are skipped.
  * Each per-ym transaction commits the four partitioned writes, the
    small-hub compaction, and the watermark advance atomically.

Schema: see etl4/docs/data_vault_design.md.
"""
from __future__ import annotations

import argparse
import gc
import os
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import psycopg
import pyarrow as pa
import pyarrow.parquet as pq

from etl4.vault import vault_io, watermark
from etl4.vault.schema import hash_series

ROOT = Path(__file__).resolve().parents[2]
VAULT_DIR         = ROOT / "data" / "silver" / "vault"
FLIGHTLIST_BRONZE = ROOT / "data" / "bronze" / "flightlists"
STATES_BRONZE     = ROOT / "data" / "bronze" / "raw_states"

DSN_DEFAULT = os.getenv(
    "POSTGRES_DSN",
    "postgresql://etl4:change_me@localhost:5432/etl4",
)

# Four flightlist-derived tables land per-ym under ym=YYYY-MM/part.parquet.
# sat_state_observation is partitioned too but built from raw_states, not here
# (T-10 handles the state path in raw_vault.py/raw_vault_spark.py).
PARTITIONED_TABLES: tuple = (
    "hub_flight",
    "link_flight_aircraft",
    "link_flight_route",
    "sat_flight_summary",
)

NON_PARTITIONED_TABLES: tuple = (
    "hub_aircraft",
    "hub_airport",
    "sat_aircraft_identity",
)


def _load_parquet_tree(path: Path) -> pd.DataFrame:
    """Read every parquet under a directory and concat (§17.3 pattern)."""
    if path.is_file():
        return pd.read_parquet(path)
    files = sorted(path.rglob("*.parquet"))
    if not files:
        return pd.DataFrame()
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


def _atomic_write_monolith(df: pd.DataFrame, name: str, out_dir: Path) -> int:
    """Write a single-file (non-partitioned) vault table atomically.

    tmp + os.replace keeps the small-hub compaction crash-safe: if the
    process dies mid-write, the previous monolith is still the authority
    on next run. The non-partitioned tables are the only accumulated
    history for hub_aircraft / hub_airport / sat_aircraft_identity, so a
    torn write would permanently drop entities.
    """
    dst_dir = out_dir / name
    dst_dir.mkdir(parents=True, exist_ok=True)
    final = dst_dir / "part.parquet"
    tmp = dst_dir / (".part.parquet.%d.tmp" % (os.getpid(),))
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, tmp, compression="snappy")
    os.replace(tmp, final)
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
def _flightlist_files(flightlist_dir: Path) -> list[Path]:
    if flightlist_dir.is_file():
        return [flightlist_dir]
    return sorted(flightlist_dir.rglob("*.parquet"))


_FINAL_DEDUP = {
    "hub_aircraft":          ["hk_aircraft"],
    "hub_airport":           ["hk_airport"],
    "hub_flight":            ["hk_flight"],
    "link_flight_aircraft":  ["hk_flight_aircraft"],
    "link_flight_route":     ["hk_flight_route"],
    "sat_aircraft_identity": ["hk_aircraft"],
    "sat_flight_summary":    ["hk_flight"],
}


def _dedup_safely(df: pd.DataFrame, subset: list[str]) -> pd.DataFrame:
    """drop_duplicates without hitting pyarrow's 2 GiB single-malloc limit.

    pandas "string" dtype is pyarrow-backed; its .factorize() routes through
    pyarrow.ChunkedArray.dictionary_encode, which pre-allocates a single
    buffer that fails at 2 GiB on large accumulators. Converting the key
    columns to numpy-object first routes dedup through pandas' python-level
    hashtable, which has no such ceiling. Convert back to "string" after.
    """
    if df.empty:
        return df
    pa_cols = [c for c in subset if df[c].dtype == "string"]
    if pa_cols:
        df = df.astype({c: "object" for c in pa_cols})
    out = df.drop_duplicates(subset=subset, keep="first")
    if pa_cols:
        out = out.astype({c: "string" for c in pa_cols})
    return out


def _states_extras_frame(states_hub_input: pd.DataFrame,
                         load_dt: datetime) -> pd.DataFrame:
    """icao24 projection from raw_states bronze folded into hub_aircraft.

    Preserves the multi-source contract from build_hub_aircraft: flightlist
    rows win (rec_src='opensky_zenodo'), state-vector rows fill the gaps
    with rec_src='opensky_s3'. The compaction pass passes this frame LAST
    into the union so _dedup_safely(keep='first') keeps the flightlist
    rec_src tag whenever both sources observed the same icao24.
    """
    if states_hub_input.empty:
        return pd.DataFrame(columns=["hk_aircraft", "icao24", "load_dt", "rec_src"])
    extra = states_hub_input.copy()
    extra["icao24"] = extra["icao24"].astype("string").str.strip().str.upper()
    extra = extra.dropna(subset=["icao24"]).drop_duplicates(subset=["icao24"])
    extra["hk_aircraft"] = hash_series(extra["icao24"])
    extra["load_dt"] = load_dt
    extra["rec_src"] = "opensky_s3"
    return extra[["hk_aircraft", "icao24", "load_dt", "rec_src"]]


def _compact_small_hub(name: str, new_frames: list[pd.DataFrame],
                       out_dir: Path) -> int:
    from etl4.vault.compact import compact_small_hub
    return compact_small_hub(name, new_frames, out_dir)


def _partition_done(table: str, ym: str, out_dir: Path) -> bool:
    """A (table, ym) is considered already-processed when both the
    deterministic part.parquet and the _DONE sentinel exist beside it."""
    ym_dir = out_dir / table / ("ym=" + ym)
    return (ym_dir / "_DONE").exists() and (ym_dir / "part.parquet").exists()


def _group_state_files_by_ym(states_dir: Path,
                             last_ym: str | None) -> dict[str, list[Path]]:
    """Group raw_states bronze files by path-derived ym; drop ym <= last_ym.

    Paths carry month in their ``date=YYYY-MM-DD`` segment, so filtering
    happens without reading the parquet. Live bronze has path ym and
    per-row epoch ym in agreement; the per-row epoch ym is still computed
    on read as the in-file column so a future out-of-month row fails the
    write-side assertion in ``_build_sat_state_observation_per_ym`` loudly
    rather than overwriting a prior month's partition on write.
    """
    out: dict[str, list[Path]] = defaultdict(list)
    if not states_dir.exists():
        return out
    for f in sorted(states_dir.rglob("*.parquet")):
        try:
            ym = vault_io.extract_ym_from_path(f)
        except ValueError:
            continue
        if last_ym is not None and ym <= last_ym:
            continue
        out[ym].append(f)
    return out


def _build_sat_state_observation_per_ym(states_dir: Path, out_dir: Path,
                                        load_dt: datetime,
                                        dsn: str = DSN_DEFAULT) -> int:
    """Per-ym build of sat_state_observation from raw_states bronze (pandas).

    Only files under ``date=YYYY-MM-*`` with ym > watermark are read.
    Each resulting partition lands at
    ``<out_dir>/sat_state_observation/ym=YYYY-MM/part.parquet`` with a
    ``_DONE`` sentinel; the watermark advances per ym in its own
    transaction. The sat watermark key is a separate row from the four
    flightlist-vault keys (different ym axes: flightlist month vs
    state-vector month) so the per-ym commit cannot share the flightlist
    transaction.

    Resumed partitions (``_DONE`` + ``part.parquet`` both present) still
    re-upsert the watermark row so a crash between a successful partition
    write and the previous watermark commit cannot leave the registry
    stuck behind a materialised month.
    """
    state_cols = ["icao24", "time", "lat", "lon", "baroaltitude",
                  "velocity", "heading", "vertrate", "callsign", "onground"]

    sat_last_ym = watermark.get("vault", "sat_state_observation", dsn=dsn)
    if sat_last_ym is not None:
        print(f"  sat_state_observation: resuming from watermark: "
              f"last_ym={sat_last_ym}", flush=True)
    else:
        print("  sat_state_observation: no watermark; processing every "
              "bronze month", flush=True)

    files_by_ym = _group_state_files_by_ym(states_dir, sat_last_ym)
    if not files_by_ym:
        print("  sat_state_observation: no new months to process", flush=True)
        return vault_io.row_count("sat_state_observation", root=out_dir)

    for ym in sorted(files_by_ym):
        if _partition_done("sat_state_observation", ym, out_dir):
            rc = vault_io.row_count("sat_state_observation", ym_filter=ym,
                                    root=out_dir)
            with psycopg.connect(dsn) as conn:
                watermark.set("vault", "sat_state_observation", ym, rc, conn)
                conn.commit()
            print(f"    sat_state_observation/ym={ym}: resumed "
                  f"(part.parquet + _DONE); watermark reaffirmed",
                  flush=True)
            continue

        frames = []
        for f in files_by_ym[ym]:
            try:
                frames.append(pd.read_parquet(f, columns=state_cols))
            except Exception as e:
                print(f"    skipped {f.relative_to(ROOT)}: "
                      f"{type(e).__name__}: {e}", flush=True)
        if not frames:
            continue
        states = pd.concat(frames, ignore_index=True)
        sat = build_sat_state_observation(states, load_dt)
        sat = sat.reset_index(drop=True)
        sat_ym = vault_io.ym_from_epoch(sat["time"])
        uniq = set(sat_ym.dropna().unique())
        if uniq and uniq != {ym}:
            raise AssertionError(
                "sat_state_observation/ym=%s: epoch-derived ym in file "
                "disagrees with path ym: %s" % (ym, sorted(uniq))
            )
        sat["ym"] = sat_ym.astype("string")

        vault_io.write_partition(sat, "sat_state_observation", ym, out_dir=out_dir)
        _write_done("sat_state_observation", ym, out_dir, load_dt)
        rc = vault_io.row_count("sat_state_observation", ym_filter=ym, root=out_dir)
        with psycopg.connect(dsn) as conn:
            watermark.set("vault", "sat_state_observation", ym, rc, conn)
            conn.commit()
        print(f"    sat_state_observation/ym={ym}: {rc:,}", flush=True)
        del states, sat
        gc.collect()

    return vault_io.row_count("sat_state_observation", root=out_dir)


def _write_done(table: str, ym: str, out_dir: Path, load_dt: datetime) -> None:
    sentinel = out_dir / table / ("ym=" + ym) / "_DONE"
    sentinel.write_text(f"load_dt={load_dt.isoformat()}\n")


def _chunked_flightlist_vault(flightlist_dir: Path, states_hub_input: pd.DataFrame,
                              out_dir: Path, load_dt: datetime,
                              dsn: str = DSN_DEFAULT) -> dict[str, int]:
    """Per-ym build: one bronze flightlist file maps to one month.

    Flow:
      1. watermark.get('vault', 'hub_flight') -> last_ym (None on first run).
      2. For each bronze file sorted by filename:
         a. ym = ym_from_filename; skip None (not a flightlist); skip
            ym <= last_ym; otherwise read the file.
         b. Build 7 tables. Write the 4 partitioned tables as
            ym=YYYY-MM/part.parquet via vault_io.write_partition; drop _DONE
            sentinels once each part lands.
         c. Union the 3 small-hub frames into the existing monolith, dedup
            on the business key, rewrite atomically. States extras fold in
            on the final month (last-in-first-seen keeps zenodo rows).
         d. Advance pipeline_watermarks for all 4 partitioned tables in one
            transaction. Commit, so the next crash recovers from the last
            fully-processed ym.

    The former shard+DuckDB dedup pass is removed: each bronze file is one
    month, so cross-file dedup is unnecessary. Per-month dedup happens
    inside each builder (drop_duplicates on business keys).
    """
    files = _flightlist_files(flightlist_dir)
    counts: dict[str, int] = {k: 0 for k in _FINAL_DEDUP}

    last_ym = watermark.get("vault", "hub_flight", dsn=dsn)
    if last_ym is not None:
        print(f"  resuming from watermark: last_ym={last_ym}", flush=True)
    else:
        print("  no vault watermark; processing every bronze month", flush=True)

    builders_partitioned = {
        "hub_flight":           lambda fl: build_hub_flight(fl, load_dt),
        "link_flight_aircraft": lambda fl: build_link_flight_aircraft(fl, load_dt),
        "link_flight_route":    lambda fl: build_link_flight_route(fl, load_dt),
        "sat_flight_summary":   lambda fl: build_sat_flight_summary(fl, load_dt),
    }
    builders_small = {
        # hub_aircraft draws only from the flightlist's icao24 column here; the
        # states_hub_input extras are folded in on the final month below.
        "hub_aircraft":          lambda fl: build_hub_aircraft(fl, pd.DataFrame(), load_dt),
        "hub_airport":           lambda fl: build_hub_airport(fl, load_dt),
        "sat_aircraft_identity": lambda fl: build_sat_aircraft_identity(fl, load_dt),
    }

    file_yms: list[tuple[Path, str]] = []
    for f in files:
        ym = vault_io.ym_from_filename(f.name)
        if ym is None:
            print(f"  skipping {f.name}: no ym match", flush=True)
            continue
        if last_ym is not None and ym <= last_ym:
            continue
        file_yms.append((f, ym))

    if not file_yms:
        if not states_hub_input.empty:
            print("  no new flightlist months, folding states extras only ...",
                  flush=True)
            extras = _states_extras_frame(states_hub_input, load_dt)
            if not extras.empty:
                _compact_small_hub("hub_aircraft", [extras], out_dir)
        for k in _FINAL_DEDUP:
            counts[k] = vault_io.row_count(k, root=out_dir)
        print("  no new flightlist months to process", flush=True)
        return counts

    print(f"  will process {len(file_yms)} new month(s): "
          f"{file_yms[0][1]} .. {file_yms[-1][1]}", flush=True)

    final_ym_index = len(file_yms) - 1
    for idx, (f, ym) in enumerate(file_yms):
        print(f"  [{idx + 1}/{len(file_yms)}] ym={ym} <- {f.name}", flush=True)

        skip_mask = [_partition_done(t, ym, out_dir) for t in PARTITIONED_TABLES]
        fl: pd.DataFrame | None = None
        if not all(skip_mask):
            fl = pd.read_parquet(f)

        for t in PARTITIONED_TABLES:
            if _partition_done(t, ym, out_dir):
                print(f"    {t}/ym={ym}: resumed (part.parquet + _DONE)",
                      flush=True)
                continue
            frame = builders_partitioned[t](fl)
            frame = frame.reset_index(drop=True)
            # Keep the partition key inside the file body so pure-file reads
            # (CLAUDE.md section 7) retain it. See architecture finding #4.
            frame["ym"] = pd.Series([ym] * len(frame), dtype="string")
            vault_io.write_partition(frame, t, ym, out_dir=out_dir)
            _write_done(t, ym, out_dir, load_dt)
            print(f"    {t}/ym={ym}: {len(frame):,}", flush=True)
            del frame

        new_small_frames: dict[str, list[pd.DataFrame]] = {
            t: [] for t in NON_PARTITIONED_TABLES
        }
        if fl is not None:
            for t in NON_PARTITIONED_TABLES:
                new_small_frames[t].append(builders_small[t](fl))
        if idx == final_ym_index and not states_hub_input.empty:
            new_small_frames["hub_aircraft"].append(
                _states_extras_frame(states_hub_input, load_dt)
            )

        for t in NON_PARTITIONED_TABLES:
            n = _compact_small_hub(t, new_small_frames[t], out_dir)
            print(f"    {t} compacted: {n:,}", flush=True)

        with psycopg.connect(dsn) as conn:
            for t in PARTITIONED_TABLES:
                rc = vault_io.row_count(t, ym_filter=ym, root=out_dir)
                watermark.set("vault", t, ym, rc, conn)
            conn.commit()

        if fl is not None:
            del fl
        gc.collect()

    for k in _FINAL_DEDUP:
        counts[k] = vault_io.row_count(k, root=out_dir)
    return counts


def build_raw_vault(flightlist_dir: Path = FLIGHTLIST_BRONZE,
                    states_dir: Path = STATES_BRONZE,
                    out_dir: Path = VAULT_DIR,
                    include_states: bool = True,
                    engine: str = "pandas") -> dict:
    """Read bronze, write every raw-vault table. Returns row counts.

    Flightlists are processed file-by-file (streaming) so large bronze trees
    (50+ monthly files, ~10 GB expanded) do not blow pandas memory.

    engine: "pandas" (default) or "spark". Spark routes only the large
    sat_state_observation build through PySpark applyInPandas; the flightlist
    hubs, links, and small satellites always stay on chunked pandas.
    """
    load_dt = datetime.now(timezone.utc)

    print("loading bronze ...")
    # Only load the slim icao24 projection of states for the hub build;
    # sat_state_observation builds the full read itself (pandas or Spark).
    # pandas read_parquet(columns=...) is more tolerant of per-file schema
    # drift (e.g. mixed int64 vs dictionary types on ancillary columns like
    # `hour`, which pyarrow's dataset loader refuses to merge).
    # The hub projection is filtered by the sat watermark so a rerun
    # against the same bronze does not re-scan months already folded.
    states_hub_input = pd.DataFrame(columns=["icao24"])
    if include_states and states_dir.exists():
        sat_last_ym = watermark.get("vault", "sat_state_observation")
        hub_files_by_ym = _group_state_files_by_ym(states_dir, sat_last_ym)
        frames = []
        for ym in sorted(hub_files_by_ym):
            for f in hub_files_by_ym[ym]:
                try:
                    frames.append(pd.read_parquet(f, columns=["icao24"]))
                except Exception as e:
                    print(f"  skipped {f.relative_to(ROOT)}: "
                          f"{type(e).__name__}: {e}")
        if frames:
            states_hub_input = pd.concat(frames, ignore_index=True)
    print(f"  state-vector icao24 rows (hub input): {len(states_hub_input):,}")

    print("streaming flightlist tree ...")
    counts = dict(_chunked_flightlist_vault(flightlist_dir, states_hub_input,
                                            out_dir, load_dt))

    if engine == "spark" and include_states:
        print("  sat_state_observation via Spark (applyInPandas on icao24) ...")
        from etl4.vault.raw_vault_spark import build_sat_state_observation_spark
        counts["sat_state_observation"] = build_sat_state_observation_spark(
            states_dir=states_dir,
            out_dir=out_dir / "sat_state_observation",
        )
    elif include_states:
        counts["sat_state_observation"] = _build_sat_state_observation_per_ym(
            states_dir, out_dir, load_dt,
        )
    else:
        # include_states=False: do NOT truncate the existing partitioned
        # tree (prior shape was a multi-GB monolith; current shape is the
        # authoritative ym=*/part.parquet set). Just report the current
        # count.
        counts["sat_state_observation"] = vault_io.row_count(
            "sat_state_observation", root=out_dir,
        )
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
    p.add_argument("--engine",         choices=["pandas", "spark"], default="pandas",
                   help="pandas (default, single-node) or spark (distributed state_observation build)")
    a = p.parse_args()
    build_raw_vault(
        Path(a.flightlist_dir), Path(a.states_dir), Path(a.out),
        include_states=not a.no_states,
        engine=a.engine,
    )


if __name__ == "__main__":
    main()
