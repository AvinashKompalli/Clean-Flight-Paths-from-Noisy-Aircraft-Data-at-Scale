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
import gc
import shutil
from datetime import datetime, timezone
from pathlib import Path

import duckdb
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


def _duckdb_dedup_shards(shard_dir: Path, final_path: Path, keys: list[str],
                         tmp_dir: Path, memory_limit: str = "4GB",
                         threads: int = 4) -> int:
    """Stream-dedup every parquet shard under shard_dir into final_path.

    PARTITION BY the business key, ORDER BY the per-shard filename so the
    first-file-seen contract from the pandas path is preserved (numbered
    `part-NNN.parquet` sort before `zzz_extras.parquet`). DuckDB spills the
    hash+sort to tmp_dir, so peak RAM is memory_limit, not the shards' total
    size. Returns the row count written.
    """
    shard_glob = str(shard_dir / "*.parquet").replace("\\", "/")
    final_out  = str(final_path).replace("\\", "/")
    tmp_dir.mkdir(parents=True, exist_ok=True)
    tmp_dir_s  = str(tmp_dir).replace("\\", "/")
    partition  = ", ".join(keys)

    sql = f"""
    COPY (
      SELECT * EXCLUDE(_fn)
      FROM (
        SELECT *, filename AS _fn
        FROM read_parquet('{shard_glob}', filename=true)
      )
      QUALIFY row_number() OVER (PARTITION BY {partition} ORDER BY _fn) = 1
    ) TO '{final_out}' (FORMAT PARQUET, COMPRESSION 'snappy')
    """
    con = duckdb.connect()
    try:
        con.execute(f"PRAGMA memory_limit='{memory_limit}'")
        con.execute(f"PRAGMA threads={threads}")
        con.execute(f"PRAGMA temp_directory='{tmp_dir_s}'")
        con.execute(sql)
        n = con.execute(f"SELECT count(*) FROM read_parquet('{final_out}')").fetchone()[0]
    finally:
        con.close()
    return int(n)


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


def _chunked_flightlist_vault(flightlist_dir: Path, states_hub_input: pd.DataFrame,
                              out_dir: Path, load_dt: datetime) -> dict[str, int]:
    """Spill each per-file builder output to a parquet shard, then dedupe
    per table. Nothing accumulates in Python memory across files, so the
    peak RSS is bounded by one file's working set (~500 MB raw flightlist)
    plus the single table being finalized at the end. This replaces the
    earlier in-RAM accumulator approach, which grew with the input and hit
    Windows commit/fragmentation limits around 10/48 files (segfault in
    pyarrow's native allocator).
    """
    files = _flightlist_files(flightlist_dir)
    shard_root = out_dir / "_shards"
    # Shards from an earlier interrupted run are reused: any file whose 7
    # per-table shards are all present is skipped. A fresh run can be forced
    # by deleting the shard_root directory before invoking build_raw_vault.

    counts: dict[str, int] = {k: 0 for k in _FINAL_DEDUP}
    if not files and states_hub_input.empty:
        for k in _FINAL_DEDUP:
            _write(pd.DataFrame(), k, out_dir)
        return counts

    builders = {
        "hub_aircraft":          lambda fl: build_hub_aircraft(fl, pd.DataFrame(), load_dt),
        "hub_airport":           lambda fl: build_hub_airport(fl, load_dt),
        "hub_flight":            lambda fl: build_hub_flight(fl, load_dt),
        "link_flight_aircraft":  lambda fl: build_link_flight_aircraft(fl, load_dt),
        "link_flight_route":     lambda fl: build_link_flight_route(fl, load_dt),
        "sat_aircraft_identity": lambda fl: build_sat_aircraft_identity(fl, load_dt),
        "sat_flight_summary":    lambda fl: build_sat_flight_summary(fl, load_dt),
    }
    for t in builders:
        (shard_root / t).mkdir(parents=True, exist_ok=True)

    for i, f in enumerate(files, 1):
        shard_paths = {t: shard_root / t / f"part-{i:03d}.parquet" for t in builders}
        if all(p.exists() for p in shard_paths.values()):
            if i % 5 == 0 or i == len(files):
                print(f"  resumed {i}/{len(files)} files from existing shards",
                      flush=True)
            continue
        # Partial shard set (prior crash mid-file): wipe partials so the file
        # is rebuilt atomically.
        for p in shard_paths.values():
            p.unlink(missing_ok=True)
        fl = pd.read_parquet(f)
        for t, builder in builders.items():
            frame = builder(fl)
            # Per-file dedup on the business key keeps shards small and speeds
            # up the final pass. Full cross-file dedup still happens below.
            frame = _dedup_safely(frame, _FINAL_DEDUP[t]).reset_index(drop=True)
            frame.to_parquet(shard_paths[t], index=False)
            del frame
        del fl
        gc.collect()
        if i % 5 == 0 or i == len(files):
            print(f"  streamed {i}/{len(files)} files, shards spilled",
                  flush=True)

    # Fold state-vector icao24s into hub_aircraft before DuckDB sees the shards,
    # so the multi-source hub contract from build_hub_aircraft is preserved.
    # Name the extras file so it sorts AFTER every `part-NNN.parquet` shard; the
    # finalize dedup orders by filename, and the pandas path's first-file-seen
    # contract maps to "numbered shards win; extras fill only the gaps".
    hub_aircraft_shard_dir = shard_root / "hub_aircraft"
    extras_path = hub_aircraft_shard_dir / "zzz_extras.parquet"
    if not states_hub_input.empty and not extras_path.exists():
        extra = states_hub_input.copy()
        extra["icao24"] = extra["icao24"].astype("string").str.strip().str.upper()
        extra = extra.dropna(subset=["icao24"]).drop_duplicates(subset=["icao24"])
        extra["hk_aircraft"] = hash_series(extra["icao24"])
        extra["load_dt"] = load_dt
        extra["rec_src"] = "opensky_s3"
        hub_aircraft_shard_dir.mkdir(parents=True, exist_ok=True)
        extra[["hk_aircraft", "icao24", "load_dt", "rec_src"]].to_parquet(
            extras_path, index=False,
        )
        del extra
        gc.collect()

    print("  finalizing shards -> one part.parquet per table (DuckDB stream) ...",
          flush=True)
    duckdb_tmp = out_dir / "_duckdb_tmp"
    for t, keys in _FINAL_DEDUP.items():
        table_shard_dir = shard_root / t
        final_path = out_dir / t / "part.parquet"
        sentinel   = out_dir / t / "_DONE"
        # Resume gate: a _DONE sentinel beside part.parquet means a prior run
        # finalized this table successfully. The old gate ("shard dir missing")
        # was unreliable because the streaming phase above re-creates shard
        # dirs at startup, so it never fired even when the final was good.
        if sentinel.exists() and final_path.exists():
            counts[t] = int(duckdb.sql(
                f"SELECT count(*) FROM read_parquet('"
                f"{str(final_path).replace(chr(92), '/')}')"
            ).fetchone()[0])
            print(f"    {t}: {counts[t]:,} (resumed from _DONE sentinel)",
                  flush=True)
            if table_shard_dir.exists():
                shutil.rmtree(table_shard_dir)
            continue

        shards = sorted(table_shard_dir.glob("*.parquet")) if table_shard_dir.exists() else []
        if not shards:
            _write(pd.DataFrame(), t, out_dir)
            counts[t] = 0
        else:
            final_path.parent.mkdir(parents=True, exist_ok=True)
            counts[t] = _duckdb_dedup_shards(
                table_shard_dir, final_path, keys, duckdb_tmp,
            )
        print(f"    {t}: {counts[t]:,}", flush=True)

        sentinel.write_text(f"load_dt={load_dt.isoformat()}\n")
        # Free this table's shards now that the final parquet + sentinel are
        # on disk. Keeps peak disk bounded during the finalize pass.
        if table_shard_dir.exists():
            shutil.rmtree(table_shard_dir)
        gc.collect()

    if duckdb_tmp.exists():
        shutil.rmtree(duckdb_tmp, ignore_errors=True)
    if shard_root.exists():
        shutil.rmtree(shard_root)
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
    states_hub_input = pd.DataFrame(columns=["icao24"])
    if include_states and states_dir.exists():
        files = sorted(states_dir.rglob("*.parquet"))
        frames = []
        for f in files:
            try:
                frames.append(pd.read_parquet(f, columns=["icao24"]))
            except Exception as e:
                print(f"  skipped {f.relative_to(ROOT)}: {type(e).__name__}: {e}")
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
        state_cols = ["icao24", "time", "lat", "lon", "baroaltitude",
                      "velocity", "heading", "vertrate", "callsign", "onground"]
        s_files = sorted(states_dir.rglob("*.parquet")) if states_dir.exists() else []
        s_frames = []
        for f in s_files:
            try:
                s_frames.append(pd.read_parquet(f, columns=state_cols))
            except Exception as e:
                print(f"  skipped {f.relative_to(ROOT)}: {type(e).__name__}: {e}")
        states = pd.concat(s_frames, ignore_index=True) if s_frames else pd.DataFrame()
        counts["sat_state_observation"] = _write(
            build_sat_state_observation(states, load_dt),
            "sat_state_observation", out_dir,
        )
        del states
    else:
        counts["sat_state_observation"] = _write(
            pd.DataFrame(), "sat_state_observation", out_dir,
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
