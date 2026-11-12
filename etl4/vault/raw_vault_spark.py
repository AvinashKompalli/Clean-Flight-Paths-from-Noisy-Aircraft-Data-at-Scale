"""Spark-backed build of the state_observation satellite.

Pandas builds the whole raw vault in-process. That is fine up to ~10M rows but
it is also the single biggest scale-ceiling in the project. This module shows
the same satellite build distributed across a Spark cluster using
`groupBy(icao24).applyInPandas(...)` — same pandas code, now parallelised.

Why only this one satellite?
  - sat_state_observation is by far the largest table (~4.4M rows today,
    grows linearly with every daily poll). The other hubs/links/sats are
    tens of thousands of rows at most — Spark overhead would dwarf them.
  - Dedup on (hk_aircraft, time) is naturally per-aircraft work, so the
    groupBy key is semantically honest, not a shoehorn.

Usage
-----
Called by raw_vault.py when `--engine spark` is passed. Standalone:

    python -m etl4.vault.raw_vault_spark \\
        --states data/bronze/raw_states \\
        --out    data/silver/vault/sat_state_observation

Requires `pyspark` installed and either a reachable Spark master
(`SPARK_MASTER` env var, default `spark://localhost:7077`) or the fallback
`local[*]` when `--local` is passed.
"""
from __future__ import annotations

import argparse
import hashlib
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
STATES_BRONZE = ROOT / "data" / "bronze" / "raw_states"
OUT_DEFAULT   = ROOT / "data" / "silver" / "vault" / "sat_state_observation"

_SAT_COLS = ["hk_aircraft", "time", "load_dt", "rec_src",
             "lat", "lon", "baroaltitude", "velocity", "heading",
             "vertrate", "callsign", "onground"]


def _build_group(pdf):
    """applyInPandas UDF: dedup one icao24's observations, project schema.

    Receives the pandas DataFrame for a single icao24 group; returns the
    rows belonging to that group's satellite entry. Pure pandas — identical
    to the single-node path, so the invariants in test_pipeline.py still hold.
    """
    import pandas as pd  # import inside UDF: executors need it, not driver

    if pdf.empty:
        return pd.DataFrame(columns=_SAT_COLS)
    df = pdf.dropna(subset=["icao24", "time"]).copy()
    if df.empty:
        return pd.DataFrame(columns=_SAT_COLS)
    canon = str(df["icao24"].iloc[0]).strip().upper()
    hk = hashlib.md5(canon.encode("utf-8")).hexdigest()
    df["hk_aircraft"] = hk
    df["time"] = df["time"].astype("int64")
    df["load_dt"] = datetime.now(timezone.utc)
    df["rec_src"] = "kafka_bronze"
    df = df.drop_duplicates(subset=["hk_aircraft", "time"], keep="first")
    for c in ["lat", "lon", "baroaltitude", "velocity", "heading", "vertrate"]:
        if c not in df.columns:
            df[c] = None
    if "callsign" not in df.columns:
        df["callsign"] = None
    if "onground" not in df.columns:
        df["onground"] = None
    return df[_SAT_COLS]


def _output_schema():
    from pyspark.sql.types import (StructType, StructField, StringType,
                                   LongType, DoubleType, BooleanType, TimestampType)
    return StructType([
        StructField("hk_aircraft",  StringType(),    False),
        StructField("time",         LongType(),      False),
        StructField("load_dt",      TimestampType(), False),
        StructField("rec_src",      StringType(),    False),
        StructField("lat",          DoubleType(),    True),
        StructField("lon",          DoubleType(),    True),
        StructField("baroaltitude", DoubleType(),    True),
        StructField("velocity",     DoubleType(),    True),
        StructField("heading",      DoubleType(),    True),
        StructField("vertrate",     DoubleType(),    True),
        StructField("callsign",     StringType(),    True),
        StructField("onground",     BooleanType(),   True),
    ])


def build_sat_state_observation_spark(states_dir: Path = STATES_BRONZE,
                                      out_dir: Path = OUT_DEFAULT,
                                      master: str | None = None) -> int:
    """Distributed state_observation satellite build. Returns row count."""
    from pyspark.sql import SparkSession

    master = master or os.getenv("SPARK_MASTER", "local[*]")
    spark = (SparkSession.builder
             .appName("etl4-sat-state-observation")
             .master(master)
             .config("spark.sql.execution.arrow.pyspark.enabled", "true")
             .getOrCreate())

    try:
        states = spark.read.parquet(str(states_dir))
        print(f"[spark] input rows: {states.count():,}", flush=True)
        if "icao24" not in states.columns:
            raise RuntimeError(f"bronze states missing icao24 column; got {states.columns}")

        sat = states.groupBy("icao24").applyInPandas(_build_group, schema=_output_schema())

        if out_dir.exists():
            shutil.rmtree(out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        tmp = out_dir / "_spark"
        sat.coalesce(1).write.mode("overwrite").parquet(str(tmp))

        part_src = next(tmp.glob("*.parquet"), None)
        if part_src is None:
            raise RuntimeError(f"Spark produced no parquet file in {tmp}")
        shutil.move(str(part_src), str(out_dir / "part.parquet"))
        shutil.rmtree(tmp)

        n = sat.count()
        print(f"[spark] sat_state_observation rows: {n:,}", flush=True)
        return int(n)
    finally:
        spark.stop()


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--states", default=str(STATES_BRONZE))
    p.add_argument("--out",    default=str(OUT_DEFAULT))
    p.add_argument("--local",  action="store_true",
                   help="force local[*] master (ignore SPARK_MASTER)")
    a = p.parse_args()
    build_sat_state_observation_spark(
        Path(a.states), Path(a.out),
        master="local[*]" if a.local else None,
    )


if __name__ == "__main__":
    main()
