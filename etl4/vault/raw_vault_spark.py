"""Spark-backed build of the state_observation satellite.

Pandas builds the whole raw vault in-process. That is fine up to ~10M rows but
it is also the single biggest scale-ceiling in the project. This module shows
the same satellite build distributed across a Spark cluster using
`groupBy(icao24).applyInPandas(...)` -- same pandas code, now parallelised.

Why only this one satellite?
  - sat_state_observation is by far the largest table (~4.4M rows today,
    grows linearly with every daily poll). The other hubs/links/sats are
    tens of thousands of rows at most -- Spark overhead would dwarf them.
  - Dedup on (hk_aircraft, time) is naturally per-aircraft work, so the
    groupBy key is semantically honest, not a shoehorn.

Partition contract:
  Output is per-ym under ``<out_dir>/ym=YYYY-MM/part.parquet``. One Spark
  job per month: filter bronze paths by the ``sat_state_observation``
  watermark, read only that month's shards, groupBy(icao24).applyInPandas,
  coalesce(1), move the single resulting shard onto the deterministic
  ``part.parquet`` so the resume gate pattern (``_DONE`` + ``part.parquet``)
  continues to work.

  ``ym`` is derived per-row inside the pandas UDF via
  ``pd.to_datetime(time, unit='s', utc=True).dt.strftime('%Y-%m')``.
  Spark's ``from_unixtime``/``date_format`` honour the session timezone
  and would shift month boundaries under IST (the dev host); the pandas
  helper is utc-fixed so the column matches ``vault_io.ym_from_epoch``
  bit-for-bit, which is what the vault_io reader invariant and the
  Postgres backfill both expect. The epoch-vs-path ym agreement check
  from the pandas path is NOT applied here: the Spark UDF returns rows
  already labelled with the row's epoch ym, which is then used only as
  a schema column (the partition directory is chosen by the driver from
  the file path).

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

# NB: psycopg / etl4.vault.watermark / etl4.vault.raw_vault are driver-only
# and are imported inside build_sat_state_observation_spark so they are not
# pickled onto executors by reference when _build_group is shipped.

ROOT = Path(__file__).resolve().parents[2]
STATES_BRONZE = ROOT / "data" / "bronze" / "raw_states"
OUT_DEFAULT   = ROOT / "data" / "silver" / "vault" / "sat_state_observation"

_SAT_COLS = ["hk_aircraft", "time", "load_dt", "rec_src",
             "lat", "lon", "baroaltitude", "velocity", "heading",
             "vertrate", "callsign", "onground"]
_SAT_COLS_WITH_YM = _SAT_COLS + ["ym"]


def _build_group(pdf):
    """applyInPandas UDF: dedup one icao24's observations, project schema.

    Receives the pandas DataFrame for a single icao24 group; returns the
    rows belonging to that group's satellite entry. Pure pandas -- identical
    to the single-node path, so the invariants in test_pipeline.py still hold.
    The ``ym`` column is derived per-row via utc-fixed pandas, not Spark's
    session-timezone-sensitive ``date_format``.
    """
    import pandas as pd  # import inside UDF: executors need it, not driver

    if pdf.empty:
        return pd.DataFrame(columns=_SAT_COLS_WITH_YM)
    df = pdf.dropna(subset=["icao24", "time"]).copy()
    if df.empty:
        return pd.DataFrame(columns=_SAT_COLS_WITH_YM)
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
    df["ym"] = (
        pd.to_datetime(df["time"], unit="s", utc=True)
          .dt.strftime("%Y-%m")
          .astype("string")
    )
    return df[_SAT_COLS_WITH_YM]


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
        StructField("ym",           StringType(),    False),
    ])


def build_sat_state_observation_spark(states_dir: Path = STATES_BRONZE,
                                      out_dir: Path = OUT_DEFAULT,
                                      master: str | None = None,
                                      dsn: str | None = None) -> int:
    """Per-ym distributed state_observation satellite build. Returns total rows.

    One Spark job per month. Pre-filters bronze paths against the
    ``('vault','sat_state_observation')`` watermark so an
    already-materialised month is never read. Writes
    ``<out_dir>/ym=YYYY-MM/part.parquet`` plus a ``_DONE`` sentinel per
    ym; advances the watermark in its own psycopg transaction per ym
    (both for newly-written and resumed partitions, so a crash between a
    partition write and its watermark commit cannot leave the registry
    stuck behind a materialised month).

    ``out_dir`` must end in ``/sat_state_observation`` so the sentinel
    path derived from its parent resolves to the same tree the writes
    land in; standalone callers passing a different basename are
    rejected up-front.
    """
    # Driver-only imports kept out of module scope so they are not
    # pickled onto executors when _build_group is shipped by reference.
    import psycopg
    from pyspark.sql import SparkSession

    from etl4.vault import vault_io, watermark
    from etl4.vault.raw_vault import (
        DSN_DEFAULT,
        _group_state_files_by_ym,
        _partition_done,
        _write_done,
    )

    dsn = dsn or DSN_DEFAULT
    out_dir = Path(out_dir)
    if out_dir.name != "sat_state_observation":
        raise ValueError(
            "out_dir basename must be 'sat_state_observation'; got %r"
            % (out_dir.name,)
        )
    vault_root = out_dir.parent
    out_dir.mkdir(parents=True, exist_ok=True)

    sat_last_ym = watermark.get("vault", "sat_state_observation", dsn=dsn)
    if sat_last_ym is not None:
        print(f"[spark] resuming from watermark: last_ym={sat_last_ym}",
              flush=True)
    else:
        print("[spark] no watermark; processing every bronze month", flush=True)

    files_by_ym = _group_state_files_by_ym(Path(states_dir), sat_last_ym)
    if not files_by_ym:
        print("[spark] no new sat months to process", flush=True)
        return vault_io.row_count("sat_state_observation", root=vault_root)

    master = master or os.getenv("SPARK_MASTER", "local[*]")
    spark = (SparkSession.builder
             .appName("etl4-sat-state-observation")
             .master(master)
             .config("spark.sql.execution.arrow.pyspark.enabled", "true")
             .getOrCreate())

    load_dt = datetime.now(timezone.utc)
    try:
        for ym in sorted(files_by_ym):
            if _partition_done("sat_state_observation", ym, vault_root):
                rc = vault_io.row_count("sat_state_observation",
                                        ym_filter=ym, root=vault_root)
                with psycopg.connect(dsn) as conn:
                    watermark.set("vault", "sat_state_observation",
                                  ym, rc, conn)
                    conn.commit()
                print(f"[spark] sat_state_observation/ym={ym}: resumed; "
                      f"watermark reaffirmed", flush=True)
                continue

            paths = [str(p) for p in files_by_ym[ym]]
            states = spark.read.parquet(*paths)
            if "icao24" not in states.columns:
                raise RuntimeError(
                    f"bronze states missing icao24 column; got {states.columns}"
                )
            sat = states.groupBy("icao24").applyInPandas(
                _build_group, schema=_output_schema(),
            )

            ym_dir = out_dir / f"ym={ym}"
            ym_dir.mkdir(parents=True, exist_ok=True)
            # tmp dir name must NOT match the ym=* glob so the reader does
            # not pick up a half-written shard.
            tmp = out_dir / f"_spark_tmp_{ym.replace('-', '_')}"
            if tmp.exists():
                shutil.rmtree(tmp)
            sat.coalesce(1).write.mode("overwrite").parquet(str(tmp))
            part_src = next(tmp.glob("*.parquet"), None)
            if part_src is None:
                raise RuntimeError(f"Spark produced no parquet file in {tmp}")
            # os.replace is atomic on both POSIX and Windows when src and
            # dst are on the same filesystem; shutil.move falls back to
            # copy+unlink on Windows when the target exists.
            os.replace(str(part_src), str(ym_dir / "part.parquet"))
            shutil.rmtree(tmp)
            _write_done("sat_state_observation", ym, vault_root, load_dt)

            rc = vault_io.row_count("sat_state_observation", ym_filter=ym,
                                    root=vault_root)
            with psycopg.connect(dsn) as conn:
                watermark.set("vault", "sat_state_observation", ym, rc, conn)
                conn.commit()
            print(f"[spark] sat_state_observation/ym={ym}: {rc:,}", flush=True)
    finally:
        spark.stop()

    return vault_io.row_count("sat_state_observation", root=vault_root)


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
