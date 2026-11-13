"""Snapshot row + byte counts across every pipeline layer into Postgres.

Powers the ETL4 - DE Pipeline dashboard. Parquet row counts come from
footer metadata (O(files), not O(rows)), Postgres counts come from
pg_class.reltuples where exact counts would be slow.

Idempotent upsert on (layer, table_name). Run after any stage that
materialises new data; cheap enough to run on every e2e tail.

Usage:
  python -m etl4.orchestration.pipeline_volumes
"""
from __future__ import annotations

import os
from pathlib import Path

import pyarrow.parquet as pq
import psycopg

ROOT = Path(__file__).resolve().parents[2]
DSN = os.getenv("POSTGRES_DSN",
                "postgresql://etl4:change_me@localhost:5432/etl4")

DDL = """
CREATE TABLE IF NOT EXISTS pipeline_volumes (
  layer       TEXT        NOT NULL,
  table_name  TEXT        NOT NULL,
  rows        BIGINT      NOT NULL DEFAULT 0,
  bytes       BIGINT      NOT NULL DEFAULT 0,
  files       INT         NOT NULL DEFAULT 1,
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (layer, table_name)
);
CREATE INDEX IF NOT EXISTS idx_pv_layer ON pipeline_volumes(layer);
"""

# layer -> {table_name: Path}
PARQUET_TARGETS = {
    "bronze": {
        "flightlists":  ROOT / "data" / "bronze" / "flightlists",
        "raw_states":   ROOT / "data" / "bronze" / "raw_states",
    },
    "silver_vault": {
        "hub_aircraft":          ROOT / "data" / "silver" / "vault" / "hub_aircraft",
        "hub_airport":           ROOT / "data" / "silver" / "vault" / "hub_airport",
        "hub_flight":            ROOT / "data" / "silver" / "vault" / "hub_flight",
        "link_flight_aircraft":  ROOT / "data" / "silver" / "vault" / "link_flight_aircraft",
        "link_flight_route":     ROOT / "data" / "silver" / "vault" / "link_flight_route",
        "sat_aircraft_identity": ROOT / "data" / "silver" / "vault" / "sat_aircraft_identity",
        "sat_flight_summary":    ROOT / "data" / "silver" / "vault" / "sat_flight_summary",
        "sat_state_observation": ROOT / "data" / "silver" / "vault" / "sat_state_observation",
    },
    "gold_marts": {
        "dim_aircraft":              ROOT / "data" / "gold" / "marts" / "dim_aircraft",
        "dim_airport":               ROOT / "data" / "gold" / "marts" / "dim_airport",
        "fact_flight":               ROOT / "data" / "gold" / "marts" / "fact_flight",
        "fact_trajectory_enriched":  ROOT / "data" / "gold" / "marts" / "fact_trajectory_enriched",
        "fact_experiment_result":    ROOT / "data" / "gold" / "marts" / "fact_experiment_result",
    },
}

POSTGRES_TARGETS = [
    "dim_aircraft", "fact_trajectory_enriched",
    "pipeline_runs", "eval_results", "ingest_runs",
    "fact_experiment_result", "deviation_events",
]


def _scan_parquet_tree(path: Path) -> tuple[int, int, int]:
    """Return (files, rows, bytes) summed from parquet footers under `path`.
    Skips the _shards transient dirs and _DONE sentinels."""
    if not path.exists():
        return 0, 0, 0
    files = [f for f in path.rglob("*.parquet") if "_shards" not in f.parts]
    rows  = sum(pq.ParquetFile(f).metadata.num_rows for f in files)
    bytes_ = sum(f.stat().st_size for f in files)
    return len(files), rows, bytes_


def _postgres_counts(cur) -> dict[str, tuple[int, int]]:
    """{table_name: (rows, bytes)} for each Postgres target. Bytes includes
    toast + index size for an honest storage picture."""
    out: dict[str, tuple[int, int]] = {}
    for t in POSTGRES_TARGETS:
        cur.execute(
            f"SELECT count(*), pg_total_relation_size(%s::regclass) FROM {t}",
            (t,),
        )
        rows, bytes_ = cur.fetchone()
        out[t] = (int(rows), int(bytes_))
    return out


UPSERT = """
INSERT INTO pipeline_volumes (layer, table_name, rows, bytes, files, updated_at)
VALUES (%s, %s, %s, %s, %s, now())
ON CONFLICT (layer, table_name) DO UPDATE
SET rows       = EXCLUDED.rows,
    bytes      = EXCLUDED.bytes,
    files      = EXCLUDED.files,
    updated_at = now()
"""


def main() -> None:
    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        for stmt in DDL.strip().split(";"):
            if stmt.strip():
                cur.execute(stmt)

        total_rows = 0
        for layer, targets in PARQUET_TARGETS.items():
            for name, path in targets.items():
                files, rows, bytes_ = _scan_parquet_tree(path)
                cur.execute(UPSERT, (layer, name, rows, bytes_, files))
                total_rows += rows
                print(f"  [{layer:<14}] {name:<30} files={files:<3} "
                      f"rows={rows:>14,}  bytes={bytes_/1e6:>8.1f} MB",
                      flush=True)

        pg_counts = _postgres_counts(cur)
        for name, (rows, bytes_) in pg_counts.items():
            cur.execute(UPSERT, ("postgres", name, rows, bytes_, 1))
            print(f"  [postgres      ] {name:<30} rows={rows:>14,}  "
                  f"bytes={bytes_/1e6:>8.1f} MB", flush=True)

        print(f"\n[pipeline_volumes] snapshot refreshed; "
              f"parquet total rows={total_rows:,}", flush=True)


if __name__ == "__main__":
    main()
