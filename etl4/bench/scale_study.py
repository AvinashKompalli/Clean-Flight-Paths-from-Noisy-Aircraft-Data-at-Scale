"""Scale-study CLI: measure how each reconstruction method scales with volume.

For each (method, volume) cell it samples real bronze state-vector parquets to
exactly ``volume`` rows (deterministic, seed=42), runs the method, and records
wall-time, rows/sec, and peak RSS in Postgres ``scale_runs``.

Boundary of what is timed: only ``kinematic_features + METHODS[method]`` runs
inside the perf_counter/RSS-sampling window. Loading parquet, ``project_raw``,
``clean``, and ``event_time_order`` are setup and excluded.

PO gate (architecture.md T-005): before launching ``kinematic_smoothing`` at a
given volume, linearly extrapolate peak RSS from the most recent prior row for
that method. If the projection exceeds 12000 MB, insert a NULL row tagged
``notes='skipped: projected_peak_rss_mb>12000'`` and move on.

Usage:
  python -m etl4.bench.scale_study [--volumes 100000,1000000,10000000,50000000]
                                   [--methods time_gap,kinematic,kinematic_smoothing]
                                   [--bronze data/bronze/raw_states]
                                   [--dsn postgresql://...] [--dry-run]
"""
from __future__ import annotations

import argparse
import os
import sys
import threading
import time
from pathlib import Path
from typing import Optional

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from etl4.clean import clean, event_time_order, kinematic_features
from etl4.clean.clean import project_raw
from etl4.reconstruct import (
    segment_time_gap,
    segment_kinematic,
    smooth_and_segment,
)

METHODS = {
    "time_gap": segment_time_gap,
    "kinematic": segment_kinematic,
    "kinematic_smoothing": smooth_and_segment,
}

DEFAULT_VOLUMES = [100_000, 1_000_000, 10_000_000, 50_000_000]
DEFAULT_METHODS = ["time_gap", "kinematic", "kinematic_smoothing"]
DEFAULT_BRONZE = "data/bronze/raw_states"

SEED = 42
RSS_LIMIT_MB = 12000
SMOOTHING_GATED_VOLUME = 50_000_000

_DDL = """
CREATE TABLE IF NOT EXISTS scale_runs (
  scale_run_id       BIGSERIAL PRIMARY KEY,
  recorded_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  data_volume_rows   BIGINT NOT NULL,
  source             TEXT NOT NULL,
  method             TEXT NOT NULL,
  noise              TEXT NOT NULL,
  wall_secs          DOUBLE PRECISION,
  rows_per_sec       DOUBLE PRECISION,
  peak_rss_mb        BIGINT,
  shuffle_mb         BIGINT,
  engine             TEXT NOT NULL,
  seed               INT NOT NULL DEFAULT 42,
  notes              TEXT
);
CREATE INDEX IF NOT EXISTS idx_scale_runs_volume ON scale_runs(data_volume_rows);
CREATE INDEX IF NOT EXISTS idx_scale_runs_method ON scale_runs(method);
"""

_INSERT = """
INSERT INTO scale_runs
    (data_volume_rows, source, method, noise,
     wall_secs, rows_per_sec, peak_rss_mb, shuffle_mb,
     engine, seed, notes)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
RETURNING scale_run_id
"""


def _parse_csv_int(s: str) -> list[int]:
    return [int(x) for x in s.split(",") if x.strip()]


def _parse_csv_str(s: str) -> list[str]:
    return [x.strip() for x in s.split(",") if x.strip()]


def _load_bronze(bronze_dir: Path) -> pd.DataFrame:
    """Concat every parquet under the bronze tree.

    Follows CLAUDE.md section 7: never ``pd.read_parquet`` a partition tree;
    read individual files then concat.
    """
    files = sorted(bronze_dir.rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"no parquet files under {bronze_dir}")
    frames = [pd.read_parquet(f) for f in files]
    return pd.concat(frames, ignore_index=True)


def _sample_rows(base: pd.DataFrame, volume: int) -> tuple[pd.DataFrame, Optional[str]]:
    """Downsample ``base`` to exactly ``volume`` rows, deterministic by seed.

    Returns (sample, cap_note). ``cap_note`` is non-None only when the tree is
    smaller than the request; the scale curve flattens there and the note
    surfaces that in Postgres.
    """
    n = len(base)
    if volume >= n:
        return base.reset_index(drop=True), f"capped: requested={volume} actual={n}"
    sample = base.sample(n=volume, random_state=SEED).reset_index(drop=True)
    return sample, None


def _fetch_prior(cursor, method: str, target_volume: int) -> Optional[dict]:
    """Most recent completed cell for ``method`` at a strictly smaller volume.

    NULL peak_rss_mb rows (previously skipped) are excluded so the projection
    uses actual measurement, not a propagated NULL.
    """
    cursor.execute(
        """
        SELECT data_volume_rows, peak_rss_mb
          FROM scale_runs
         WHERE method = %s
           AND data_volume_rows < %s
           AND peak_rss_mb IS NOT NULL
         ORDER BY recorded_at DESC
         LIMIT 1
        """,
        (method, target_volume),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    return {"data_volume_rows": int(row[0]), "peak_rss_mb": int(row[1])}


def _projection_exceeds(
    prior: Optional[dict], target_volume: int, limit_mb: int = RSS_LIMIT_MB
) -> tuple[bool, Optional[str]]:
    """Pure skip-gate evaluator; exposed for the T-005 smoke test."""
    if prior is None:
        return False, None
    prior_rss = prior.get("peak_rss_mb")
    prior_vol = prior.get("data_volume_rows")
    if not prior_rss or not prior_vol or prior_vol <= 0:
        return False, None
    projected = prior_rss * (target_volume / prior_vol)
    if projected > limit_mb:
        return True, f"skipped: projected_peak_rss_mb>{limit_mb}"
    return False, None


def _run_method_with_rss(method: str, silver: pd.DataFrame) -> tuple[float, int, int]:
    """Run ``kinematic_features + METHODS[method]`` with wall-time and peak-RSS.

    Returns (wall_secs, peak_rss_mb, rows_out). The RSS sampler ticks every
    500 ms on a daemon thread; a sample is taken before start and after join
    so even sub-500 ms cells never record 0.
    """
    try:
        import psutil
    except ImportError as e:
        raise RuntimeError("psutil required for scale_study; pin in requirements.txt") from e

    proc = psutil.Process()
    samples: list[int] = [proc.memory_info().rss]
    stop = threading.Event()

    def _sampler():
        while not stop.wait(0.5):
            samples.append(proc.memory_info().rss)

    t = threading.Thread(target=_sampler, daemon=True)
    t.start()
    try:
        t0 = time.perf_counter()
        feat = kinematic_features(silver)
        gold = METHODS[method](feat)
        wall_s = time.perf_counter() - t0
    finally:
        stop.set()
        t.join(timeout=1.0)
        samples.append(proc.memory_info().rss)

    peak_rss_mb = max(samples) // (1024 * 1024)
    return wall_s, int(peak_rss_mb), int(len(gold))


def _prepare_silver(sample: pd.DataFrame, source: str) -> pd.DataFrame:
    df = project_raw(sample, source=source)
    df, _rejected = clean(df)
    df = event_time_order(df)
    return df


def _insert_row(
    cursor,
    *,
    volume: int,
    source: str,
    method: str,
    wall_secs: Optional[float],
    rows_per_sec: Optional[float],
    peak_rss_mb: Optional[int],
    notes: Optional[str],
) -> int:
    cursor.execute(
        _INSERT,
        (
            int(volume),
            source,
            method,
            "N0",
            wall_secs,
            rows_per_sec,
            peak_rss_mb,
            None,
            "pandas",
            SEED,
            notes,
        ),
    )
    return int(cursor.fetchone()[0])


def _print_status(method: str, volume: int, wall_s: Optional[float],
                  rss_mb: Optional[int], status: str) -> None:
    wall = f"{wall_s:.3f}" if wall_s is not None else "NA"
    rss = f"{rss_mb}" if rss_mb is not None else "NA"
    print(
        f"[scale_study] method={method} volume={volume} "
        f"wall_s={wall} rss_mb={rss} status={status}",
        flush=True,
    )


def _print_plan(volumes: list[int], methods: list[str], bronze: Path) -> None:
    n = len(volumes) * len(methods)
    print(
        f"[scale_study] dry-run plan: {n} cells "
        f"({len(volumes)} volumes x {len(methods)} methods)",
        flush=True,
    )
    print(f"[scale_study] bronze={bronze}", flush=True)
    for m in methods:
        for v in volumes:
            print(f"[scale_study]   method={m} volume={v}", flush=True)


def run(
    volumes: list[int],
    methods: list[str],
    bronze_dir: Path,
    dsn: str,
    source: str = "s3",
) -> list[dict]:
    """Execute the full matrix against Postgres. Returns per-cell summaries."""
    import psycopg

    results: list[dict] = []
    base: Optional[pd.DataFrame] = None

    with psycopg.connect(dsn, connect_timeout=5) as conn:
        with conn.cursor() as cur:
            cur.execute(_DDL)
        conn.commit()

        for method in methods:
            for volume in volumes:
                with conn.cursor() as cur:
                    if method == "kinematic_smoothing" and volume >= SMOOTHING_GATED_VOLUME:
                        prior = _fetch_prior(cur, method, volume)
                        skip, note = _projection_exceeds(prior, volume)
                        if skip:
                            _insert_row(
                                cur,
                                volume=volume,
                                source=source,
                                method=method,
                                wall_secs=None,
                                rows_per_sec=None,
                                peak_rss_mb=None,
                                notes=note,
                            )
                            conn.commit()
                            _print_status(method, volume, None, None, "skipped")
                            results.append({"method": method, "volume": volume, "status": "skipped"})
                            continue

                if base is None:
                    base = _load_bronze(bronze_dir)

                sample, cap_note = _sample_rows(base, volume)
                silver = _prepare_silver(sample, source)
                if "icao24" in silver.columns and silver["icao24"].dtype != "string":
                    silver["icao24"] = silver["icao24"].astype("string")

                rows_in = int(len(silver))
                wall_s, peak_rss_mb, rows_out = _run_method_with_rss(method, silver)
                rows_per_sec = (rows_in / wall_s) if wall_s > 0 else None

                with conn.cursor() as cur:
                    _insert_row(
                        cur,
                        volume=volume,
                        source=source,
                        method=method,
                        wall_secs=wall_s,
                        rows_per_sec=rows_per_sec,
                        peak_rss_mb=peak_rss_mb,
                        notes=cap_note,
                    )
                conn.commit()
                _print_status(method, volume, wall_s, peak_rss_mb, "ok")
                results.append({
                    "method": method,
                    "volume": volume,
                    "status": "ok",
                    "wall_s": wall_s,
                    "peak_rss_mb": peak_rss_mb,
                    "rows_out": rows_out,
                })

    return results


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(description="ETL4 reconstruction-method scale study")
    p.add_argument(
        "--volumes",
        type=_parse_csv_int,
        default=DEFAULT_VOLUMES,
        help="comma-separated row volumes, e.g. 100000,1000000",
    )
    p.add_argument(
        "--methods",
        type=_parse_csv_str,
        default=DEFAULT_METHODS,
        help="comma-separated methods: time_gap,kinematic,kinematic_smoothing",
    )
    p.add_argument("--bronze", default=DEFAULT_BRONZE,
                   help="bronze state-vector tree (parquet)")
    p.add_argument("--source", default="s3", choices=["s3", "rest", "synthetic"],
                   help="project_raw source for bronze schema (default: s3)")
    p.add_argument(
        "--dsn",
        default=os.getenv("POSTGRES_DSN", "postgresql://etl4:change_me@localhost:5432/etl4"),
        help="Postgres DSN (reads POSTGRES_DSN env var when unset)",
    )
    p.add_argument("--dry-run", action="store_true",
                   help="print the plan and exit without touching parquet or Postgres")
    args = p.parse_args(argv)

    bronze = Path(args.bronze)
    unknown = [m for m in args.methods if m not in METHODS]
    if unknown:
        print(f"[scale_study] unknown method(s): {unknown}", flush=True)
        return 2

    if args.dry_run:
        _print_plan(args.volumes, args.methods, bronze)
        return 0

    run(args.volumes, args.methods, bronze, args.dsn, source=args.source)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
