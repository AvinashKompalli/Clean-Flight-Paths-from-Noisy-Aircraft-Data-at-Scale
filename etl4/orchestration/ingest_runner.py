"""Thin CLI wrapper that runs one ingest path and records telemetry.

Keeps existing ingest scripts unchanged — this wrapper owns the measurement
so every path emits the same metric shape to the `ingest_runs` table.

Real sources only. synthetic.py stays on disk for unit-test fixtures but
is not exposed as an orchestration source. The OpenSky S3 `data-samples`
bucket had its `states/` prefix emptied by OpenSky in late 2026, so the
S3 batch path was retired - live REST (`live_producer`) is the streaming
secondary now, with Zenodo as the batch primary.

Usage:
  python -m etl4.orchestration.ingest_runner --source zenodo --latest
  python -m etl4.orchestration.ingest_runner --source live   --limit 2000
  python -m etl4.orchestration.ingest_runner --source replay --speed max --limit 50000
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from etl4.orchestration.ingest_metrics import ingest_run

ROOT = Path(__file__).resolve().parents[2]


def _du_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        return path.stat().st_size
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())


def _rows_in_tree(path: Path) -> int:
    """Row count across all parquet files under `path`. Schema-agnostic —
    reads only footer metadata, no column load."""
    if not path.exists():
        return 0
    import pyarrow.parquet as pq
    files = [path] if path.is_file() else sorted(path.rglob("*.parquet"))
    return sum(pq.ParquetFile(f).metadata.num_rows for f in files)


def _run(cmd: list[str]) -> int:
    print("exec:", " ".join(cmd), flush=True)
    return subprocess.call(cmd, cwd=str(ROOT))


def _live(limit: int, interval: float) -> dict:
    """Poll the OpenSky REST API and stream `limit` records through Kafka.

    OAuth2 credentials read from env by live_producer itself (OPENSKY_CLIENT_ID /
    OPENSKY_CLIENT_SECRET): with creds -> 4000 credits/day, otherwise anonymous
    at 400/day. Interval is the poll period in seconds; 5s with creds, 10s
    without. Bytes_out is left at 0 because the producer owns the writes and
    we only measure the record count on exit.
    """
    cmd = [sys.executable, "etl4/ingest/live_producer.py",
           "--limit", str(limit), "--interval", str(interval)]
    if _run(cmd):
        raise RuntimeError("live_producer exited non-zero")
    return {"rows_out": limit, "bytes_out": 0}


def _zenodo(latest: bool) -> dict:
    raw   = ROOT / "data" / "raw"    / "opensky_flightlists"
    bronze = ROOT / "data" / "bronze" / "flightlists"
    raw_before    = _du_bytes(raw)
    bronze_before = _du_bytes(bronze)
    cmd = [sys.executable, "etl4/screpper.py", "--repo", "zenodo", "--download"]
    if latest:
        cmd += ["--latest"]
    if _run(cmd):
        raise RuntimeError("screpper zenodo download failed")
    if _run([sys.executable, "-m", "etl4.vault.bronze_flightlists"]):
        raise RuntimeError("bronze_flightlists conversion failed")
    return {"rows_out":  _rows_in_tree(bronze),
            "bytes_out": _du_bytes(raw) - raw_before}


def _replay(speed: str, limit: int) -> dict:
    cmd = [sys.executable, "etl4/ingest/replay_producer.py",
           "--speed", speed, "--limit", str(limit)]
    if _run(cmd):
        raise RuntimeError("replay producer exited non-zero")
    return {"rows_out": limit, "bytes_out": 0}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--source", required=True,
                   choices=["zenodo", "live", "replay"])
    p.add_argument("--latest", action="store_true")
    p.add_argument("--speed", default="max")
    p.add_argument("--limit", type=int, default=50000)
    p.add_argument("--interval", type=float, default=5.0,
                   help="live poll period (s); 5 with OAuth creds, 10 without")
    a = p.parse_args()

    pattern = {"zenodo": "batch", "live": "stream", "replay": "stream"}[a.source]

    with ingest_run(a.source, pattern,
                    extras={"speed": a.speed, "limit": a.limit,
                            "interval": a.interval}) as rec:
        if a.source == "zenodo":
            out = _zenodo(a.latest)
        elif a.source == "live":
            out = _live(a.limit, a.interval)
        else:
            out = _replay(a.speed, a.limit)
        rec["rows_out"]  = out["rows_out"]
        rec["bytes_out"] = out["bytes_out"]

    print(f"[ingest_runner] {a.source} done rows={rec['rows_out']} "
          f"bytes={rec['bytes_out']}", flush=True)


if __name__ == "__main__":
    main()
