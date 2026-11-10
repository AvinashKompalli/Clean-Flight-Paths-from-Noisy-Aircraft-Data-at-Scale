"""Thin CLI wrapper that runs one ingest path and records telemetry.

Keeps existing ingest scripts unchanged — this wrapper owns the measurement
so every path emits the same metric shape to the `ingest_runs` table.

Usage:
  python -m etl4.orchestration.ingest_runner --source s3       --date 2022-06-27
  python -m etl4.orchestration.ingest_runner --source zenodo   --latest
  python -m etl4.orchestration.ingest_runner --source synthetic --date 2022-06-27
  python -m etl4.orchestration.ingest_runner --source replay   --speed max --limit 50000
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
    if not path.exists():
        return 0
    import pandas as pd
    files = [path] if path.is_file() else sorted(path.rglob("*.parquet"))
    return sum(len(pd.read_parquet(f, columns=["time"])) for f in files)


def _run(cmd: list[str]) -> int:
    print("exec:", " ".join(cmd), flush=True)
    return subprocess.call(cmd, cwd=str(ROOT))


def _s3(date: str) -> dict:
    out = ROOT / "data" / "bronze" / "raw_states" / f"date={date}"
    before = _du_bytes(out)
    rc = _run([sys.executable, "etl4/screpper.py",
               "--repo", "opensky",
               "--start-date", date, "--end-date", date])
    if rc:
        raise SystemExit(rc)
    return {"rows_out": _rows_in_tree(out), "bytes_out": _du_bytes(out) - before}


def _zenodo(latest: bool) -> dict:
    out = ROOT / "data" / "bronze" / "flightlists"
    before = _du_bytes(out)
    cmd = [sys.executable, "etl4/screpper.py", "--repo", "zenodo"]
    if latest:
        cmd += ["--latest"]
    if _run(cmd):
        raise SystemExit(1)
    return {"rows_out": _rows_in_tree(out), "bytes_out": _du_bytes(out) - before}


def _synthetic(date: str) -> dict:
    out = ROOT / "data" / "bronze" / "raw_states" / f"date={date}"
    before = _du_bytes(out)
    if _run([sys.executable, "etl4/ingest/synthetic.py", "--date", date]):
        raise SystemExit(1)
    return {"rows_out": _rows_in_tree(out), "bytes_out": _du_bytes(out) - before}


def _replay(speed: str, limit: int) -> dict:
    cmd = [sys.executable, "etl4/ingest/replay_producer.py",
           "--speed", speed, "--limit", str(limit)]
    if _run(cmd):
        raise SystemExit(1)
    return {"rows_out": limit, "bytes_out": 0}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--source", required=True,
                   choices=["s3", "zenodo", "synthetic", "replay"])
    p.add_argument("--date")
    p.add_argument("--latest", action="store_true")
    p.add_argument("--speed", default="max")
    p.add_argument("--limit", type=int, default=50000)
    a = p.parse_args()

    pattern = {"s3": "batch", "zenodo": "batch",
               "synthetic": "generative", "replay": "stream"}[a.source]

    with ingest_run(a.source, pattern,
                    extras={"date": a.date, "speed": a.speed,
                            "limit": a.limit}) as rec:
        if a.source == "s3":
            out = _s3(a.date)
        elif a.source == "zenodo":
            out = _zenodo(a.latest)
        elif a.source == "synthetic":
            out = _synthetic(a.date)
        else:
            out = _replay(a.speed, a.limit)
        rec["rows_out"]  = out["rows_out"]
        rec["bytes_out"] = out["bytes_out"]

    print(f"[ingest_runner] {a.source} done rows={rec['rows_out']} "
          f"bytes={rec['bytes_out']}", flush=True)


if __name__ == "__main__":
    main()
