"""Full Data Vault 2.0 build: bronze flightlists -> raw vault -> marts.

Three stages, each a self-contained module:

  1. etl4.vault.bronze_flightlists  Zenodo .csv.gz -> bronze parquet
  2. etl4.vault.raw_vault           bronze -> hubs/links/satellites
  3. etl4.vault.marts               raw vault -> dim/fact marts

Usage:
  python etl4/vault/run_vault.py
  python etl4/vault/run_vault.py --no-states              # faster dev loop
  python etl4/vault/run_vault.py --skip bronze_flightlists --skip raw_vault
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from etl4.vault.bronze_flightlists import convert_tree
from etl4.vault.raw_vault import build_raw_vault
from etl4.vault.marts import build_marts


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--skip", action="append", default=[],
                   choices=["bronze_flightlists", "raw_vault", "marts"],
                   help="stage to skip (can pass multiple times)")
    p.add_argument("--no-states", action="store_true",
                   help="raw_vault: skip sat_state_observation")
    a = p.parse_args()

    overall = time.perf_counter()

    if "bronze_flightlists" not in a.skip:
        print("=" * 60)
        print("STAGE 1: bronze_flightlists")
        print("=" * 60)
        t0 = time.perf_counter()
        convert_tree()
        print(f"  stage 1 wall: {time.perf_counter() - t0:.1f}s\n")

    if "raw_vault" not in a.skip:
        print("=" * 60)
        print("STAGE 2: raw_vault")
        print("=" * 60)
        t0 = time.perf_counter()
        build_raw_vault(include_states=not a.no_states)
        print(f"  stage 2 wall: {time.perf_counter() - t0:.1f}s\n")

    if "marts" not in a.skip:
        print("=" * 60)
        print("STAGE 3: marts")
        print("=" * 60)
        t0 = time.perf_counter()
        build_marts()
        print(f"  stage 3 wall: {time.perf_counter() - t0:.1f}s\n")

    print(f"\ntotal wall: {time.perf_counter() - overall:.1f}s")


if __name__ == "__main__":
    main()
