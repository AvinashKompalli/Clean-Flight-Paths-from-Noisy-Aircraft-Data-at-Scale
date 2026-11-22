"""Row-count reconciliation across bronze -> vault -> marts.

Catches silent row drops introduced by schema drift, join key misses, or
a dedup pass that is too aggressive. Reads parquet footers only (no
dataframe load), so it is O(files) not O(rows) and stays cheap even on
the ~10 GB bronze.

Checks performed:
  1. bronze flightlists row-sum vs vault hub_flight row count
     (hub_flight >= (1 - tol) * bronze_flightlist_rows)
  2. bronze raw_states row-sum vs vault sat_state_observation row count
     (sat_state_observation >= (1 - tol) * bronze_state_rows)
  3. vault sat_state_observation vs mart fact_trajectory_enriched
     (fact_trajectory_enriched <= sat_state_observation AND
      fact_trajectory_enriched >= (1 - tol) * sat_state_observation)

Exit code:
  0  every check within tolerance
  1  one or more checks breached tolerance

Usage:
  python -m etl4.qa.reconcile
  python -m etl4.qa.reconcile --tolerance 0.02
  python -m etl4.qa.reconcile --json out/reconcile.json
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[2]

BRONZE_FLIGHTLISTS   = ROOT / "data" / "bronze" / "flightlists"
BRONZE_RAW_STATES    = ROOT / "data" / "bronze" / "raw_states"
VAULT_HUB_FLIGHT     = ROOT / "data" / "silver" / "vault" / "hub_flight"
VAULT_SAT_STATE_OBS  = ROOT / "data" / "silver" / "vault" / "sat_state_observation"
MART_FACT_TRAJECTORY = ROOT / "data" / "gold" / "marts" / "fact_trajectory_enriched"


def _count_rows(path: Path) -> int:
    """Row count under `path`. Reads parquet footer metadata only."""
    if not path.exists():
        return 0
    files = [path] if path.is_file() else sorted(path.rglob("*.parquet"))
    return sum(pq.ParquetFile(f).metadata.num_rows for f in files)


def _check(label: str, upstream_rows: int, downstream_rows: int,
           tolerance: float, require_shrink: bool = False) -> dict:
    """One reconciliation row.

    upstream_rows:   count at the earlier layer.
    downstream_rows: count at the later layer.
    tolerance:       max fractional drop allowed (0.01 == 1%).
    require_shrink:  if True, downstream must also be <= upstream
                     (catches duplication that inflates a later layer).
    """
    if upstream_rows == 0:
        return {"label": label, "upstream": 0, "downstream": downstream_rows,
                "drop_pct": None, "status": "skip",
                "note": "upstream has no rows - nothing to compare"}
    drop = (upstream_rows - downstream_rows) / upstream_rows
    passed = drop <= tolerance
    if require_shrink and downstream_rows > upstream_rows:
        passed = False
    status = "ok" if passed else "fail"
    return {"label": label,
            "upstream": upstream_rows,
            "downstream": downstream_rows,
            "drop_pct": round(drop * 100.0, 3),
            "tolerance_pct": round(tolerance * 100.0, 3),
            "status": status}


def reconcile(tolerance: float) -> list[dict]:
    bronze_fl   = _count_rows(BRONZE_FLIGHTLISTS)
    bronze_st   = _count_rows(BRONZE_RAW_STATES)
    hub_flight  = _count_rows(VAULT_HUB_FLIGHT)
    sat_obs     = _count_rows(VAULT_SAT_STATE_OBS)
    fact_traj   = _count_rows(MART_FACT_TRAJECTORY)

    return [
        _check("bronze_flightlists -> vault.hub_flight",
               bronze_fl, hub_flight, tolerance),
        _check("bronze_raw_states -> vault.sat_state_observation",
               bronze_st, sat_obs, tolerance),
        _check("vault.sat_state_observation -> mart.fact_trajectory_enriched",
               sat_obs, fact_traj, tolerance, require_shrink=True),
    ]


def _print_row(r: dict) -> None:
    label = r["label"]
    up    = r["upstream"]
    down  = r["downstream"]
    if r["status"] == "skip":
        print(f"[reconcile] SKIP  {label}  ({r['note']})", flush=True)
        return
    mark = "OK  " if r["status"] == "ok" else "FAIL"
    print(f"[reconcile] [{mark}] {label}  "
          f"upstream={up:,}  downstream={down:,}  "
          f"drop={r['drop_pct']}%  tol={r['tolerance_pct']}%", flush=True)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--tolerance", type=float, default=0.01,
                   help="max fractional drop at any step (default: 0.01 = 1%%)")
    p.add_argument("--json", default=None, help="write report to this path")
    a = p.parse_args()

    if a.tolerance < 0.0 or a.tolerance > 1.0:
        sys.exit(f"[reconcile] tolerance must be in [0, 1], got {a.tolerance}")

    results = reconcile(a.tolerance)

    print("[reconcile] ---- row-count reconciliation ----", flush=True)
    for r in results:
        _print_row(r)

    any_failed = any(r["status"] == "fail" for r in results)
    print(f"[reconcile] summary: "
          f"{sum(1 for r in results if r['status'] == 'ok')} ok, "
          f"{sum(1 for r in results if r['status'] == 'fail')} fail, "
          f"{sum(1 for r in results if r['status'] == 'skip')} skip",
          flush=True)

    if a.json:
        Path(a.json).write_text(json.dumps(results, indent=2))
        print(f"[reconcile] report written: {a.json}", flush=True)

    sys.exit(1 if any_failed else 0)


if __name__ == "__main__":
    main()
