"""T-15 smoke: cross-month reader rule in `marts._latest_sfs`.

Covers acceptance for T-15 (architecture.md section 5, PR3; ADR 2026-11-13
Risk 1): the reducer that picks the latest sat_flight_summary row per
`hk_flight` must prefer the later `lastseen` and only fall back to
`load_dt DESC` on a `lastseen` tie.

Unit-level: exercises `marts._latest_sfs` directly on hand-built DataFrames
whose dtypes mirror `raw_vault.build_sat_flight_summary` (lastseen /
firstseen -> datetime64[ns, UTC]; load_dt -> datetime). No Postgres, no
filesystem fixtures.

Scenarios:
  (a) Feb row has the later load_dt as well as the later lastseen -> Feb wins.
  (b) Jan row has the later load_dt (ingest rerun), Feb has the later
      lastseen -> Feb still wins. This is the case the pre-T-15 reducer
      would have miscalled.
  (c) Both rows share identical lastseen -> later load_dt wins (tiebreak).
  (d) Second `hk_flight` in the same frame survives dedup untouched.
  (e) NA lastseen loses to a real one.

Prints `LATEST_SFS SMOKE OK` when all assertions pass. Windows cp1252-safe
(no arrows, no em-dashes).

Invoke:
    python multi-agent-engineering/projects/PROJ-20261118-impr01-partition-contract/artifacts/t15_latest_sfs_smoke.py
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO_ROOT))

import pandas as pd

from etl4.vault import marts


HK_A = "a" * 32
HK_B = "b" * 32


def _sfs_frame(rows: list[dict]) -> pd.DataFrame:
    """Build a sat_flight_summary-shaped frame with dtypes matching
    raw_vault.build_sat_flight_summary (lastseen/firstseen UTC-aware)."""
    df = pd.DataFrame(rows)
    df["firstseen"] = pd.to_datetime(df["firstseen"], utc=True)
    df["lastseen"]  = pd.to_datetime(df["lastseen"],  utc=True)
    return df


def _assert_unique(label: str, out: pd.DataFrame) -> None:
    assert out["hk_flight"].is_unique, (
        f"{label}: reducer returned duplicates for hk_flight"
    )


def _pick(out: pd.DataFrame, hk: str) -> pd.Series:
    row = out.loc[out["hk_flight"] == hk]
    assert len(row) == 1, f"expected 1 row for {hk}, got {len(row)}"
    return row.iloc[0]


def case_a_feb_wins_by_lastseen_and_load_dt() -> None:
    frame = _sfs_frame([
        {"hk_flight": HK_A,
         "firstseen": "2019-01-31 23:30:00+00:00",
         "lastseen":  "2019-01-31 23:50:00+00:00",
         "duration_s": 1200.0, "altitude_1": 100.0, "altitude_2": 200.0,
         "load_dt": datetime(2019, 2, 1, 1, 0, tzinfo=timezone.utc),
         "rec_src": "opensky_zenodo"},
        {"hk_flight": HK_A,
         "firstseen": "2019-01-31 23:30:00+00:00",
         "lastseen":  "2019-02-01 00:10:00+00:00",
         "duration_s": 2400.0, "altitude_1": 100.0, "altitude_2": 250.0,
         "load_dt": datetime(2019, 3, 1, 1, 0, tzinfo=timezone.utc),
         "rec_src": "opensky_zenodo"},
    ])
    out = marts._latest_sfs(frame)
    _assert_unique("case a", out)
    picked = _pick(out, HK_A)
    assert picked["lastseen"] == pd.Timestamp("2019-02-01 00:10:00+00:00"), (
        f"case a: expected Feb lastseen, got {picked['lastseen']!r}"
    )
    assert picked["altitude_2"] == 250.0, (
        f"case a: expected end-of-flight altitude_2=250.0, got {picked['altitude_2']}"
    )
    print("  case (a) OK: later lastseen + later load_dt -> Feb row wins",
          flush=True)


def case_b_feb_wins_despite_older_load_dt() -> None:
    frame = _sfs_frame([
        {"hk_flight": HK_A,
         "firstseen": "2019-01-31 23:30:00+00:00",
         "lastseen":  "2019-01-31 23:50:00+00:00",
         "duration_s": 1200.0, "altitude_1": 100.0, "altitude_2": 200.0,
         "load_dt": datetime(2019, 3, 1, 1, 0, tzinfo=timezone.utc),
         "rec_src": "opensky_zenodo"},
        {"hk_flight": HK_A,
         "firstseen": "2019-01-31 23:30:00+00:00",
         "lastseen":  "2019-02-01 00:10:00+00:00",
         "duration_s": 2400.0, "altitude_1": 100.0, "altitude_2": 250.0,
         "load_dt": datetime(2019, 2, 1, 1, 0, tzinfo=timezone.utc),
         "rec_src": "opensky_zenodo"},
    ])
    out = marts._latest_sfs(frame)
    _assert_unique("case b", out)
    picked = _pick(out, HK_A)
    assert picked["lastseen"] == pd.Timestamp("2019-02-01 00:10:00+00:00"), (
        f"case b (cross-month rerun): expected Feb lastseen even with older "
        f"load_dt on Feb row, got {picked['lastseen']!r}. "
        "Pre-T-15 reducer would have picked the Jan row by load_dt."
    )
    assert picked["altitude_2"] == 250.0
    print("  case (b) OK: Feb wins even when Jan has later load_dt "
          "(the pre-T-15 miscall)", flush=True)


def case_c_tiebreak_on_load_dt() -> None:
    frame = _sfs_frame([
        {"hk_flight": HK_A,
         "firstseen": "2019-01-15 10:00:00+00:00",
         "lastseen":  "2019-01-15 12:00:00+00:00",
         "duration_s": 7200.0, "altitude_1": 100.0, "altitude_2": 300.0,
         "load_dt": datetime(2019, 2, 1, 1, 0, tzinfo=timezone.utc),
         "rec_src": "opensky_zenodo"},
        {"hk_flight": HK_A,
         "firstseen": "2019-01-15 10:00:00+00:00",
         "lastseen":  "2019-01-15 12:00:00+00:00",
         "duration_s": 7200.0, "altitude_1": 100.0, "altitude_2": 350.0,
         "load_dt": datetime(2019, 3, 1, 1, 0, tzinfo=timezone.utc),
         "rec_src": "opensky_zenodo"},
    ])
    out = marts._latest_sfs(frame)
    _assert_unique("case c", out)
    picked = _pick(out, HK_A)
    assert picked["altitude_2"] == 350.0, (
        f"case c: identical lastseen -> later load_dt should win; "
        f"expected altitude_2=350.0, got {picked['altitude_2']}"
    )
    print("  case (c) OK: identical lastseen -> later load_dt wins",
          flush=True)


def case_d_second_hk_survives() -> None:
    frame = _sfs_frame([
        {"hk_flight": HK_A,
         "firstseen": "2019-01-31 23:30:00+00:00",
         "lastseen":  "2019-01-31 23:50:00+00:00",
         "duration_s": 1200.0, "altitude_1": 100.0, "altitude_2": 200.0,
         "load_dt": datetime(2019, 2, 1, 1, 0, tzinfo=timezone.utc),
         "rec_src": "opensky_zenodo"},
        {"hk_flight": HK_A,
         "firstseen": "2019-01-31 23:30:00+00:00",
         "lastseen":  "2019-02-01 00:10:00+00:00",
         "duration_s": 2400.0, "altitude_1": 100.0, "altitude_2": 250.0,
         "load_dt": datetime(2019, 2, 1, 1, 0, tzinfo=timezone.utc),
         "rec_src": "opensky_zenodo"},
        {"hk_flight": HK_B,
         "firstseen": "2019-01-10 08:00:00+00:00",
         "lastseen":  "2019-01-10 09:30:00+00:00",
         "duration_s": 5400.0, "altitude_1": 50.0, "altitude_2": 60.0,
         "load_dt": datetime(2019, 2, 1, 1, 0, tzinfo=timezone.utc),
         "rec_src": "opensky_zenodo"},
    ])
    out = marts._latest_sfs(frame)
    _assert_unique("case d", out)
    assert set(out["hk_flight"]) == {HK_A, HK_B}, (
        f"case d: expected both hk_flights to survive, got "
        f"{sorted(out['hk_flight'].unique())}"
    )
    assert len(out) == 2
    print("  case (d) OK: a second hk_flight passes through untouched",
          flush=True)


def case_e_na_lastseen_loses() -> None:
    frame = _sfs_frame([
        {"hk_flight": HK_A,
         "firstseen": "2019-01-15 10:00:00+00:00",
         "lastseen":  None,
         "duration_s": None, "altitude_1": 100.0, "altitude_2": 300.0,
         "load_dt": datetime(2019, 3, 1, 1, 0, tzinfo=timezone.utc),
         "rec_src": "opensky_zenodo"},
        {"hk_flight": HK_A,
         "firstseen": "2019-01-15 10:00:00+00:00",
         "lastseen":  "2019-01-15 12:00:00+00:00",
         "duration_s": 7200.0, "altitude_1": 100.0, "altitude_2": 350.0,
         "load_dt": datetime(2019, 2, 1, 1, 0, tzinfo=timezone.utc),
         "rec_src": "opensky_zenodo"},
    ])
    out = marts._latest_sfs(frame)
    _assert_unique("case e", out)
    picked = _pick(out, HK_A)
    assert not pd.isna(picked["lastseen"]), (
        "case e: NA lastseen should lose to a real one; "
        "the reducer kept the NA row"
    )
    assert picked["altitude_2"] == 350.0
    print("  case (e) OK: NA lastseen loses to a real one", flush=True)


def case_empty() -> None:
    empty = pd.DataFrame(columns=[
        "hk_flight", "firstseen", "lastseen", "duration_s",
        "altitude_1", "altitude_2", "load_dt", "rec_src",
    ])
    out = marts._latest_sfs(empty)
    assert out.empty
    print("  empty-input OK: reducer returns empty frame", flush=True)


def run() -> None:
    case_empty()
    case_a_feb_wins_by_lastseen_and_load_dt()
    case_b_feb_wins_despite_older_load_dt()
    case_c_tiebreak_on_load_dt()
    case_d_second_hk_survives()
    case_e_na_lastseen_loses()
    print("LATEST_SFS SMOKE OK", flush=True)


if __name__ == "__main__":
    try:
        run()
    except AssertionError as e:
        print(f"SMOKE FAIL: {e}", flush=True)
        sys.exit(1)
