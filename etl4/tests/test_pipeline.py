"""End-to-end pipeline tests using the synthetic generator as the data source."""
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

from etl4.bench.run_experiment import run_experiment
from etl4.clean import clean, event_time_order, kinematic_features
from etl4.clean.clean import project_raw
from etl4.reconstruct import (
    segment_time_gap, segment_kinematic, smooth_and_segment,
    detect_deviations, correct_trajectory,
    great_circle_route, cross_track_distance_m,
)
from etl4.evaluate import (
    position_rmse_m, segmentation_prf, continuity_ratio,
)

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def synth_bronze(tmp_path_factory):
    """Generate one hour of synthetic data once per test module."""
    out = tmp_path_factory.mktemp("bronze")
    subprocess.run(
        [sys.executable, str(ROOT / "etl4" / "ingest" / "synthetic.py"),
         "--hours", "1", "--date", "2026-01-01", "--out", str(out)],
        check=True,
    )
    f = out / "date=2026-01-01" / "hour=0" / "part-0.parquet"
    return pd.read_parquet(f)


# --- clean ----------------------------------------------------------------

def test_clean_removes_null_positions(synth_bronze):
    df = synth_bronze.copy()
    df.loc[df.index[:5], "lat"] = None
    projected = project_raw(df, source="synthetic")
    cleaned, rejected = clean(projected)
    assert (rejected["reason"] == "null_key_or_position").sum() >= 5
    assert cleaned["lat"].notna().all()


def test_clean_drops_exact_duplicates(synth_bronze):
    df = project_raw(synth_bronze, source="synthetic")
    doubled = pd.concat([df, df.sample(50, random_state=0)], ignore_index=True)
    cleaned, _ = clean(doubled)
    assert len(cleaned) == len(df.drop_duplicates(subset=["icao24", "time", "lat", "lon"]))


def test_clean_range_check_rejects_bad_lat():
    df = pd.DataFrame({
        "icao24": ["abc123"], "time": [1_000_000], "lat": [95.0], "lon": [10.0],
        "velocity": [200.0], "heading": [0.0], "vertrate": [0.0],
        "callsign": ["TST001"], "onground": [False], "alert": [False], "spi": [False],
        "squawk": ["1200"], "baroaltitude": [10000.0], "geoaltitude": [10030.0],
        "lastposupdate": [1_000_000.0], "lastcontact": [1_000_000.0], "hour": [1_000_000],
    })
    _, rejected = clean(df)
    assert (rejected["reason"] == "out_of_range").any()


# --- features -------------------------------------------------------------

def test_features_dt_is_positive_within_aircraft(synth_bronze):
    df = kinematic_features(event_time_order(project_raw(synth_bronze, source="synthetic")))
    dt_per_group = df.groupby("icao24")["dt"].agg(lambda s: s.dropna().min())
    assert (dt_per_group >= 0).all()


def test_features_speed_est_near_broadcast_velocity(synth_bronze):
    df = kinematic_features(event_time_order(project_raw(synth_bronze, source="synthetic")))
    rel = (df["speed_est"] - df["velocity"]).abs() / df["velocity"].abs()
    # synthetic flights go in straight lines → speed_est should be close
    assert rel.dropna().median() < 0.1


# --- methods --------------------------------------------------------------

def test_time_gap_assigns_flight_ids(synth_bronze):
    df = segment_time_gap(event_time_order(project_raw(synth_bronze, source="synthetic")))
    assert "flight_id" in df.columns
    assert df["flight_id"].notna().all()


def test_time_gap_breaks_on_long_gap():
    df = pd.DataFrame({
        "icao24": ["abc123"] * 4,
        "time":   [1000, 1010, 1_000_000, 1_000_010],   # huge gap in the middle
        "lat": [10.0] * 4, "lon": [20.0] * 4,
    })
    out = segment_time_gap(df)
    assert out["flight_id"].nunique() == 2


def test_kinematic_breaks_on_position_jump():
    df = pd.DataFrame({
        "icao24": ["abc123"] * 3,
        "time":   [1000, 1010, 1020],
        "lat":    [10.0, 10.001, 50.0],       # 40° jump in 10 s
        "lon":    [20.0, 20.001, 20.001],
        "callsign": ["X"] * 3,
        "position_jump": [False, False, True],
        "dt": [None, 10.0, 10.0],
        "alt_rate": [0.0, 0.0, 0.0],
        "turn_rate": [0.0, 0.0, 0.0],
    })
    out = segment_kinematic(df)
    assert out["flight_id"].nunique() == 2


def test_kalman_converges_on_straight_line():
    """Kalman smoothed path should track noisy observations within a few hundred m."""
    import numpy as np
    rng = np.random.default_rng(42)
    n = 60
    t = 1_000_000 + np.arange(n) * 10
    true_lat = 13.0 + np.arange(n) * 0.001
    true_lon = 77.0 + np.arange(n) * 0.001
    noisy_lat = true_lat + rng.normal(0, 0.0005, n)   # ~55 m noise
    noisy_lon = true_lon + rng.normal(0, 0.0005, n)
    df = pd.DataFrame({
        "icao24": ["abc123"] * n, "time": t,
        "lat": noisy_lat, "lon": noisy_lon,
        "velocity": [100.0] * n, "heading": [45.0] * n, "vertrate": [0.0] * n,
        "callsign": ["X"] * n, "onground": [False] * n,
        "baroaltitude": [10000.0] * n, "geoaltitude": [10000.0] * n,
        "position_jump": [False] * n,
        "dt": [None] + [10.0] * (n - 1),
        "alt_rate": [0.0] * n, "turn_rate": [0.0] * n,
    })
    out = smooth_and_segment(df)
    from etl4.reconstruct.reference import _haversine_m
    err = _haversine_m(out["smoothed_lat"].to_numpy(), out["smoothed_lon"].to_numpy(),
                       true_lat, true_lon)
    assert float(err[10:].mean()) < 100.0


# --- reference / deviation / correction -----------------------------------

def test_great_circle_endpoints_preserved():
    r = great_circle_route(13.0, 77.0, 28.5, 77.1, n_points=50)
    assert abs(r["lat"].iloc[0]  - 13.0) < 0.01
    assert abs(r["lat"].iloc[-1] - 28.5) < 0.01


def test_cross_track_distance_zero_on_path():
    r = great_circle_route(13.0, 77.0, 28.5, 77.1, n_points=100)
    d = cross_track_distance_m(r["lat"].iloc[50], r["lon"].iloc[50],
                               r["lat"], r["lon"])
    assert d < 100.0


def test_deviation_detects_offset_segment():
    df = segment_time_gap(event_time_order(project_raw(_offset_flight(), source="synthetic")))
    df = kinematic_features(df)
    events = detect_deviations(df, use_kalman_residual=False)
    assert len(events) >= 1


def _offset_flight():
    import numpy as np
    n = 120
    t = 1_000_000 + np.arange(n) * 10
    lat = np.concatenate([np.linspace(13.0, 15.0, 60), np.linspace(15.0, 15.0, 60) + 1.0])
    lon = np.concatenate([np.linspace(77.0, 79.0, 60), np.linspace(79.0, 82.0, 60)])
    return pd.DataFrame({
        "icao24": ["abc123"] * n, "time": t,
        "lat": lat, "lon": lon,
        "velocity": [200.0] * n, "heading": [45.0] * n, "vertrate": [0.0] * n,
        "callsign": ["ABC001"] * n, "onground": [False] * n,
        "alert": [False] * n, "spi": [False] * n, "squawk": ["1200"] * n,
        "baroaltitude": [10000.0] * n, "geoaltitude": [10000.0] * n,
        "lastposupdate": t.astype(float), "lastcontact": t.astype(float),
        "hour": (t // 3600) * 3600,
    })


# --- metrics --------------------------------------------------------------

def test_segmentation_prf_perfect_match():
    r = segmentation_prf([100, 200, 300], [100, 200, 300])
    assert r["precision"] == 1.0 and r["recall"] == 1.0 and r["f1"] == 1.0


def test_position_rmse_zero_on_identical_frames(synth_bronze):
    df = project_raw(synth_bronze, source="synthetic")
    rmse = position_rmse_m(df, df)
    assert rmse < 1.0


def test_continuity_ratio_perfect_on_regular_steps():
    n = 100
    df = pd.DataFrame({
        "icao24": ["abc123"] * n, "time": list(range(0, 10 * n, 10)),
        "flight_id": ["abc123_1"] * n,
    })
    assert continuity_ratio(df, expected_step_s=10.0) > 0.95


# --- experiment runner ----------------------------------------------------

def test_run_experiment_time_gap_end_to_end(synth_bronze, tmp_path, monkeypatch):
    import importlib
    runner_mod = importlib.import_module("etl4.bench.run_experiment")
    monkeypatch.setattr(runner_mod, "RUNS_CSV", tmp_path / "runs.csv")
    m = runner_mod.run_experiment(synth_bronze, method="time_gap", source="synthetic")
    assert m["rows_out"] > 0
    assert m["n_flights"] > 0
    assert "wall_s" in m


# --- crash-safety and strengthened invariants (Round 7) -------------------

def test_matrix_csv_written_after_every_cell(tmp_path, monkeypatch):
    """Guards plan.md §13 crash-safety: matrix.csv must be persisted after
    every (method, noise) cell, so a kill on cell N preserves cells 1..N-1."""
    import importlib
    import sys as _sys
    rm = importlib.import_module("etl4.bench.run_matrix")

    csv_path = tmp_path / "matrix.csv"
    calls = {"n": 0}

    def fake_run(bronze, method, truth=None, noise="N0", source="synthetic"):
        calls["n"] += 1
        if calls["n"] >= 2:
            raise RuntimeError("simulated crash on cell 2")
        return {"wall_s": 1.23, "n_flights": 10, "n_deviations": 0,
                "continuity": 0.95, "rows_out": 1000}

    monkeypatch.setattr(rm, "run_experiment", fake_run)
    monkeypatch.setattr(rm, "_load", lambda p: pd.DataFrame(
        {"icao24": ["a"], "time": [0], "lat": [0.0], "lon": [0.0]}))
    monkeypatch.setattr(rm, "MATRIX_CSV", csv_path)
    monkeypatch.setattr(_sys, "argv", [
        "run_matrix.py", "--bronze", str(tmp_path),
        "--methods", "time_gap", "kinematic", "--profiles", "N0",
    ])
    with pytest.raises(RuntimeError):
        rm.main()
    assert csv_path.exists(), "matrix.csv missing after crash — crash-safety broken"
    rows = pd.read_csv(csv_path)
    assert len(rows) == 1, f"expected 1 persisted cell, got {len(rows)}"
    assert rows.iloc[0]["method"] == "time_gap"


def test_kinematic_na_safety_strong():
    """Guards plan.md §17.4: segmentation must not raise on BooleanDtype/Int64
    NAs in position_jump, alt_rate, turn_rate. Without `.fillna()` before the
    boolean OR chain, pandas raises TypeError: boolean value of NA is ambiguous."""
    n = 5
    df = pd.DataFrame({
        "icao24": ["abc123"] * n,
        "time": [1000, 1010, 1020, 1030, 1040],
        "lat": [13.0] * n,
        "lon": [77.0, 77.001, 77.002, 77.003, 77.004],
        "callsign": ["X"] * n,
        "position_jump": pd.array([False, None, True, False, None], dtype="boolean"),
        "dt": pd.array([None, 10.0, 10.0, 10.0, 10.0], dtype="Float64"),
        "alt_rate": pd.array([0.0, None, 0.0, 0.0, 0.0], dtype="Float64"),
        "turn_rate": pd.array([0.0, 0.0, None, 0.0, 0.0], dtype="Float64"),
    })
    out = segment_kinematic(df, break_on_jump=True)
    assert "flight_id" in out.columns
    assert out["flight_id"].notna().all()
    assert len(out) == n
    assert out["flight_id"].nunique() >= 2  # at least one break on the NA-containing jump


def test_clean_coerces_icao24_to_string_dtype():
    """Guards plan.md §0.6 / CLAUDE.md §2.6: icao24 must END UP pandas `string`
    through project_raw and clean, regardless of input dtype. Pandas can
    silently parse a hex string like `0a1b2c` to int `10534700`, which
    cross-joins airframes — project_raw must FORCE string.

    Two shapes: (1) input is already `StringDtype` (preservation); (2) input
    is `object` dtype (forcing). Both must end in StringDtype.
    """
    def _fixture(icao_col):
        return pd.DataFrame({
            "icao24":        icao_col,
            "time":          [1_000_000, 1_000_010],
            "lat":           [10.0, 11.0], "lon": [20.0, 21.0],
            "velocity":      [200.0, 200.0], "heading": [0.0, 0.0], "vertrate": [0.0, 0.0],
            "callsign":      ["TST001", "TST001"], "onground": [False, False],
            "alert":         [False, False], "spi": [False, False], "squawk": ["1200", "1200"],
            "baroaltitude":  [10000.0, 10000.0], "geoaltitude": [10030.0, 10030.0],
            "lastposupdate": [1_000_000.0, 1_000_010.0],
            "lastcontact":   [1_000_000.0, 1_000_010.0],
            "hour":          [1_000_000, 1_000_000],
        })

    # 1. preservation: StringDtype in -> StringDtype out
    df_str = _fixture(pd.array(["abc123", "def456"], dtype="string"))
    projected_str = project_raw(df_str, source="synthetic")
    assert isinstance(projected_str["icao24"].dtype, pd.StringDtype)
    cleaned_str, _ = clean(projected_str)
    assert isinstance(cleaned_str["icao24"].dtype, pd.StringDtype)

    # 2. forcing: object-dtype hex input -> StringDtype output
    df_obj = _fixture(pd.Series(["0a1b2c", "ff0000"], dtype="object"))
    projected_obj = project_raw(df_obj, source="synthetic")
    assert isinstance(projected_obj["icao24"].dtype, pd.StringDtype), \
        f"project_raw did not coerce object-dtype icao24 to string (got {projected_obj['icao24'].dtype})"
    # original hex form preserved — no silent int parse
    assert list(projected_obj["icao24"]) == ["0a1b2c", "ff0000"]


def test_deviations_duration_filter_30s_no_fire_90s_fire():
    """Guards plan.md §9 / deviations.py `MIN_EVENT_DURATION_S=60`: a 30-s
    off-route spike must NOT produce an event; a 90-s run must."""
    import numpy as np

    def _flight(fid, bump_rows):
        n = 30
        t = np.arange(n, dtype=int) * 10        # 0..290 s
        lat = np.full(n, 13.0, dtype=float)
        lon = np.linspace(77.0, 78.0, n)
        for i in bump_rows:
            lat[i] = 15.0                        # ~222 km off-route -> >> 5 km threshold
        return pd.DataFrame({"icao24": ["abc123"] * n, "flight_id": [fid] * n,
                             "time": t, "lat": lat, "lon": lon})

    short = _flight("F_30", [15, 16, 17, 18])             # 4 rows span 30 s
    long_ = _flight("F_90", list(range(15, 25)))          # 10 rows span 90 s
    df = pd.concat([short, long_], ignore_index=True)
    events = detect_deviations(df, use_kalman_residual=False)
    assert (events["flight_id"] == "F_30").sum() == 0, "30-s spike fired an event"
    assert (events["flight_id"] == "F_90").sum() == 1, "90-s run did not fire"
    row = events[events["flight_id"] == "F_90"].iloc[0]
    assert row["end_time"] - row["start_time"] >= 60
