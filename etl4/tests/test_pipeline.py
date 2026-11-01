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
