"""Unit tests for the NoiseTruth contract (T-001) and the __truth_id
round-trip (T-002). Anchors ADR 2026-11-14 and architecture.md invariants.

Each test anchors one invariant; see docstrings for the specific property.
"""
from __future__ import annotations

import dataclasses

import numpy as np
import pandas as pd
import pytest

from etl4.bench.noise import NOISE_PROFILES, TRUTH_ID, NoiseTruth, _infer_boundaries, inject_noise


# --- helpers --------------------------------------------------------------

def _make_fixture(n_per_aircraft: int = 50, n_aircraft: int = 2, seed: int = 0) -> pd.DataFrame:
    """Minimal clean synthetic frame: time-sequential, two icao24 groups,
    with all the columns the noise injector may mutate. Deterministic.
    """
    rng = np.random.default_rng(seed)
    frames = []
    for i in range(n_aircraft):
        t0 = 1_000_000 + i * 10_000
        t = t0 + np.arange(n_per_aircraft) * 10
        frames.append(pd.DataFrame({
            "icao24":       pd.array([f"abc{i:03d}"] * n_per_aircraft, dtype="string"),
            "time":         t.astype(np.int64),
            "lat":          13.0 + np.arange(n_per_aircraft) * 0.001,
            "lon":          77.0 + np.arange(n_per_aircraft) * 0.001,
            "baroaltitude": 10_000.0 + rng.normal(0, 5, n_per_aircraft),
            "velocity":     200.0 * np.ones(n_per_aircraft),
            "heading":      45.0 * np.ones(n_per_aircraft),
            "callsign":     ["SYN001"] * n_per_aircraft,
        }))
    return pd.concat(frames, ignore_index=True)


# --- 1. NoiseTruth is frozen ---------------------------------------------

def test_noise_truth_is_frozen():
    """A frozen dataclass must refuse field assignment (FrozenInstanceError)."""
    df = _make_fixture()
    _, truth = inject_noise(df, "N0", seed=42)
    with pytest.raises(dataclasses.FrozenInstanceError):
        truth.profile = "N3"
    with pytest.raises(dataclasses.FrozenInstanceError):
        truth.dropped_ids = frozenset({1})


# --- 2. N0 is an identity injection ---------------------------------------

def test_n0_is_identity():
    """Profile N0 has zero probabilities for every step, so NoiseTruth must
    report zero drops/dups/shuffles/jumps and preserve the row count."""
    df = _make_fixture()
    out, truth = inject_noise(df, "N0", seed=42)

    assert truth.profile == "N0"
    assert truth.seed == 42
    assert truth.original_row_count == len(df)
    assert len(out) == len(df)
    assert truth.dropped_ids == frozenset()
    assert truth.duplicated_ids == frozenset()
    assert truth.shuffled_ids == frozenset()
    assert truth.jumped_ids == frozenset()
    assert len(truth.surviving_ids()) == len(df)
    assert truth.surviving_ids() == frozenset(range(len(df)))


# --- 3. Determinism given a fixed seed ------------------------------------

def test_determinism_under_fixed_seed():
    """Two calls with the same (df, profile, seed) must produce element-wise
    equal noisy frames and set-equal NoiseTruth id-sets."""
    df = _make_fixture()
    out1, truth1 = inject_noise(df, "N2", seed=42)
    out2, truth2 = inject_noise(df, "N2", seed=42)

    pd.testing.assert_frame_equal(out1.reset_index(drop=True),
                                  out2.reset_index(drop=True))
    assert truth1.dropped_ids == truth2.dropped_ids
    assert truth1.duplicated_ids == truth2.duplicated_ids
    assert truth1.shuffled_ids == truth2.shuffled_ids
    assert truth1.jumped_ids == truth2.jumped_ids
    assert truth1.true_boundaries == truth2.true_boundaries


# --- 4. Dropped disjoint from surviving; duplicated ids repeat in output --

def test_dropped_disjoint_duplicated_repeat_n3():
    """Under N3 a dropped id must not appear in the noisy output, and every
    duplicated id must appear at least twice in the output."""
    df = _make_fixture(n_per_aircraft=100, n_aircraft=2)
    out, truth = inject_noise(df, "N3", seed=42)

    out_ids = {int(x) for x in out[TRUTH_ID]}
    assert truth.dropped_ids.isdisjoint(out_ids), (
        "dropped id surfaced in output: contract violation"
    )

    # Every duplicated id present in output >= 2 times.
    counts = out[TRUTH_ID].value_counts()
    for tid in truth.duplicated_ids:
        assert counts.get(tid, 0) >= 2, f"duplicated id {tid} not >=2 in output"

    # And something real happened at N3: non-empty drops.
    assert len(truth.dropped_ids) > 0, "N3 produced no drops on 200 rows"


# --- 5. Jumped ids have corresponding coordinate records ------------------

def test_jumped_ids_have_original_coords():
    """truth.jumped_ids must be exactly the set of __truth_id values recorded
    in truth.original_coords (so metrics.position_rmse_m has a pre-jump row
    to join against for every jumped id)."""
    df = _make_fixture(n_per_aircraft=200, n_aircraft=2)
    out, truth = inject_noise(df, "N3", seed=42)

    assert len(truth.jumped_ids) > 0, "N3 produced no jumps; cannot assert"
    coord_ids = {int(x) for x in truth.original_coords[TRUTH_ID]}
    assert truth.jumped_ids == coord_ids
    # Columns on original_coords match architecture.md contract.
    assert set(truth.original_coords.columns) == {TRUTH_ID, "lat", "lon", "baroaltitude"}


# --- 6. true_boundaries are list[tuple[str, int]] -------------------------

def test_true_boundaries_shape_and_dtypes():
    """Each element of truth.true_boundaries must be a 2-tuple (icao24, time)
    where icao24 is str and time is int -- binding contract from
    architecture.md section Data contracts."""
    df = _make_fixture(n_per_aircraft=20, n_aircraft=3)
    _, truth = inject_noise(df, "N0", seed=42)

    assert isinstance(truth.true_boundaries, list)
    # One start per icao24 (no 1800s gaps in this fixture).
    assert len(truth.true_boundaries) == 3
    for entry in truth.true_boundaries:
        assert isinstance(entry, tuple)
        assert len(entry) == 2
        icao, t = entry
        assert isinstance(icao, str), f"icao24 not str: {type(icao)}"
        assert isinstance(t, int), f"time not int (int leaked?): {type(t)}"
        assert not isinstance(t, bool), "bool is int subclass; reject"


# --- 7. Fallback when __truth_id is absent on the input -------------------

def test_fallback_generates_truth_id_when_absent():
    """A caller that forgets to add __truth_id upstream must still get a
    valid NoiseTruth: the injector generates monotonic ids inline."""
    df = _make_fixture(n_per_aircraft=30, n_aircraft=1)
    assert TRUTH_ID not in df.columns

    out, truth = inject_noise(df, "N1", seed=42)

    assert TRUTH_ID in out.columns
    assert out[TRUTH_ID].dtype == np.int64
    assert truth.original_row_count == len(df)
    # Surviving ids are a subset of the generated range.
    assert truth.surviving_ids().issubset(frozenset(range(len(df))))
    # NoiseTruth is still a valid instance.
    assert isinstance(truth, NoiseTruth)


# --- 8. _infer_boundaries returns [] when icao24/time absent --------------

def test_infer_boundaries_missing_columns_returns_empty():
    """_infer_boundaries must silently return [] when the frame lacks
    icao24 or time, not raise. Called on cleaned frames that drop columns."""
    # Missing icao24.
    df_no_icao = pd.DataFrame({"time": [1, 2, 3], "lat": [0.0, 0.0, 0.0]})
    assert _infer_boundaries(df_no_icao) == []
    # Missing time.
    df_no_time = pd.DataFrame({"icao24": ["a", "b"], "lat": [0.0, 0.0]})
    assert _infer_boundaries(df_no_time) == []
    # Empty frame but correct columns.
    df_empty = pd.DataFrame({"icao24": [], "time": [], "lat": []})
    assert _infer_boundaries(df_empty) == []


# --- 9. Duplicated ids count shape: N1 produces >=1 dup on a big sample ---

def test_n1_profile_counts_are_well_formed():
    """Sanity: on a 500-row fixture N1 produces non-empty duplicated_ids and
    non-empty dropped_ids with the architecture.md probabilities."""
    df = _make_fixture(n_per_aircraft=250, n_aircraft=2)
    out, truth = inject_noise(df, "N1", seed=42)

    cfg = NOISE_PROFILES["N1"]
    # Lower-bound check: at least 1 dropped (~2% * 500 = ~10 expected).
    assert len(truth.dropped_ids) >= 1
    # Lower-bound check: at least 1 duplicated (0.5% expected -> small, but
    # at least one at 500 rows).
    assert len(truth.duplicated_ids) >= 1
    # And N1 jumps have coords on the pre-noise frame.
    assert set(int(x) for x in truth.original_coords[TRUTH_ID]) == truth.jumped_ids
    assert cfg["gaps"] > 0  # structural


# --- 10. __truth_id round-trip (skipped until T-002 lands) ----------------

def _synthetic_emits_truth_id() -> bool:
    """Probe whether etl4.ingest.synthetic.generate_synthetic already emits
    the __truth_id column (T-002 deliverable). Returns False if synthetic.py
    is importable but the column is absent, True if present."""
    try:
        from etl4.ingest.synthetic import generate_synthetic
    except Exception:
        return False
    try:
        df = generate_synthetic(10, seed=42)
        return TRUTH_ID in df.columns
    except Exception:
        return False


@pytest.mark.skipif(
    not _synthetic_emits_truth_id(),
    reason="T-002 not landed: synthetic.generate_synthetic does not emit __truth_id yet",
)
def test_synthetic_truth_id_is_monotonic_unique_int64():
    """When T-002 has landed, synthetic generation must emit __truth_id as a
    monotonic, unique int64 column. Auto-unskips once T-002 completes."""
    from etl4.ingest.synthetic import generate_synthetic

    df = generate_synthetic(500, seed=42)
    assert TRUTH_ID in df.columns
    assert df[TRUTH_ID].dtype == np.int64
    assert df[TRUTH_ID].is_monotonic_increasing
    assert df[TRUTH_ID].is_unique
