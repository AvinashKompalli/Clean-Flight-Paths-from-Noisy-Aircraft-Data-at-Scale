"""Controlled noise injection for robustness experiments.

Profiles N0 to N3 inject gaps, duplicates, out-of-order shuffles, and
position jumps. Applied AFTER a clean synthetic dataset is generated so
we have ground truth to evaluate against.

`inject_noise` returns a `(noisy_df, NoiseTruth)` tuple. `NoiseTruth`
records the exact rows the injector touched so downstream metrics can
score reconstruction against the pre-noise ground truth without having
to re-derive it from the random seed. See
`etl4/docs/decisions.md` ADR 2026-11-14 for the contract.
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

NOISE_PROFILES = {
    "N0": {"gaps": 0.00, "duplicates": 0.000, "out_of_order": 0.00, "jumps": 0.000},
    "N1": {"gaps": 0.02, "duplicates": 0.005, "out_of_order": 0.01, "jumps": 0.001},
    "N2": {"gaps": 0.05, "duplicates": 0.010, "out_of_order": 0.03, "jumps": 0.005},
    "N3": {"gaps": 0.10, "duplicates": 0.020, "out_of_order": 0.10, "jumps": 0.010},
}

TRUTH_ID = "__truth_id"
_FLIGHT_GAP_S = 1800.0


@dataclass(frozen=True)
class NoiseTruth:
    profile: str
    seed: int
    original_row_count: int
    original_ids: frozenset
    original_coords: pd.DataFrame
    dropped_ids: frozenset
    duplicated_ids: frozenset
    shuffled_ids: frozenset
    jumped_ids: frozenset
    true_boundaries: list  # list[tuple[str, int]] of (icao24, time) at flight starts

    def surviving_ids(self) -> frozenset:
        # Correct across sparse / pre-sparsified __truth_id inputs:
        # survivors = exact input ids minus those dropped by the injector.
        # The earlier range(original_row_count) formula broke when upstream
        # synthetic.add_noise pre-sparsified the id set.
        return self.original_ids - self.dropped_ids


def _infer_boundaries(df: pd.DataFrame) -> list:
    """Per-flight start points as (icao24, time) tuples on the clean df.
    Shape is architecture.md binding: list[tuple[str, int]]. Downstream
    segmentation_prf1 compares predicted (icao24, time) boundaries against
    these without reverse-mapping through __truth_id.
    """
    if "icao24" not in df.columns or "time" not in df.columns or df.empty:
        return []
    ordered = df.sort_values(["icao24", "time"], kind="stable")
    icao = ordered["icao24"].to_numpy()
    time = ordered["time"].to_numpy()
    new_icao = np.empty(len(ordered), dtype=bool)
    new_icao[0] = True
    new_icao[1:] = icao[1:] != icao[:-1]
    dt = np.empty(len(ordered), dtype=float)
    dt[0] = np.inf
    dt[1:] = time[1:].astype(float) - time[:-1].astype(float)
    boundary_mask = new_icao | (dt > _FLIGHT_GAP_S)
    return list(zip(icao[boundary_mask].tolist(),
                    [int(t) for t in time[boundary_mask].tolist()]))


def inject_noise(
    df: pd.DataFrame, profile: str, seed: int = 42
) -> tuple[pd.DataFrame, NoiseTruth]:
    cfg = NOISE_PROFILES[profile]
    rng = np.random.default_rng(seed)
    out = df.copy().reset_index(drop=True)

    if TRUTH_ID not in out.columns:
        out[TRUTH_ID] = np.arange(len(out), dtype=np.int64)
    else:
        out[TRUTH_ID] = out[TRUTH_ID].astype(np.int64)

    original_row_count = len(out)
    original_ids = out[TRUTH_ID].to_numpy().copy()
    true_boundaries = _infer_boundaries(out)

    has_coords = {"lat", "lon", "baroaltitude"}.issubset(out.columns)
    if has_coords:
        pre_coords = out[[TRUTH_ID, "lat", "lon", "baroaltitude"]].copy()
        pre_coords = pre_coords.set_index(TRUTH_ID, drop=False)
    else:
        pre_coords = pd.DataFrame(
            columns=[TRUTH_ID, "lat", "lon", "baroaltitude"]
        ).set_index(TRUTH_ID, drop=False)

    # 1. gaps
    if cfg["gaps"] > 0:
        keep = rng.random(len(out)) > cfg["gaps"]
        out = out[keep].reset_index(drop=True)

    # 2. duplicates
    if cfg["duplicates"] > 0:
        n_dup = int(len(out) * cfg["duplicates"])
        if n_dup:
            dup_idx = rng.integers(0, len(out), size=n_dup)
            out = pd.concat([out, out.iloc[dup_idx]], ignore_index=True)

    # 3. position jumps
    jumped_ids_arr = np.empty(0, dtype=np.int64)
    if cfg["jumps"] > 0 and has_coords:
        n_jump = int(len(out) * cfg["jumps"])
        if n_jump:
            jump_idx = rng.integers(0, len(out), size=n_jump)
            jumped_ids_arr = out.loc[jump_idx, TRUTH_ID].to_numpy()
            out.loc[jump_idx, "lat"] = (
                out.loc[jump_idx, "lat"].to_numpy() + rng.uniform(-2.0, 2.0, n_jump)
            )
            out.loc[jump_idx, "lon"] = (
                out.loc[jump_idx, "lon"].to_numpy() + rng.uniform(-2.0, 2.0, n_jump)
            )

    # 4. out-of-order shuffle (single iloc[perm], never per-row swap)
    if cfg["out_of_order"] > 0:
        n_shuffle = int(len(out) * cfg["out_of_order"])
        if n_shuffle:
            perm = np.arange(len(out))
            picked = rng.choice(len(out), size=n_shuffle, replace=False)
            perm[picked] = rng.permutation(perm[picked])
            out = out.iloc[perm]

    out = out.reset_index(drop=True)

    out_ids = out[TRUTH_ID].to_numpy()
    out_ids_set = frozenset(int(x) for x in out_ids)
    original_ids_set = frozenset(int(x) for x in original_ids)

    dropped_ids = original_ids_set - out_ids_set

    if len(out_ids):
        counts = pd.Series(out_ids).value_counts()
        duplicated_ids = frozenset(int(x) for x in counts[counts > 1].index.tolist())
    else:
        duplicated_ids = frozenset()

    jumped_ids = frozenset(int(x) for x in jumped_ids_arr.tolist())

    first_pos = {}
    for pos, tid in enumerate(out_ids.tolist()):
        if tid not in first_pos:
            first_pos[tid] = pos
    shuffled = []
    for original_pos, tid in enumerate(original_ids.tolist()):
        if tid in first_pos and first_pos[tid] != original_pos:
            shuffled.append(int(tid))
    shuffled_ids = frozenset(shuffled)

    if has_coords and jumped_ids:
        original_coords = pre_coords.loc[list(jumped_ids)].reset_index(drop=True)
    else:
        original_coords = pd.DataFrame(
            columns=[TRUTH_ID, "lat", "lon", "baroaltitude"]
        )

    truth = NoiseTruth(
        profile=profile,
        seed=seed,
        original_row_count=original_row_count,
        original_ids=original_ids_set,
        original_coords=original_coords,
        dropped_ids=dropped_ids,
        duplicated_ids=duplicated_ids,
        shuffled_ids=shuffled_ids,
        jumped_ids=jumped_ids,
        true_boundaries=true_boundaries,
    )
    return out, truth
