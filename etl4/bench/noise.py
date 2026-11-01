"""Controlled noise injection for robustness experiments (plan §12.2).

Profiles N0 → N3 inject gaps, duplicates, out-of-order shuffles, and
position jumps. Applied AFTER a clean synthetic dataset is generated so
we have ground truth to evaluate against.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

NOISE_PROFILES = {
    "N0": {"gaps": 0.00, "duplicates": 0.000, "out_of_order": 0.00, "jumps": 0.000},
    "N1": {"gaps": 0.02, "duplicates": 0.005, "out_of_order": 0.01, "jumps": 0.001},
    "N2": {"gaps": 0.05, "duplicates": 0.010, "out_of_order": 0.03, "jumps": 0.005},
    "N3": {"gaps": 0.10, "duplicates": 0.020, "out_of_order": 0.10, "jumps": 0.010},
}


def inject_noise(df: pd.DataFrame, profile: str, seed: int = 42) -> pd.DataFrame:
    cfg = NOISE_PROFILES[profile]
    rng = np.random.default_rng(seed)
    out = df.copy().reset_index(drop=True)

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
    if cfg["jumps"] > 0:
        n_jump = int(len(out) * cfg["jumps"])
        if n_jump:
            jump_idx = rng.integers(0, len(out), size=n_jump)
            out.loc[jump_idx, "lat"] += rng.uniform(-2.0, 2.0, n_jump)
            out.loc[jump_idx, "lon"] += rng.uniform(-2.0, 2.0, n_jump)

    # 4. out-of-order shuffle (vectorised: permute a random subset of positions)
    if cfg["out_of_order"] > 0:
        n_shuffle = int(len(out) * cfg["out_of_order"])
        if n_shuffle:
            perm = np.arange(len(out))
            picked = rng.choice(len(out), size=n_shuffle, replace=False)
            perm[picked] = rng.permutation(perm[picked])
            out = out.iloc[perm]

    return out.reset_index(drop=True)
