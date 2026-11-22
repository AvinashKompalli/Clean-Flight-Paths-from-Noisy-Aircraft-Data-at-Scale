"""Round-trip smoke: `__truth_id` survives synthetic -> noise -> clean ->
event-time ordering -> reconstruct for all three methods without being
dropped, duplicated across different source rows, or shuffled out of
correspondence with ground truth.

Run from repo root with the opensky conda env active:
    python multi-agent-engineering/projects/PROJ-20261114-wi1-metrics/artifacts/t002_roundtrip.py
"""
from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[4]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from etl4.bench.noise import TRUTH_ID, inject_noise
from etl4.clean.clean import clean, project_raw
from etl4.clean.features import event_time_order, kinematic_features
from etl4.ingest.synthetic import generate_synthetic
from etl4.reconstruct.kalman import smooth_and_segment
from etl4.reconstruct.kinematic import segment_kinematic
from etl4.reconstruct.time_gap import segment_time_gap


def _assert_preserves(stage: str, df, truth_ground, require_unique: bool):
    assert TRUTH_ID in df.columns, f"{stage}: dropped {TRUTH_ID}"
    assert df[TRUTH_ID].notna().all(), f"{stage}: null {TRUTH_ID}"
    if require_unique:
        assert df[TRUTH_ID].is_unique, f"{stage}: duplicate {TRUTH_ID} in output"
    surviving = truth_ground.surviving_ids()
    out_ids = frozenset(int(x) for x in df[TRUTH_ID].tolist())
    missing = out_ids - surviving
    assert not missing, (
        f"{stage}: ids outside surviving set: {sorted(missing)[:5]}"
    )


def _assert_correspondence(stage: str, df, gt):
    merged = df[[TRUTH_ID, "icao24", "time"]].merge(
        gt, on=TRUTH_ID, suffixes=("", "_gt"), validate="m:1",
    )
    assert len(merged) == len(df), (
        f"{stage}: unmatched rows after merge-to-truth "
        f"({len(merged)} vs {len(df)})"
    )
    mismatched = (merged["icao24"] != merged["icao24_gt"]).sum()
    assert mismatched == 0, (
        f"{stage}: {mismatched} rows have icao24 different from ground truth"
    )
    time_diff = (merged["time"] != merged["time_gt"]).sum()
    assert time_diff == 0, (
        f"{stage}: {time_diff} rows have time different from ground truth"
    )


def main() -> int:
    clean_df = generate_synthetic(500, seed=42)
    assert TRUTH_ID in clean_df.columns, "generate_synthetic missing __truth_id"
    assert clean_df[TRUTH_ID].is_unique, f"{TRUTH_ID} not unique at source"
    assert clean_df[TRUTH_ID].is_monotonic_increasing, (
        f"{TRUTH_ID} not monotonic at source"
    )
    gt = clean_df[[TRUTH_ID, "icao24", "time"]].copy()

    n_input = len(clean_df)
    noisy_df, truth = inject_noise(clean_df, "N2", seed=42)

    projected = project_raw(noisy_df, source="synthetic")
    cleaned, _rejected = clean(projected)
    ordered = event_time_order(cleaned)

    # project_raw runs before dedup, so duplicates from noise.py's dup step
    # still coexist; uniqueness is only required post-dedup.
    _assert_preserves("project_raw", projected, truth, require_unique=False)
    _assert_preserves("clean", cleaned, truth, require_unique=True)
    _assert_preserves("event_time_order", ordered, truth, require_unique=True)
    _assert_correspondence("event_time_order", ordered, gt)

    featurised = kinematic_features(ordered.copy())
    tg = segment_time_gap(featurised.copy())
    kin = segment_kinematic(featurised.copy())
    kal = smooth_and_segment(featurised.copy())

    for name, out in (("time_gap", tg), ("kinematic", kin), ("kalman", kal)):
        _assert_preserves(name, out, truth, require_unique=True)
        _assert_correspondence(name, out, gt)

    out_ids_set = frozenset(int(x) for x in tg[TRUTH_ID].tolist())
    surviving = truth.surviving_ids()
    k_matched = len(out_ids_set & surviving)
    n_output = len(tg)
    print(
        f"ROUND-TRIP OK: {n_input} input, {n_output} output, "
        f"{k_matched} surviving ids match"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
