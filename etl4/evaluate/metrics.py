"""Trajectory and segmentation quality metrics.

All functions take plain pandas DataFrames and return scalar floats / dicts.
Keep them pure: no I/O, no side effects, no config lookups.

Two families of functions coexist here:

  * Legacy (icao24, time) joiners -- `position_rmse_m`, `coverage_ratio`,
    `segmentation_prf`, `deviation_prf`, `boundary_time_error_s`. These
    accept pandas frames and lists and are used by older call sites and
    the existing test suite.
  * `__truth_id`-aware variants -- `segmentation_prf1`, `deviation_prf1`,
    plus `position_rmse_m` / `coverage_ratio` overloads that activate
    when a `NoiseTruth` is passed instead of a frame. These are the
    scorers wired into `bench/run_experiment.py::reconstruct_from_silver`
    for the synthetic-path metric matrix (plan.md section 15, course
    PDF section 3, ADR 2026-11-14).

NoiseTruth is only type-referenced for annotations -- `etl4.bench` is
imported for runtime isinstance checks via a local import to avoid a
circular with `etl4.evaluate` during package loading.
"""
from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING, Iterable

import numpy as np
import pandas as pd

from etl4.reconstruct.reference import _haversine_m

if TYPE_CHECKING:
    from etl4.bench.noise import NoiseTruth

BOUNDARY_TOLERANCE_S = 60.0

TRUTH_ID = "__truth_id"


def _is_noise_truth(obj) -> bool:
    """Runtime check without eager-importing etl4.bench at module load."""
    from etl4.bench.noise import NoiseTruth as _NT
    return isinstance(obj, _NT)


def _bucket_by_icao(boundaries: Iterable) -> dict:
    """Group (icao24, time) boundary tuples by icao24, returning a
    per-icao24 list of times sorted ascending. Shared by the matched
    scorers (`segmentation_prf1`, `boundary_time_error_s`) so the
    matching semantics stay aligned."""
    buckets: dict = defaultdict(list)
    for b in boundaries:
        if isinstance(b, tuple) and len(b) == 2:
            icao, t = b
            buckets[str(icao)].append(float(t))
        else:
            buckets[None].append(float(b))
    for v in buckets.values():
        v.sort()
    return buckets


def position_rmse_m(predicted: pd.DataFrame, truth, truth_df: pd.DataFrame | None = None,
                    on: tuple = ("icao24", "time")) -> float | None:
    """Root-mean-square distance (metres) between predicted and truth positions.

    Overloaded on the type of `truth`:

      * If `truth` is a `NoiseTruth`, join `predicted` to `truth_df` on
        `__truth_id` restricted to `truth.surviving_ids()`. This is the
        synthetic-path scorer (`__truth_id` survives `clean + order +
        reconstruct`, so correspondence is exact under drops / dups /
        shuffles / jumps). Returns `None` if the join is empty.
      * Otherwise `truth` is a DataFrame and the legacy path joins on
        `(icao24, time)`. Returns `float('nan')` on empty join for
        backward compat with the existing test suite.
    """
    if {"smoothed_lat", "smoothed_lon"}.issubset(predicted.columns):
        pred_lat_col, pred_lon_col = "smoothed_lat", "smoothed_lon"
    else:
        pred_lat_col, pred_lon_col = "lat", "lon"

    if _is_noise_truth(truth):
        if truth_df is None:
            return None
        if TRUTH_ID not in predicted.columns or TRUTH_ID not in truth_df.columns:
            return None
        surviving = truth.surviving_ids()
        if not surviving:
            return None
        tdf = truth_df[[TRUTH_ID, "lat", "lon"]].dropna(subset=[TRUTH_ID])
        tdf = tdf[tdf[TRUTH_ID].astype("int64").isin(surviving)]
        tdf = tdf.drop_duplicates(subset=[TRUTH_ID])
        pdf = predicted[[TRUTH_ID, pred_lat_col, pred_lon_col]].dropna(subset=[TRUTH_ID])
        pdf = pdf.rename(columns={pred_lat_col: "lat", pred_lon_col: "lon"})
        join = pdf.merge(tdf, on=TRUTH_ID, suffixes=("_p", "_t"))
        if join.empty:
            return None
        d = _haversine_m(join["lat_p"], join["lon_p"], join["lat_t"], join["lon_t"])
        return float(np.sqrt(np.mean(d ** 2)))

    # Legacy (icao24, time) path.
    if pred_lat_col != "lat":
        pred_use = predicted[list(on) + [pred_lat_col, pred_lon_col]].rename(
            columns={pred_lat_col: "lat", pred_lon_col: "lon"})
    else:
        pred_use = predicted[list(on) + ["lat", "lon"]]
    join = pred_use.merge(truth[list(on) + ["lat", "lon"]],
                          on=list(on), suffixes=("_p", "_t"))
    if join.empty:
        return float("nan")
    d = _haversine_m(join["lat_p"], join["lon_p"], join["lat_t"], join["lon_t"])
    return float(np.sqrt(np.mean(d ** 2)))


def segmentation_prf(predicted_boundaries: Iterable, truth_boundaries: Iterable,
                     tolerance_s: float = BOUNDARY_TOLERANCE_S) -> dict:
    """Precision / recall / F1 of predicted segmentation boundary timestamps.
    Each boundary is an (icao24, time) tuple; a scalar time is treated as a
    scoped-to-nobody boundary and will only match a scalar truth boundary.
    A boundary is a TP if its icao24 matches and its time is within ±tol."""
    def _key(b):
        return b if isinstance(b, tuple) else (None, b)

    predicted = sorted((_key(b) for b in predicted_boundaries), key=lambda x: x[1])
    truth     = sorted((_key(b) for b in truth_boundaries),     key=lambda x: x[1])
    tp = 0
    matched_truth = set()
    for p_icao, p_t in predicted:
        for i, (t_icao, t_t) in enumerate(truth):
            if i in matched_truth:
                continue
            if p_icao == t_icao and abs(p_t - t_t) <= tolerance_s:
                tp += 1
                matched_truth.add(i)
                break
    fp = len(predicted) - tp
    fn = len(truth) - tp
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall    = tp / (tp + fn) if (tp + fn) else 0.0
    f1        = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
    return {"precision": precision, "recall": recall, "f1": f1,
            "tp": tp, "fp": fp, "fn": fn}


def segmentation_prf1(predicted_boundaries: Iterable, true_boundaries: Iterable,
                      tolerance_s: float = 5.0) -> dict:
    """Precision / recall / F1 of predicted boundaries vs truth boundaries,
    scoped per icao24 with greedy within-tolerance matching.

    Each boundary is a (icao24, time) tuple (architecture.md binding).
    TP = a predicted (icao24, t_pred) where some unmatched truth
    (icao24, t_true) on the same icao24 has |t_pred - t_true| <= tolerance_s;
    each truth boundary can be matched at most once.

    Returns the keys the sink persists: `seg_precision`, `seg_recall`,
    `seg_f1`.
    """
    truth_buckets = _bucket_by_icao(true_boundaries)
    matched: dict = defaultdict(set)
    tp = 0
    n_pred = 0
    for b in predicted_boundaries:
        n_pred += 1
        if isinstance(b, tuple) and len(b) == 2:
            icao, t_pred = str(b[0]), float(b[1])
        else:
            icao, t_pred = None, float(b)
        candidates = truth_buckets.get(icao, [])
        for i, t_true in enumerate(candidates):
            if i in matched[icao]:
                continue
            if abs(t_pred - t_true) <= tolerance_s:
                matched[icao].add(i)
                tp += 1
                break
    n_truth = sum(len(v) for v in truth_buckets.values())
    fp = n_pred - tp
    fn = n_truth - tp
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall    = tp / (tp + fn) if (tp + fn) else 0.0
    f1        = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
    return {"seg_precision": precision, "seg_recall": recall, "seg_f1": f1}


def boundary_time_error_s(predicted_boundaries: Iterable,
                          truth_boundaries: Iterable,
                          tolerance_s: float = BOUNDARY_TOLERANCE_S) -> float | None:
    """Mean |t_pred - t_true| across greedy per-icao24 matched pairs.

    A predicted boundary contributes only when it finds an unmatched
    truth boundary on the same icao24 within `tolerance_s`; unmatched
    predictions are excluded (otherwise huge-tolerance semantics would
    conflate miss distance with match error). Returns `None` when no
    pair matches, so Postgres can render NULL rather than a spurious
    zero.
    """
    truth_buckets = _bucket_by_icao(truth_boundaries)
    matched: dict = defaultdict(set)
    errors: list = []
    for b in predicted_boundaries:
        if isinstance(b, tuple) and len(b) == 2:
            icao, t_pred = str(b[0]), float(b[1])
        else:
            icao, t_pred = None, float(b)
        candidates = truth_buckets.get(icao, [])
        best_i, best_dt = -1, float("inf")
        for i, t_true in enumerate(candidates):
            if i in matched[icao]:
                continue
            dt = abs(t_pred - t_true)
            if dt < best_dt:
                best_dt = dt
                best_i = i
        if best_i >= 0 and best_dt <= tolerance_s:
            matched[icao].add(best_i)
            errors.append(best_dt)
    if not errors:
        return None
    return float(np.mean(errors))


def coverage_ratio(predicted: pd.DataFrame, truth) -> float:
    """Fraction of ground-truth rows the reconstruction covers.

    Overloaded on the type of `truth`:

      * `NoiseTruth`: unique reconstructed `__truth_id` count divided by
        `len(truth.original_ids)`, clamped to [0, 1]. This is the
        synthetic-path coverage metric persisted to Postgres.
      * `pd.DataFrame`: legacy track-length ratio per icao24, averaged.
    """
    if _is_noise_truth(truth):
        if TRUTH_ID not in predicted.columns:
            return 0.0
        original_n = len(truth.original_ids)
        if original_n <= 0:
            return 0.0
        unique_pred = predicted[TRUTH_ID].dropna().astype("int64").unique()
        ratio = float(len(unique_pred)) / float(original_n)
        return max(0.0, min(1.0, ratio))

    def track_len(df):
        if len(df) < 2:
            return 0.0
        d = _haversine_m(df["lat"].to_numpy()[:-1], df["lon"].to_numpy()[:-1],
                         df["lat"].to_numpy()[1:],  df["lon"].to_numpy()[1:])
        return float(np.nansum(d))

    truth_grp = truth.groupby("icao24")
    pred_grp  = predicted.groupby("icao24")
    ratios = []
    for icao, tdf in truth_grp:
        if icao in pred_grp.groups:
            tl = track_len(tdf)
            pl = track_len(pred_grp.get_group(icao))
            if tl > 0:
                ratios.append(pl / tl)
    return float(np.mean(ratios)) if ratios else float("nan")


def deviation_prf1(flagged_mask, flagged_truth_ids, truth) -> dict:
    """P/R/F1 of a per-row deviation mask against `truth.jumped_ids`.

    `flagged_mask` and `flagged_truth_ids` are two length-N series
    aligned to the reconstructed rows; `flagged_truth_ids[i]` is the
    row's `__truth_id`, `flagged_mask[i]` is whether the detector
    flagged it. TP = (ids with mask=True) intersected with
    `truth.jumped_ids`. Returns zero precision/recall/F1 (not NaN) when
    `truth.jumped_ids` is empty, so Postgres gets a real number rather
    than NULL propagating downstream.
    """
    jumped = set(int(x) for x in truth.jumped_ids)
    if not jumped:
        return {"dev_precision": 0.0, "dev_recall": 0.0, "dev_f1": 0.0}
    mask = pd.Series(flagged_mask).reset_index(drop=True).fillna(False).astype(bool)
    ids  = pd.Series(flagged_truth_ids).reset_index(drop=True)
    flagged_ids = set(
        int(x) for x in ids[mask].dropna().tolist()
    )
    tp = len(flagged_ids & jumped)
    fp = len(flagged_ids - jumped)
    fn = len(jumped - flagged_ids)
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall    = tp / (tp + fn) if (tp + fn) else 0.0
    f1        = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
    return {"dev_precision": precision, "dev_recall": recall, "dev_f1": f1}


def continuity_ratio(df: pd.DataFrame, expected_step_s: float) -> float:
    """1 − (unexpected gaps) / (expected points − 1), per flight averaged.
    An unexpected gap is dt > 1.5 × expected_step_s."""
    if df.empty or "flight_id" not in df.columns:
        return float("nan")
    ratios = []
    for _, g in df.groupby("flight_id", sort=False):
        if len(g) < 2:
            continue
        dt = g["time"].diff().dropna()
        bad = (dt > 1.5 * expected_step_s).sum()
        ratios.append(1.0 - bad / max(len(g) - 1, 1))
    return float(np.mean(ratios)) if ratios else float("nan")


def deviation_prf(predicted_events: pd.DataFrame,
                  truth_events: pd.DataFrame,
                  tolerance_s: float = BOUNDARY_TOLERANCE_S) -> dict:
    """P/R/F1 of deviation events, matched by overlapping time windows."""
    if truth_events.empty and predicted_events.empty:
        return {"precision": 1.0, "recall": 1.0, "f1": 1.0,
                "tp": 0, "fp": 0, "fn": 0}
    tp = 0
    matched_truth = set()
    for _, p in predicted_events.iterrows():
        for ti, t in truth_events.iterrows():
            if ti in matched_truth:
                continue
            overlap = (p["start_time"] <= t["end_time"] + tolerance_s and
                       p["end_time"]   >= t["start_time"] - tolerance_s and
                       p["icao24"] == t["icao24"])
            if overlap:
                tp += 1
                matched_truth.add(ti)
                break
    fp = len(predicted_events) - tp
    fn = len(truth_events)     - tp
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall    = tp / (tp + fn) if (tp + fn) else 0.0
    f1        = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
    return {"precision": precision, "recall": recall, "f1": f1,
            "tp": tp, "fp": fp, "fn": fn}
