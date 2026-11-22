"""Compact a globally-unique small-hub table across multiple month batches.

See architecture.md section 2.4 for why ``hub_aircraft``, ``hub_airport``,
and ``sat_aircraft_identity`` are non-partitioned: each represents a global
entity (airframe, airport, aircraft identity) that persists across every
month. Instead of ``ym=`` partitioning, we union new-month rows into the
existing monolith and dedup on the business key.

Replaces the inline ``_compact_small_hub`` stub in ``raw_vault.py`` (T-08).
The 10-item contract pinned by the T-11 vault review section E:

  1. Signature compatibility with the T-08 inline call site.
  2. Returns int row count of the compacted monolith (not the delta).
  3. Existing-monolith rows are unioned FIRST so ``keep='first'`` preserves
     the earliest-seen ``rec_src`` tag (multi-source precedence from WI-1).
  4. ``new_frames=[]`` is a legal input and a true no-op when the monolith
     already exists -- the file is NOT rewritten, mtime is preserved.
  5. Atomic tmp + ``os.replace`` write; never leaves a partial monolith.
  6. Business-key dedup columns match ``_FINAL_DEDUP`` in ``raw_vault.py``
     (``hub_aircraft -> hk_aircraft``, ``hub_airport -> hk_airport``,
     ``sat_aircraft_identity -> hk_aircraft``).
  7. Dedup routes through numpy-object to avoid pyarrow's 2 GiB single-
     malloc ceiling on large accumulators (pandas 2.x pyarrow-backed
     string dtype).
  8. ``icao24`` stays pandas ``"string"`` dtype throughout; the dedup
     round-trip is the only place it may transit ``object``.
  9. Windows cp1252-safe: no arrows, em-dashes, or non-ASCII characters
     in any stdout path.
 10. Deterministic: same input frames and same existing monolith produce
     byte-identical output on repeated invocations.
"""
from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

__all__ = ["compact_small_hub", "DEDUP_KEYS"]


# Business-key dedup columns per non-partitioned hub. Kept in lock-step
# with ``etl4.vault.raw_vault._FINAL_DEDUP`` -- duplicated here (instead of
# imported) to avoid a circular import, because ``raw_vault.py`` imports
# ``compact_small_hub`` inside ``_compact_small_hub``. The T-09 smoke
# asserts this dict matches the production ``_FINAL_DEDUP`` slice for the
# three non-partitioned tables. TODO: hoist to a shared module when a
# second caller appears.
DEDUP_KEYS: dict[str, list[str]] = {
    "hub_aircraft":          ["hk_aircraft"],
    "hub_airport":           ["hk_airport"],
    "sat_aircraft_identity": ["hk_aircraft"],
}


def _dedup_safely(df: pd.DataFrame, subset: list[str]) -> pd.DataFrame:
    """``drop_duplicates`` without the pyarrow 2 GiB single-malloc limit.

    pandas ``"string"`` dtype is pyarrow-backed; its ``.factorize`` routes
    through ``pyarrow.ChunkedArray.dictionary_encode`` which pre-allocates
    a single buffer that fails at 2 GiB on large accumulators. Converting
    the key columns to numpy ``object`` first routes dedup through pandas'
    python-level hashtable, which has no such ceiling. Cast back to
    ``"string"`` after.
    """
    if df.empty:
        return df
    pa_cols = [c for c in subset if df[c].dtype == "string"]
    if pa_cols:
        df = df.astype({c: "object" for c in pa_cols})
    out = df.drop_duplicates(subset=subset, keep="first")
    if pa_cols:
        out = out.astype({c: "string" for c in pa_cols})
    return out


def _atomic_write(df: pd.DataFrame, target: Path) -> int:
    """Write ``df`` to ``target`` atomically via tmp + ``os.replace``.

    tmp lives in the same directory so the rename is a same-filesystem
    atomic operation on both Windows and POSIX. ``icao24``, when present,
    is explicitly cast back to pandas ``"string"`` before the pyarrow
    write defends against ``_dedup_safely``'s object round-trip leaving
    the column as ``object``.
    """
    target.parent.mkdir(parents=True, exist_ok=True)
    if "icao24" in df.columns:
        df = df.copy()
        df["icao24"] = df["icao24"].astype("string")
    tmp = target.parent / (".part.parquet.%d.tmp" % (os.getpid(),))
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, tmp, compression="snappy")
    os.replace(tmp, target)
    return len(df)


def compact_small_hub(
    name: str,
    new_frames: list[pd.DataFrame],
    out_dir: Path,
) -> int:
    """Union new-month frames into the existing small-hub monolith.

    Parameters
    ----------
    name :
        One of ``'hub_aircraft'``, ``'hub_airport'``, ``'sat_aircraft_identity'``.
    new_frames :
        Per-month hub frames collected inside the current ``_chunked_flightlist_vault``
        iteration. Empty frames are tolerated and filtered out.
    out_dir :
        Vault root. The monolith lives at ``<out_dir>/<name>/part.parquet``.

    Returns
    -------
    int
        Row count of the compacted monolith after the write (or of the
        untouched existing monolith when ``new_frames`` yields no rows).

    Notes
    -----
    Call-site behavior across the four "empty" cases:
      * existing monolith present, no non-empty new frames
        -> skip the write entirely; return the existing footer row count
        (mtime preserved -- a correct no-op per T-11 section A.4).
      * existing monolith present, at least one non-empty new frame
        -> union existing + new, dedup on business key with existing-first
        precedence, atomic rewrite.
      * no existing monolith, at least one non-empty new frame
        -> dedup the new frames alone, atomic write.
      * no existing monolith, no non-empty new frames
        -> write an empty monolith so downstream readers that
        ``stat()`` the file keep passing.
    """
    if name not in DEDUP_KEYS:
        raise ValueError(
            "compact_small_hub: unknown table %r; expected one of %s"
            % (name, sorted(DEDUP_KEYS))
        )
    out_dir = Path(out_dir)
    target = out_dir / name / "part.parquet"
    non_empty = [f for f in new_frames if not f.empty]

    if target.is_file() and not non_empty:
        return pq.ParquetFile(target).metadata.num_rows

    frames: list[pd.DataFrame] = []
    if target.is_file():
        frames.append(pd.read_parquet(target))
    frames.extend(non_empty)

    if not frames:
        return _atomic_write(pd.DataFrame(), target)

    stacked = pd.concat(frames, ignore_index=True)
    stacked = _dedup_safely(stacked, DEDUP_KEYS[name]).reset_index(drop=True)
    return _atomic_write(stacked, target)
