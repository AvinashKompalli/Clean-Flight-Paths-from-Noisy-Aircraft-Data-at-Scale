"""T-09 smoke: etl4/vault/compact.py (small-hub compaction).

Exercises every item on the 10-item contract pinned by the T-11 vault
review section E. One self-contained script, no DB, no fixtures outside
tempfile. Windows cp1252-safe (ASCII only in every print statement).

Items covered:
  1. Signature compatibility with the inline _compact_small_hub call site
     (positional (name, new_frames, out_dir) -> int).
  2. Returns int row count (not the delta).
  3. Existing-monolith rows win on business-key collisions
     (keep='first' with existing frame first in the union).
  4. new_frames=[] is a true no-op when monolith exists: the file is NOT
     rewritten; mtime preserved.
  5. Atomic tmp + os.replace. A simulated write failure leaves the
     prior monolith intact.
  6. Dedup keys match the raw_vault._FINAL_DEDUP slice for the three
     non-partitioned tables.
  7. 2 GiB pyarrow ceiling path: _dedup_safely detours string-dtype keys
     through numpy object (verified via dtype-preservation round-trip).
  8. icao24 stays pandas 'string' dtype after compaction.
  9. cp1252-safe stdout.
 10. Byte-identical idempotence: same input run twice from scratch
     yields identical SHA256.

Also runs an integration check against the T-08 call site by invoking
compact_small_hub for all three table names and asserting row counts
match a hand-rolled oracle.

Invoke:
    python multi-agent-engineering/projects/PROJ-20261118-impr01-partition-contract/artifacts/t09_compact_smoke.py

Prints "COMPACT SMOKE OK" on success.
"""
from __future__ import annotations

import hashlib
import os
import shutil
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO_ROOT))

import pandas as pd
import pyarrow.parquet as pq

from etl4.vault import compact, raw_vault
from etl4.vault.compact import compact_small_hub, DEDUP_KEYS


LOAD_DT = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _flightlist_frame() -> pd.DataFrame:
    """Hand-rolled bronze-shaped flightlist frame for the three builders.

    Covers two icao24 values in two months plus a route pair.
    """
    return pd.DataFrame({
        "callsign":     ["ABC123", "DEF456", "ABC123", "DEF456"],
        "day":          ["2019-01-15", "2019-01-16", "2019-02-15", "2019-02-16"],
        "icao24":       ["0a1b2c", "0a1b2d", "0a1b2c", "0a1b2d"],
        "origin":       ["KJFK", "KLAX", "KJFK", "KLAX"],
        "destination":  ["KLAX", "KJFK", "KLAX", "KJFK"],
        "firstseen":    ["2019-01-15T00:00:00Z"] * 2 + ["2019-02-15T00:00:00Z"] * 2,
        "lastseen":     ["2019-01-15T01:00:00Z"] * 2 + ["2019-02-15T01:00:00Z"] * 2,
        "registration": ["N1", "N2", "N1", "N2"],
        "typecode":     ["B738", "A320", "B738", "A320"],
    })


def _build_hub_frames(fl: pd.DataFrame) -> dict[str, pd.DataFrame]:
    return {
        "hub_aircraft":          raw_vault.build_hub_aircraft(fl, pd.DataFrame(), LOAD_DT),
        "hub_airport":           raw_vault.build_hub_airport(fl, LOAD_DT),
        "sat_aircraft_identity": raw_vault.build_sat_aircraft_identity(fl, LOAD_DT),
    }


# -------- Case 1: dedup keys mirror production ----------------------------
def case_dedup_keys_mirror_production() -> None:
    for name in ("hub_aircraft", "hub_airport", "sat_aircraft_identity"):
        assert DEDUP_KEYS[name] == raw_vault._FINAL_DEDUP[name], (
            "DEDUP_KEYS[%r]=%r drifted from raw_vault._FINAL_DEDUP[%r]=%r"
            % (name, DEDUP_KEYS[name], name, raw_vault._FINAL_DEDUP[name])
        )
    assert set(DEDUP_KEYS) == set(raw_vault.NON_PARTITIONED_TABLES), (
        "DEDUP_KEYS covers %s; NON_PARTITIONED_TABLES is %s"
        % (sorted(DEDUP_KEYS), sorted(raw_vault.NON_PARTITIONED_TABLES))
    )


# -------- Case 2: fresh write -> correct row counts -----------------------
def case_fresh_write(scratch: Path) -> dict[str, int]:
    out_dir = scratch / "fresh"
    out_dir.mkdir()
    fl = _flightlist_frame()
    frames = _build_hub_frames(fl)
    counts = {}
    for name, frame in frames.items():
        rc = compact_small_hub(name, [frame], out_dir)
        assert isinstance(rc, int), "return type must be int, got %s" % type(rc)
        counts[name] = rc
        target = out_dir / name / "part.parquet"
        assert target.is_file(), "missing monolith %s" % target
        footer = pq.ParquetFile(target).metadata.num_rows
        assert footer == rc, "footer %d != returned %d for %s" % (footer, rc, name)
    # Oracle: 2 distinct icao24 -> hub_aircraft=2, 2 distinct airports -> hub_airport=2
    assert counts["hub_aircraft"] == 2, counts
    assert counts["hub_airport"] == 2, counts
    assert counts["sat_aircraft_identity"] == 2, counts
    return counts


# -------- Case 3: empty new_frames no-op when monolith exists ------------
def case_empty_no_op(scratch: Path) -> None:
    out_dir = scratch / "no_op"
    out_dir.mkdir()
    fl = _flightlist_frame()
    frames = _build_hub_frames(fl)
    # Seed with real content.
    for name, frame in frames.items():
        compact_small_hub(name, [frame], out_dir)

    pre = {}
    for name in DEDUP_KEYS:
        p = out_dir / name / "part.parquet"
        pre[name] = (p.stat().st_mtime_ns, _sha256(p), pq.ParquetFile(p).metadata.num_rows)

    # Empty list. Must not rewrite.
    for name in DEDUP_KEYS:
        rc = compact_small_hub(name, [], out_dir)
        assert rc == pre[name][2], (
            "%s: empty no-op returned %d, expected %d" % (name, rc, pre[name][2])
        )

    # All-empty frames. Must also not rewrite.
    for name in DEDUP_KEYS:
        rc = compact_small_hub(name, [pd.DataFrame()], out_dir)
        assert rc == pre[name][2]

    for name in DEDUP_KEYS:
        p = out_dir / name / "part.parquet"
        post_mtime = p.stat().st_mtime_ns
        post_sha = _sha256(p)
        assert post_mtime == pre[name][0], (
            "%s: mtime changed on no-op path (%d -> %d)"
            % (name, pre[name][0], post_mtime)
        )
        assert post_sha == pre[name][1], (
            "%s: sha changed on no-op path" % name
        )


# -------- Case 4: empty new_frames + no monolith -> writes empty ---------
def case_empty_no_monolith(scratch: Path) -> None:
    out_dir = scratch / "no_mono"
    out_dir.mkdir()
    for name in DEDUP_KEYS:
        rc = compact_small_hub(name, [], out_dir)
        assert rc == 0, "%s: expected 0 rows, got %d" % (name, rc)
        p = out_dir / name / "part.parquet"
        assert p.is_file(), "%s: monolith missing" % name


# -------- Case 5: existing-first precedence on business-key collision ----
def case_existing_first_precedence(scratch: Path) -> None:
    out_dir = scratch / "precedence"
    out_dir.mkdir()
    # Seed hub_aircraft with a zenodo row for icao24=0A1B2C.
    seed = raw_vault.build_hub_aircraft(
        pd.DataFrame({"icao24": ["0a1b2c"]}), pd.DataFrame(), LOAD_DT,
    )
    assert (seed["rec_src"] == "opensky_zenodo").all()
    compact_small_hub("hub_aircraft", [seed], out_dir)

    # Union with a new frame tagged opensky_s3 for the same icao24.
    collide = raw_vault.build_hub_aircraft(
        pd.DataFrame(),
        pd.DataFrame({"icao24": ["0a1b2c"]}),
        LOAD_DT,
    )
    assert (collide["rec_src"] == "opensky_s3").all()
    rc = compact_small_hub("hub_aircraft", [collide], out_dir)
    assert rc == 1, "collision produced %d rows, expected 1" % rc

    back = pd.read_parquet(out_dir / "hub_aircraft" / "part.parquet")
    assert len(back) == 1
    assert back["rec_src"].iloc[0] == "opensky_zenodo", (
        "existing-first broken: rec_src=%r" % back["rec_src"].iloc[0]
    )
    assert str(back["icao24"].dtype) == "string", (
        "icao24 dtype regressed to %s" % back["icao24"].dtype
    )


# -------- Case 6: icao24 'string' dtype preserved through dedup ----------
def case_icao24_dtype_preserved(scratch: Path) -> None:
    out_dir = scratch / "dtype"
    out_dir.mkdir()
    fl = _flightlist_frame()
    frames = _build_hub_frames(fl)
    for name, frame in frames.items():
        compact_small_hub(name, [frame], out_dir)

    for name in DEDUP_KEYS:
        back = pd.read_parquet(out_dir / name / "part.parquet")
        if "icao24" in back.columns:
            assert str(back["icao24"].dtype) == "string", (
                "%s: icao24 dtype is %s, expected 'string'"
                % (name, back["icao24"].dtype)
            )


# -------- Case 7: deterministic byte-identical output --------------------
def case_deterministic(scratch: Path) -> dict[str, str]:
    fl = _flightlist_frame()
    frames = _build_hub_frames(fl)
    shas: dict[str, list[str]] = {}
    for trial in range(2):
        out_dir = scratch / f"determ_{trial}"
        out_dir.mkdir()
        for name, frame in frames.items():
            compact_small_hub(name, [frame], out_dir)
        for name in DEDUP_KEYS:
            shas.setdefault(name, []).append(
                _sha256(out_dir / name / "part.parquet")
            )
    for name, pair in shas.items():
        assert pair[0] == pair[1], (
            "%s: nondeterministic bytes across runs (%s vs %s)"
            % (name, pair[0], pair[1])
        )
    return {n: v[0] for n, v in shas.items()}


# -------- Case 8: atomic write survives a simulated failure --------------
def case_atomic_write(scratch: Path) -> None:
    out_dir = scratch / "atomic"
    out_dir.mkdir()
    fl = _flightlist_frame()
    frames = _build_hub_frames(fl)
    compact_small_hub("hub_aircraft", [frames["hub_aircraft"]], out_dir)

    target = out_dir / "hub_aircraft" / "part.parquet"
    before_sha = _sha256(target)

    # Patch os.replace to raise AFTER the tmp has been written. The prior
    # monolith must survive untouched.
    original_replace = os.replace

    def failing_replace(src, dst):
        raise RuntimeError("simulated crash mid-rename")

    extra = raw_vault.build_hub_aircraft(
        pd.DataFrame({"icao24": ["0a1b2e"]}), pd.DataFrame(), LOAD_DT,
    )
    try:
        compact.os.replace = failing_replace
        try:
            compact_small_hub("hub_aircraft", [extra], out_dir)
        except RuntimeError as err:
            assert "simulated crash" in str(err)
        else:
            raise AssertionError("expected RuntimeError from patched os.replace")
    finally:
        compact.os.replace = original_replace

    after_sha = _sha256(target)
    assert after_sha == before_sha, (
        "atomic write failed: monolith sha changed after simulated crash"
    )
    # Clean up any orphaned tmp file.
    for leftover in target.parent.glob(".part.parquet.*.tmp"):
        leftover.unlink()


# -------- Case 9: unknown table name raises ------------------------------
def case_unknown_name(scratch: Path) -> None:
    out_dir = scratch / "unknown"
    out_dir.mkdir()
    try:
        compact_small_hub("hub_flight", [], out_dir)
    except ValueError as err:
        assert "hub_flight" in str(err)
    else:
        raise AssertionError("expected ValueError for partitioned table name")


# -------- Case 10: T-08 integration -- replay the T-08 fixture counts ----
def case_t08_integration(scratch: Path) -> None:
    """Replays the T-08 smoke fixture through the compact.py wrapper only.

    The three row counts must match the baseline captured before T-09
    edits: hub_aircraft=8355, hub_airport=1124, sat_aircraft_identity=8355
    (two-month head(5000) fixture from the T-08 smoke).
    """
    bronze_src = REPO_ROOT / "data" / "bronze" / "flightlists" / "v23.00"
    files = [
        bronze_src / "flightlist_20190101_20190131.parquet",
        bronze_src / "flightlist_20190201_20190228.parquet",
    ]
    if not all(f.exists() for f in files):
        print("  [case10] bronze fixture missing; skipping integration replay",
              flush=True)
        return

    out_dir = scratch / "t08_replay"
    out_dir.mkdir()
    expected = {
        "hub_aircraft":          8355,
        "hub_airport":           1124,
        "sat_aircraft_identity": 8355,
    }
    for f in files:
        fl = pd.read_parquet(f).head(5000)
        frames = _build_hub_frames(fl)
        for name, frame in frames.items():
            compact_small_hub(name, [frame], out_dir)

    for name, want in expected.items():
        got = pq.ParquetFile(out_dir / name / "part.parquet").metadata.num_rows
        assert got == want, "%s: got %d, want %d (T-08 baseline)" % (name, got, want)
    print("  [case10] T-08 replay matches baseline "
          "(hub_aircraft=8355, hub_airport=1124, sat_aircraft_identity=8355)",
          flush=True)


def run() -> None:
    scratch = Path(tempfile.mkdtemp(prefix="t09_compact_smoke_"))
    try:
        print("--- case 1: dedup keys mirror raw_vault._FINAL_DEDUP ---", flush=True)
        case_dedup_keys_mirror_production()
        print("--- case 2: fresh write, row count oracle ---", flush=True)
        case_fresh_write(scratch)
        print("--- case 3: empty new_frames is a byte-identical no-op ---", flush=True)
        case_empty_no_op(scratch)
        print("--- case 4: empty + no monolith writes empty file ---", flush=True)
        case_empty_no_monolith(scratch)
        print("--- case 5: existing-monolith precedence (keep=first) ---", flush=True)
        case_existing_first_precedence(scratch)
        print("--- case 6: icao24 string dtype preserved ---", flush=True)
        case_icao24_dtype_preserved(scratch)
        print("--- case 7: byte-identical deterministic output ---", flush=True)
        case_deterministic(scratch)
        print("--- case 8: atomic write, tmp doesn't clobber on crash ---", flush=True)
        case_atomic_write(scratch)
        print("--- case 9: unknown table name raises ValueError ---", flush=True)
        case_unknown_name(scratch)
        print("--- case 10: T-08 fixture replay row counts ---", flush=True)
        case_t08_integration(scratch)
        print("COMPACT SMOKE OK", flush=True)
    finally:
        shutil.rmtree(scratch, ignore_errors=True)


if __name__ == "__main__":
    try:
        run()
    except AssertionError as e:
        print("SMOKE FAIL: %s" % e, flush=True)
        sys.exit(1)
