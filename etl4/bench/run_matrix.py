"""Run the full (method x noise) experiment matrix on one dataset.

Two-pass medallion-aware runner:

  Pass 1 - prepare silver ONCE per noise profile (project + inject + clean +
           event-time-order), write parquet to data/silver/cleaned_states.
           Cached in-memory as {noise: silver_df} so pass 2 does not re-run
           the noise-dependent stages per method.

  Pass 2 - reconstruct (features + segment + gold write + deviations +
           metrics) for each (noise, method) combo.  Independent, so we
           run them in a ProcessPool over `--workers` (default 4) workers;
           each worker reads the silver parquet the main process wrote.

Writes:
  data/silver/cleaned_states/<source>/<noise>/part.parquet
  data/gold/marts/reconstructions/<source>/<noise>/method=<method>/part.parquet
  Postgres pipeline_runs + eval_results rows (via write_run)
  data/eval/matrix.csv                       (legacy convenience view)

Usage:
  python etl4/bench/run_matrix.py --bronze data/bronze/raw_states --source zenodo
  python etl4/bench/run_matrix.py --bronze ... --workers 1   # force sequential
"""
from __future__ import annotations

import argparse
import itertools
import re
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pandas as pd

from etl4.bench.run_experiment import prepare_silver, reconstruct_from_silver
from etl4.sink.postgres import write_run
from etl4.vault import watermark

ROOT = Path(__file__).resolve().parents[2]

SILVER_DIR = ROOT / "data" / "silver" / "cleaned_states"
GOLD_DIR   = ROOT / "data" / "gold"   / "marts" / "reconstructions"
MATRIX_CSV = ROOT / "data" / "eval"   / "matrix.csv"
SYNTHETIC_TRUTH_DIR = ROOT / "etl4" / "data" / "reference" / "synthetic_truth"

METHODS = ["time_gap", "kinematic", "kinematic_smoothing"]
NOISE_PROFILES = ["N0", "N1", "N2", "N3"]

REAL_SOURCES = ("s3", "zenodo", "rest")
SYNTHETIC_YM_DEFAULT = "SYN-002A"  # DDL backfill literal, architecture section 2.3
_YM_CALENDAR_RE = re.compile(r"^[0-9]{4}-(0[1-9]|1[0-2])$")


def _derive_synthetic_truth(bronze_path: Path) -> Path | None:
    """Infer the synthetic truth parquet from the bronze tree.

    Bronze layout is `<root>/date=YYYY-MM-DD/hour=H/part*.parquet`; the
    matching truth lives at `etl4/data/reference/synthetic_truth/<date>.parquet`.
    Returns None when the date token is absent or ambiguous so callers can
    fall back to the original --truth=None path.
    """
    dates: set[str] = set()
    for part in bronze_path.parts:
        if part.startswith("date="):
            dates.add(part[len("date="):])
    if not dates and bronze_path.is_dir():
        for child in bronze_path.iterdir():
            if child.is_dir() and child.name.startswith("date="):
                dates.add(child.name[len("date="):])
    if len(dates) != 1:
        return None
    return SYNTHETIC_TRUTH_DIR / f"{dates.pop()}.parquet"


def _load(path: Path, ym: str | None = None) -> pd.DataFrame:
    """Load bronze parquet(s) under ``path``. If ``ym`` is a calendar
    YYYY-MM and the tree has ``date=`` subdirectories, restrict the read
    to files under ``date={ym}-*``. Synthetic bronze is typically flat and
    uses the SYN sentinel; no filter is applied in that case.
    """
    if path.is_file():
        return pd.read_parquet(path)
    if ym and _YM_CALENDAR_RE.match(ym) and any(path.glob("date=*")):
        files = sorted(path.glob(f"date={ym}-*/**/*.parquet"))
        if not files:
            raise SystemExit(
                f"error: no parquet under {path} for ym={ym} "
                f"(looked for date={ym}-*)"
            )
    else:
        files = sorted(path.rglob("*.parquet"))
    if not files:
        raise SystemExit(f"error: no parquet under {path}")
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


def _silver_path(source: str, noise: str) -> Path:
    return SILVER_DIR / source / noise / "part.parquet"


def _worker(args: tuple) -> dict:
    """ProcessPool entry: read silver parquet, run method, return metrics."""
    silver_path, truth_path, method, noise, source, rows_in, noise_truth = args
    silver = pd.read_parquet(silver_path)
    truth  = pd.read_parquet(truth_path) if truth_path else None
    return reconstruct_from_silver(
        silver, method, noise, source,
        truth=truth, rows_in=rows_in,
        write_gold_dir=GOLD_DIR,
        noise_truth=noise_truth,
    )


def main():
    p = argparse.ArgumentParser()
    # --bronze is validated manually below so --ym errors surface first for
    # real sources (argparse required= would fire ahead of our ym check).
    p.add_argument("--bronze")
    p.add_argument("--truth",  default=None)
    p.add_argument("--source", default="zenodo",
                   choices=["synthetic", "rest", "zenodo", "s3"])
    p.add_argument("--ym", default=None,
                   help="Partition month YYYY-MM; required when "
                        "--source is s3|zenodo|rest. Synthetic defaults "
                        "to SYN-002A (architecture section 2.3).")
    p.add_argument("--profiles", nargs="+", default=NOISE_PROFILES)
    p.add_argument("--methods",  nargs="+", default=METHODS)
    p.add_argument("--workers",  type=int, default=4,
                   help="parallel workers for pass 2 reconstruction")
    a = p.parse_args()

    # Source-driven ym validation. Real sources must provide a calendar
    # YYYY-MM; synthetic uses the SYN-<seed_hex> sentinel. This check runs
    # BEFORE --bronze validation so operators see the actionable error.
    if a.source in REAL_SOURCES:
        if not a.ym:
            p.error(f"--ym required when --source is one of "
                    f"{'|'.join(REAL_SOURCES)}")
        if not _YM_CALENDAR_RE.match(a.ym):
            p.error(f"--ym must be YYYY-MM for --source={a.source}; "
                    f"got {a.ym!r}")
    elif a.source == "synthetic":
        if a.ym is None:
            a.ym = SYNTHETIC_YM_DEFAULT

    if not a.bronze:
        p.error("--bronze required")

    if a.truth is None and a.source == "synthetic":
        derived = _derive_synthetic_truth(Path(a.bronze))
        if derived is None:
            print("WARNING: could not auto-derive --truth from --bronze "
                  "(no unique date= token); PDF metrics on synthetic cells "
                  "will be NaN", flush=True)
        elif derived.exists():
            a.truth = str(derived)
            print(f"auto-derived --truth from --bronze: {derived}", flush=True)
        else:
            print(f"WARNING: auto-derived --truth path missing: {derived}; "
                  "proceeding without truth frame", flush=True)

    bronze = _load(Path(a.bronze), ym=a.ym)
    rows_in = int(len(bronze))
    print(f"loaded {rows_in:,} bronze rows  ym={a.ym}", flush=True)

    # ---- Pass 1: silver cache, one write per noise profile ----
    print(f"\n[pass 1] prepare silver for {len(a.profiles)} noise profile(s)",
          flush=True)
    t1 = time.perf_counter()
    noise_truths: dict = {}
    for noise in a.profiles:
        tn = time.perf_counter()
        silver, _rejected, noise_truth = prepare_silver(
            bronze, noise, a.source, write_silver_dir=SILVER_DIR,
        )
        # Cache the frozen NoiseTruth so pass-2 workers receive the real
        # jumped_ids / shuffled_ids / duplicated_ids from pass 1. The ADR
        # says "no disk persistence"; in-memory hand-off through pickled
        # ProcessPool args is in-memory. Scope to synthetic only -- real
        # sources either skip inject_noise entirely (N0) or produce a
        # NoiseTruth with no ground-truth interpretation.
        noise_truths[noise] = noise_truth if a.source == "synthetic" else None
        print(f"  silver  noise={noise}  rows={len(silver):,}  "
              f"wall={time.perf_counter() - tn:.1f}s  -> "
              f"{_silver_path(a.source, noise).relative_to(ROOT)}", flush=True)
        del silver
    print(f"[pass 1] done in {time.perf_counter() - t1:.1f}s", flush=True)
    del bronze  # free before parallel reconstruction

    # ---- Pass 2: reconstruct every (noise, method) combo ----
    tasks = [
        (
            str(_silver_path(a.source, noise)),
            a.truth,
            method,
            noise,
            a.source,
            rows_in,
            noise_truths.get(noise),
        )
        for noise, method in itertools.product(a.profiles, a.methods)
    ]
    workers = max(1, min(a.workers, len(tasks)))
    print(f"\n[pass 2] {len(tasks)} reconstruct tasks, workers={workers}",
          flush=True)
    t2 = time.perf_counter()

    rows = []
    run_ids: list[int | None] = []

    def _record(m: dict):
        """Write Postgres run + append matrix.csv row."""
        run_id = write_run(
            {
                **m,
                "source":  a.source,
                "method":  m["method"],
                "noise":   m["noise"],
                "rows_in": rows_in,
            },
            ym=a.ym,
        )
        run_ids.append(run_id)
        if run_id:
            print(f"    [postgres run_id={run_id}]", flush=True)
        def _r(key, digits):
            v = m.get(key)
            if v is None:
                return float("nan")
            try:
                return round(float(v), digits)
            except (TypeError, ValueError):
                return float("nan")
        rows.append({
            "noise":      m["noise"],
            "method":     m["method"],
            "wall_s":     m["wall_s"],
            "flights":    m["n_flights"],
            "deviations": m["n_deviations"],
            "continuity": round(m["continuity"], 4),
            "rows_out":   m["rows_out"],
            "rmse_m":     _r("position_rmse_m", 1),
            "seg_f1":     _r("seg_f1", 3),
            "coverage":   _r("coverage_ratio", 3),
        })
        MATRIX_CSV.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(rows).to_csv(MATRIX_CSV, index=False)

    if workers == 1:
        for t in tasks:
            print(f"  >>> noise={t[3]}  method={t[2]}", flush=True)
            m = _worker(t)
            _record(m)
            print(f"      wall={m['wall_s']}s  flights={m['n_flights']}  "
                  f"rows_out={m['rows_out']}", flush=True)
    else:
        with ProcessPoolExecutor(max_workers=workers) as ex:
            futs = {ex.submit(_worker, t): t for t in tasks}
            for fut in as_completed(futs):
                t = futs[fut]
                m = fut.result()
                print(f"  done  noise={t[3]:>3}  method={t[2]:<20}  "
                      f"wall={m['wall_s']}s  flights={m['n_flights']}",
                      flush=True)
                _record(m)

    print(f"[pass 2] done in {time.perf_counter() - t2:.1f}s", flush=True)

    _advance_matrix_watermark(a.ym, run_ids)

    df = pd.DataFrame(rows).sort_values(["noise", "method"]).reset_index(drop=True)
    print(f"\nwrote {MATRIX_CSV.relative_to(ROOT)}\n")
    print(df.to_string(index=False))


def _advance_matrix_watermark(ym: str, run_ids: list) -> None:
    """Advance ('matrix', 'pipeline_runs') for calendar-month ym only.

    The DDL CHECK on ``pipeline_watermarks.last_ym`` rejects the synthetic
    SYN-<seed_hex> sentinel by design (architecture section 2.3, DDL
    comment at create_tables.sql line 137). Synthetic cells therefore skip
    the watermark write entirely -- the matrix watermark only tracks real
    calendar partitions. Also skip if the Postgres sink was unreachable
    (any None in run_ids) so a dev run without the DB does not advance a
    watermark for cells that never landed.
    """
    if not _YM_CALENDAR_RE.match(ym or ""):
        print(f"  [matrix watermark: skipped for ym={ym} "
              f"(synthetic sentinel; non-calendar)]", flush=True)
        return
    if not run_ids or any(rid is None for rid in run_ids):
        print("  [matrix watermark: skipped (sink unreachable or no cells)]",
              flush=True)
        return
    import psycopg
    with psycopg.connect(watermark.DSN_DEFAULT, connect_timeout=3) as conn:
        current = watermark.get("matrix", "pipeline_runs")
        if current is not None and _YM_CALENDAR_RE.match(current) \
                and ym < current:
            print(f"  [matrix watermark: ym={ym} < current={current}; "
                  f"skipping to preserve high-water]", flush=True)
            return
        watermark.set("matrix", "pipeline_runs", ym,
                      len(run_ids), conn)
        conn.commit()
    print(f"  [matrix watermark advanced: ('matrix', 'pipeline_runs') "
          f"-> ym={ym}, row_count={len(run_ids)}]", flush=True)


if __name__ == "__main__":
    main()
