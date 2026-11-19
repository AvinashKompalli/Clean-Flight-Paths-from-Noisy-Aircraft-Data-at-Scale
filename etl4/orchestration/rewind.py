"""Idempotent partition + watermark rewind (IMPR-01, architecture section 7).

Rolls one or more pipeline layers back to before a specified ``ym`` range so
the next pipeline invocation re-does only those months. The watermark rows
under ``pipeline_watermarks`` carry the authoritative high-water mark per
(layer, table_name); the filesystem under ``data/silver/vault/``,
``data/gold/marts/`` and the Postgres tables ``fact_trajectory_enriched`` and
``pipeline_runs`` carry the materialized partitions. Rewinding deletes the
partitions, then rolls the watermark back to the calendar month immediately
before ``--from``, so the vault / marts / sink / matrix builders rebuild
from that point.

Order of operations per layer (SAFE ORDER):
    1. delete filesystem partitions OR delete Postgres rows
    2. reset ``pipeline_watermarks.last_ym`` to ``month_minus_one(from_ym)``
       for every row under the targeted layer whose current ``last_ym``
       is >= ``from_ym`` (never rolls a watermark forward)

Filesystem deletes run BEFORE the Postgres watermark rollback. A crash
between the two leaves the watermark ahead of what is materialized on disk;
the ``run_e2e`` drift check (FR-17 / T-22) catches that and the operator
reruns the rewind. The reverse order would leave the watermark BEHIND real
data, which the builder would silently skip -- data missing forever.

Layer semantics:

* ``vault``   -- ``rm -rf data/silver/vault/<tbl>/ym=<ym>/`` for every
                 partitioned vault table in ``PARTITIONED_VAULT``; reset
                 ``layer='vault'`` watermarks.
* ``marts``   -- same for ``data/gold/marts/<fact>/ym=<ym>/`` under
                 ``PARTITIONED_MARTS``; reset ``layer='marts'`` watermarks.
* ``sink``    -- ``DELETE FROM fact_trajectory_enriched WHERE ym BETWEEN
                 from_ym AND to_ym``; reset ``layer='sink'`` watermarks.
                 Non-partitioned ``dim_aircraft`` is never deleted (airframes
                 persist across months).
* ``matrix``  -- ``DELETE FROM pipeline_runs WHERE ym BETWEEN from_ym AND
                 to_ym``; ``eval_results`` + ``deviation_events`` cascade.
                 Reset ``layer='matrix'`` watermarks.
* ``eval_mart`` -- reset ``layer='eval_mart'`` watermarks only. The
                 ``fact_experiment_result`` shape is latest-per-cell
                 (architecture section 4.5, MAX(ym) over pipeline_runs);
                 dropping rows for a ym range would wipe cells from months
                 outside the range. Re-run ``stage_eval_mart`` AFTER the
                 matrix rewind to rebuild it. (Under-specified in the task
                 spec; see handoff RISKS.)
* ``all``     -- chain in downstream-first order so no step leaves orphan
                 upstream data visible to a mid-rewind reader. Order is
                 ``eval_mart -> matrix -> sink -> marts -> vault``. The
                 architecture section 7 rationale ("downstream-first
                 deletion so upstream rebuilds never leave orphans")
                 overrides the dispatch's forward-order listing.
                 ``--yes`` REQUIRED for this layer; the CLI exits non-zero
                 without it.

Dry-run (``--dry-run``) prints the plan with row counts / file sizes and
exits 0 without touching anything.

Usage:
    python -m etl4.orchestration.rewind --layer vault --from 2022-03 --to 2022-05
    python -m etl4.orchestration.rewind --layer all  --from 2022-03 --to 2022-05 --yes
    python -m etl4.orchestration.rewind --layer sink --from 2022-01 --to 2022-01 --dry-run

Windows cp1252-safe: no arrows, no em-dashes in any stdout line.
"""
from __future__ import annotations

import argparse
import os
import re
import shutil
import sys
from pathlib import Path
from typing import List, Optional

import psycopg

__all__ = [
    "LAYERS",
    "PARTITIONED_VAULT",
    "PARTITIONED_MARTS",
    "SILVER_VAULT_ROOT",
    "MARTS_ROOT",
    "months_between",
    "month_minus_one",
    "rewind_vault",
    "rewind_marts",
    "rewind_sink",
    "rewind_matrix",
    "rewind_eval_mart",
    "rewind_all",
    "main",
]

ROOT = Path(__file__).resolve().parents[2]
SILVER_VAULT_ROOT = ROOT / "data" / "silver" / "vault"
MARTS_ROOT        = ROOT / "data" / "gold"   / "marts"

LAYERS = ("vault", "marts", "sink", "matrix", "eval_mart", "all")

PARTITIONED_VAULT = (
    "hub_flight",
    "link_flight_aircraft",
    "link_flight_route",
    "sat_flight_summary",
    "sat_state_observation",
)
PARTITIONED_MARTS = (
    "fact_flight",
    "fact_trajectory_enriched",
)

DSN_DEFAULT = os.getenv(
    "POSTGRES_DSN",
    "postgresql://etl4:change_me@localhost:5432/etl4",
)

_YM_RE = re.compile(r"^[0-9]{4}-(0[1-9]|1[0-2])$")


def _validate_ym(ym: str, label: str) -> None:
    if not isinstance(ym, str) or not _YM_RE.match(ym):
        raise ValueError(
            "%s must match YYYY-MM with month in 01..12; got %r"
            % (label, ym)
        )


def months_between(from_ym: str, to_ym: str) -> List[str]:
    """Inclusive calendar-month range, ``YYYY-MM`` strings sorted ascending."""
    _validate_ym(from_ym, "--from")
    _validate_ym(to_ym, "--to")
    if from_ym > to_ym:
        raise ValueError(
            "--from %s must be <= --to %s (inclusive range)"
            % (from_ym, to_ym)
        )
    y1, m1 = int(from_ym[:4]), int(from_ym[5:7])
    y2, m2 = int(to_ym[:4]),   int(to_ym[5:7])
    out: List[str] = []
    y, m = y1, m1
    while (y, m) <= (y2, m2):
        out.append("%04d-%02d" % (y, m))
        m += 1
        if m == 13:
            m = 1
            y += 1
    return out


def month_minus_one(ym: str) -> str:
    """Return the calendar month immediately before ``ym``."""
    _validate_ym(ym, "ym")
    y, m = int(ym[:4]), int(ym[5:7])
    m -= 1
    if m == 0:
        m = 12
        y -= 1
    return "%04d-%02d" % (y, m)


def _dir_size(path: Path) -> int:
    total = 0
    if not path.exists():
        return 0
    for p in path.rglob("*"):
        if p.is_file():
            try:
                total += p.stat().st_size
            except OSError:
                pass
    return total


def _reset_layer_watermark(
    conn,
    layer: str,
    from_ym: str,
    *,
    table_filter: Optional[tuple] = None,
) -> int:
    """Roll back ``pipeline_watermarks.last_ym`` to ``month_minus_one(from_ym)``
    for the specified ``layer`` on any row whose current ``last_ym`` is
    >= ``from_ym``. Never rolls forward. Returns rowcount.

    When ``table_filter`` is given, only those ``table_name`` values are
    touched -- used so the sink rewind does not disturb ``dim_aircraft``
    (non-partitioned, latest-wins).
    """
    prev = month_minus_one(from_ym)
    with conn.cursor() as cur:
        if table_filter is None:
            cur.execute(
                "UPDATE pipeline_watermarks "
                "   SET last_ym = %s, updated_at = now() "
                " WHERE layer = %s AND last_ym >= %s",
                (prev, layer, from_ym),
            )
        else:
            cur.execute(
                "UPDATE pipeline_watermarks "
                "   SET last_ym = %s, updated_at = now() "
                " WHERE layer = %s "
                "   AND table_name = ANY(%s) "
                "   AND last_ym >= %s",
                (prev, layer, list(table_filter), from_ym),
            )
        return cur.rowcount


def rewind_vault(
    from_ym: str,
    to_ym: str,
    *,
    dry_run: bool = False,
    root: Path = SILVER_VAULT_ROOT,
    dsn: str = DSN_DEFAULT,
) -> dict:
    """Delete ``ym=YYYY-MM`` partitions across ``PARTITIONED_VAULT`` and
    reset the vault watermarks."""
    yms = months_between(from_ym, to_ym)
    plan: List[dict] = []
    total_bytes = 0
    for tbl in PARTITIONED_VAULT:
        for ym in yms:
            p = Path(root) / tbl / ("ym=" + ym)
            if p.is_dir():
                sz = _dir_size(p)
                total_bytes += sz
                plan.append({"table": tbl, "ym": ym, "path": str(p),
                             "bytes": sz})

    print("[rewind vault] from=%s to=%s dry_run=%s" %
          (from_ym, to_ym, dry_run), flush=True)
    for entry in plan:
        print("  would delete: %s (%d bytes)" %
              (entry["path"], entry["bytes"]), flush=True)
    print("  total: %d dirs, %d bytes" %
          (len(plan), total_bytes), flush=True)

    if dry_run:
        print("  [dry-run] no filesystem or Postgres changes", flush=True)
        return {"layer": "vault", "dirs": len(plan),
                "bytes": total_bytes, "watermarks_reset": 0,
                "dry_run": True}

    for entry in plan:
        shutil.rmtree(entry["path"])
        print("  deleted: %s" % (entry["path"],), flush=True)

    with psycopg.connect(dsn, connect_timeout=5) as conn:
        try:
            n = _reset_layer_watermark(conn, "vault", from_ym,
                                       table_filter=PARTITIONED_VAULT)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    print("  watermark reset: layer=vault rows_affected=%d last_ym=%s" %
          (n, month_minus_one(from_ym)), flush=True)
    return {"layer": "vault", "dirs": len(plan),
            "bytes": total_bytes, "watermarks_reset": n,
            "dry_run": False}


def rewind_marts(
    from_ym: str,
    to_ym: str,
    *,
    dry_run: bool = False,
    root: Path = MARTS_ROOT,
    dsn: str = DSN_DEFAULT,
) -> dict:
    """Delete ``ym=YYYY-MM`` partitions across ``PARTITIONED_MARTS`` and
    reset the mart watermarks. ``dim_*`` monoliths are untouched."""
    yms = months_between(from_ym, to_ym)
    plan: List[dict] = []
    total_bytes = 0
    for tbl in PARTITIONED_MARTS:
        for ym in yms:
            p = Path(root) / tbl / ("ym=" + ym)
            if p.is_dir():
                sz = _dir_size(p)
                total_bytes += sz
                plan.append({"table": tbl, "ym": ym, "path": str(p),
                             "bytes": sz})

    print("[rewind marts] from=%s to=%s dry_run=%s" %
          (from_ym, to_ym, dry_run), flush=True)
    for entry in plan:
        print("  would delete: %s (%d bytes)" %
              (entry["path"], entry["bytes"]), flush=True)
    print("  total: %d dirs, %d bytes" %
          (len(plan), total_bytes), flush=True)

    if dry_run:
        print("  [dry-run] no filesystem or Postgres changes", flush=True)
        return {"layer": "marts", "dirs": len(plan),
                "bytes": total_bytes, "watermarks_reset": 0,
                "dry_run": True}

    for entry in plan:
        shutil.rmtree(entry["path"])
        print("  deleted: %s" % (entry["path"],), flush=True)

    with psycopg.connect(dsn, connect_timeout=5) as conn:
        try:
            n = _reset_layer_watermark(conn, "marts", from_ym,
                                       table_filter=PARTITIONED_MARTS)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    print("  watermark reset: layer=marts rows_affected=%d last_ym=%s" %
          (n, month_minus_one(from_ym)), flush=True)
    return {"layer": "marts", "dirs": len(plan),
            "bytes": total_bytes, "watermarks_reset": n,
            "dry_run": False}


def rewind_sink(
    from_ym: str,
    to_ym: str,
    *,
    dry_run: bool = False,
    dsn: str = DSN_DEFAULT,
) -> dict:
    """DELETE from ``fact_trajectory_enriched`` for the ym range in one
    transaction with the sink watermark reset."""
    _validate_ym(from_ym, "--from")
    _validate_ym(to_ym, "--to")

    print("[rewind sink] from=%s to=%s dry_run=%s" %
          (from_ym, to_ym, dry_run), flush=True)

    with psycopg.connect(dsn, connect_timeout=5) as conn:
        if dry_run:
            # Tolerate a pre-migration DB so the full --layer all --dry-run
            # chain plan always prints. Never swallow the same error in a
            # real run -- a missing column must fail loudly below.
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT count(*) FROM fact_trajectory_enriched "
                        " WHERE ym BETWEEN %s AND %s",
                        (from_ym, to_ym),
                    )
                    row = cur.fetchone()
                    row_count = int(row[0]) if row else 0
                print("  would delete: fact_trajectory_enriched rows=%d" %
                      (row_count,), flush=True)
            except psycopg.errors.UndefinedColumn:
                conn.rollback()
                row_count = 0
                print("  would delete: fact_trajectory_enriched.ym "
                      "column missing (run the section 6.1 schema "
                      "migration first); 0 rows planned",
                      flush=True)
            print("  [dry-run] no Postgres changes", flush=True)
            return {"layer": "sink", "rows_deleted": 0,
                    "would_delete": row_count,
                    "watermarks_reset": 0, "dry_run": True}

        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM fact_trajectory_enriched "
                " WHERE ym BETWEEN %s AND %s",
                (from_ym, to_ym),
            )
            row = cur.fetchone()
            row_count = int(row[0]) if row else 0
        print("  would delete: fact_trajectory_enriched rows=%d" %
              (row_count,), flush=True)

        try:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM fact_trajectory_enriched "
                    " WHERE ym BETWEEN %s AND %s",
                    (from_ym, to_ym),
                )
                deleted = cur.rowcount
            n = _reset_layer_watermark(
                conn, "sink", from_ym,
                table_filter=("fact_trajectory_enriched",),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

        print("  deleted: %d rows from fact_trajectory_enriched" %
              (deleted,), flush=True)
    print("  watermark reset: layer=sink rows_affected=%d last_ym=%s" %
          (n, month_minus_one(from_ym)), flush=True)
    return {"layer": "sink", "rows_deleted": deleted,
            "watermarks_reset": n, "dry_run": False}


def rewind_matrix(
    from_ym: str,
    to_ym: str,
    *,
    dry_run: bool = False,
    dsn: str = DSN_DEFAULT,
) -> dict:
    """DELETE from ``pipeline_runs`` for the ym range in one transaction
    with the matrix watermark reset. ``eval_results`` + ``deviation_events``
    cascade via the ``ON DELETE CASCADE`` on their FKs to pipeline_runs."""
    _validate_ym(from_ym, "--from")
    _validate_ym(to_ym, "--to")

    print("[rewind matrix] from=%s to=%s dry_run=%s" %
          (from_ym, to_ym, dry_run), flush=True)

    with psycopg.connect(dsn, connect_timeout=5) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM pipeline_runs "
                " WHERE ym BETWEEN %s AND %s",
                (from_ym, to_ym),
            )
            row = cur.fetchone()
            run_count = int(row[0]) if row else 0
            cur.execute(
                "SELECT count(*) FROM eval_results er "
                "  JOIN pipeline_runs pr USING (run_id) "
                " WHERE pr.ym BETWEEN %s AND %s",
                (from_ym, to_ym),
            )
            row = cur.fetchone()
            eval_count = int(row[0]) if row else 0
        print("  would delete: pipeline_runs rows=%d "
              "(eval_results cascade rows=%d)" %
              (run_count, eval_count), flush=True)

        if dry_run:
            print("  [dry-run] no Postgres changes", flush=True)
            return {"layer": "matrix", "rows_deleted": 0,
                    "would_delete": run_count,
                    "cascade_rows": eval_count,
                    "watermarks_reset": 0, "dry_run": True}

        try:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM pipeline_runs "
                    " WHERE ym BETWEEN %s AND %s",
                    (from_ym, to_ym),
                )
                deleted = cur.rowcount
            n = _reset_layer_watermark(conn, "matrix", from_ym)
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    print("  deleted: %d rows from pipeline_runs "
          "(eval_results + deviation_events cascaded)" %
          (deleted,), flush=True)
    print("  watermark reset: layer=matrix rows_affected=%d last_ym=%s" %
          (n, month_minus_one(from_ym)), flush=True)
    return {"layer": "matrix", "rows_deleted": deleted,
            "watermarks_reset": n, "dry_run": False}


def rewind_eval_mart(
    from_ym: str,
    to_ym: str,
    *,
    dry_run: bool = False,
    dsn: str = DSN_DEFAULT,
) -> dict:
    """Reset the eval_mart watermarks only.

    ``fact_experiment_result`` is grouped latest-per-cell across all ym
    (architecture section 4.5, MAX(ym) over pipeline_runs). Dropping its
    rows for a ym range would wipe cells whose latest observation lives
    outside the range. The rebuild path is a subsequent ``stage_eval_mart``
    invocation AFTER the matrix rewind -- MAX(ym) then settles to the
    pre-rewind value automatically.
    """
    _validate_ym(from_ym, "--from")
    _validate_ym(to_ym, "--to")

    print("[rewind eval_mart] from=%s to=%s dry_run=%s" %
          (from_ym, to_ym, dry_run), flush=True)
    print("  note: fact_experiment_result rows NOT deleted "
          "(latest-per-cell shape); re-run stage_eval_mart "
          "after matrix rewind to resettle MAX(ym)", flush=True)

    if dry_run:
        print("  [dry-run] no Postgres changes", flush=True)
        return {"layer": "eval_mart", "watermarks_reset": 0,
                "dry_run": True}

    with psycopg.connect(dsn, connect_timeout=5) as conn:
        try:
            n = _reset_layer_watermark(conn, "eval_mart", from_ym)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    print("  watermark reset: layer=eval_mart rows_affected=%d last_ym=%s" %
          (n, month_minus_one(from_ym)), flush=True)
    return {"layer": "eval_mart", "watermarks_reset": n,
            "dry_run": False}


def rewind_all(
    from_ym: str,
    to_ym: str,
    *,
    yes: bool = False,
    dry_run: bool = False,
    dsn: str = DSN_DEFAULT,
) -> List[dict]:
    """Chain all five layers in downstream-first order.

    Order: eval_mart -> matrix -> sink -> marts -> vault. Downstream-first
    deletion keeps mid-rewind readers from seeing orphan upstream rows
    (architecture section 7 rationale).
    """
    if not yes:
        raise SystemExit(
            "--layer all is destructive across every layer; "
            "re-run with --yes to confirm"
        )
    print("[rewind all] from=%s to=%s yes=%s dry_run=%s" %
          (from_ym, to_ym, yes, dry_run), flush=True)
    print("  chain: eval_mart -> matrix -> sink -> marts -> vault "
          "(downstream-first)", flush=True)
    results: List[dict] = []
    results.append(rewind_eval_mart(from_ym, to_ym, dry_run=dry_run, dsn=dsn))
    results.append(rewind_matrix   (from_ym, to_ym, dry_run=dry_run, dsn=dsn))
    results.append(rewind_sink     (from_ym, to_ym, dry_run=dry_run, dsn=dsn))
    results.append(rewind_marts    (from_ym, to_ym, dry_run=dry_run, dsn=dsn))
    results.append(rewind_vault    (from_ym, to_ym, dry_run=dry_run, dsn=dsn))
    print("[rewind all] complete: %d layers processed" %
          (len(results),), flush=True)
    return results


def _parse_argv(argv: Optional[list] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="etl4.orchestration.rewind",
        description="Rewind one or more pipeline layers to before a ym range.",
    )
    p.add_argument("--layer", required=True, choices=LAYERS,
                   help="Layer to rewind, or 'all' for the full chain.")
    p.add_argument("--from", dest="from_ym", required=True, metavar="YYYY-MM",
                   help="Start calendar month (inclusive).")
    p.add_argument("--to",   dest="to_ym",   required=True, metavar="YYYY-MM",
                   help="End calendar month (inclusive).")
    p.add_argument("--yes", action="store_true",
                   help="Required for --layer all (destructive).")
    p.add_argument("--dry-run", action="store_true",
                   help="Print the plan and exit 0 without touching anything.")
    return p.parse_args(argv)


def main(argv: Optional[list] = None) -> int:
    args = _parse_argv(argv)
    layer = args.layer
    from_ym = args.from_ym
    to_ym = args.to_ym

    if layer == "all":
        rewind_all(from_ym, to_ym, yes=args.yes, dry_run=args.dry_run)
    elif layer == "vault":
        rewind_vault(from_ym, to_ym, dry_run=args.dry_run)
    elif layer == "marts":
        rewind_marts(from_ym, to_ym, dry_run=args.dry_run)
    elif layer == "sink":
        rewind_sink(from_ym, to_ym, dry_run=args.dry_run)
    elif layer == "matrix":
        rewind_matrix(from_ym, to_ym, dry_run=args.dry_run)
    elif layer == "eval_mart":
        rewind_eval_mart(from_ym, to_ym, dry_run=args.dry_run)
    else:
        print("unknown layer: %s" % (layer,), file=sys.stderr, flush=True)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
