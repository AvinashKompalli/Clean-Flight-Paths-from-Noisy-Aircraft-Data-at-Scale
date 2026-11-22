"""One-shot zero-reprocess migration: monolith to ym=YYYY-MM partitions.

Streams each existing ``<table>/part.parquet`` monolith through
``pyarrow.parquet.ParquetFile.iter_batches``, derives ``ym`` per row from
the table's natural month-bearing column, and writes
``<table>/ym=YYYY-MM/part.parquet`` partitions atomically.

Guarantees (architecture.md section 6):

* Zero re-processing of upstream data -- parquet rechunk only.
* Row-count equality gate: pass only if
  ``sum(partition_rows) == monolith_rows``. Any NA ym row is a loss, so a
  column with nulls aborts the table before any rename lands. A failing
  table leaves the monolith untouched and cleans up every ``.tmp`` file
  under its own directory.
* Idempotent: a table whose ym tree already sums to the monolith row
  count is reported ``already_migrated``; the next run is a byte-identical
  no-op (st_mtime_ns preserved).
* Streaming writes via per-month ``pq.ParquetWriter`` into
  ``<ym_dir>/.part.parquet.<pid>.tmp``, renamed to ``part.parquet`` via
  ``os.replace`` only after the row-count gate passes. The ``.tmp`` suffix
  keeps writer temporaries invisible to ``vault_io``'s ``*.parquet`` glob
  and ``reconcile.rglob("*.parquet")``.
* Does not delete monoliths. The operator deletes them AFTER verifying
  partition reads (see handoff for the exit condition).

Postgres-side migration is NOT run from here. The script prints the
``fact_trajectory_enriched.ym`` ADD COLUMN + backfill + index block to
stdout at the end so the operator can pipe it to ``psql``. The
``pipeline_runs.ym`` constraint already lives in
``etl4/infra/create_tables.sql`` (IMPR-01 T-01B) and is not re-emitted
here.

CLI::

    python -m etl4.scripts.migrate_partition_ym \
        [--tables hub_flight,sat_state_observation,...] \
        [--dry-run] \
        [--vault-dir data/silver/vault] \
        [--marts-dir data/gold/marts]

Exit codes:
  0  all requested tables migrated, already-migrated, or had no monolith
  1  one or more tables failed (schema mismatch, row-count gate, etc.)
  2  argparse / usage error

Windows cp1252-safe: no arrows, no em-dashes in stdout.
"""
from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from etl4.vault import vault_io
from etl4.vault import watermark

__all__ = [
    "TABLE_SPECS",
    "TableSpec",
    "MigrationResult",
    "migrate_one",
    "seed_watermarks",
    "main",
]

DSN_DEFAULT = os.getenv(
    "POSTGRES_DSN",
    "postgresql://etl4:change_me@localhost:5432/etl4",
)

BATCH_SIZE = 1_000_000
DONE_SENTINEL = "_DONE"


@dataclass(frozen=True)
class TableSpec:
    """One row of the migration manifest.

    ``layer`` is the pipeline_watermarks layer tag ('vault' or 'marts').
    ``subdir`` roots the table under its layer-specific on-disk directory
    (``data/silver/vault`` for vault, ``data/gold/marts`` for marts).
    ``ym_column`` is the first choice of month-bearing column on the
    monolith; ``helper`` returns a ``YYYY-MM`` string Series from that
    column's pandas representation.
    ``unsupported_reason``, when non-empty, marks the table as a known
    spec gap: ``migrate_one`` returns ``schema_mismatch`` with this
    reason verbatim and never opens the monolith. Used for tables where
    the architecture section 6 ym key disagrees with the T-08 writer
    (sat_flight_summary firstseen vs file-month) or the column is
    physically absent (link_flight_aircraft, link_flight_route).
    """
    name: str
    layer: str
    subdir: str
    ym_column: str
    helper: Callable[[pd.Series], pd.Series]
    unsupported_reason: str = ""


def _ym_from_date_or_ts(series: pd.Series) -> pd.Series:
    """Date / datetime / timestamp[ns] column -> 'YYYY-MM' string column.

    Thin wrapper over :func:`vault_io.ym_from_date` so the dispatch table
    keeps a single callable per table.
    """
    return vault_io.ym_from_date(series)


def _ym_from_epoch(series: pd.Series) -> pd.Series:
    """Epoch-second int64 column -> 'YYYY-MM' string column."""
    return vault_io.ym_from_epoch(series)


TABLE_SPECS: dict[str, TableSpec] = {
    "hub_flight": TableSpec(
        "hub_flight", "vault", "silver/vault", "day", _ym_from_date_or_ts,
    ),
    # Architecture section 6 PARTITIONED_TABLES assigns both link tables to
    # the 'day' column, but the production monoliths carry neither 'day'
    # nor any other per-row month key (only load_dt, an ingest timestamp,
    # and the hk_flight hash). The schema check below surfaces this; the
    # explicit unsupported_reason here is a faster, more actionable
    # message that avoids reading the monolith footer.
    "link_flight_aircraft": TableSpec(
        "link_flight_aircraft", "vault", "silver/vault", "day", _ym_from_date_or_ts,
        unsupported_reason=(
            "link_flight_aircraft monolith has no in-row month column "
            "(day absent; load_dt is a single ingest timestamp; filename "
            "is the shard enumeration not the bronze source). Architecture "
            "section 6 assumes 'day' is present; either extend "
            "raw_vault.build_link_flight_aircraft to emit 'day' and "
            "regenerate the monolith, or accept that this table is rebuilt "
            "from flightlist bronze rather than migrated."
        ),
    ),
    "link_flight_route": TableSpec(
        "link_flight_route", "vault", "silver/vault", "day", _ym_from_date_or_ts,
        unsupported_reason=(
            "link_flight_route monolith has no in-row month column "
            "(day absent; load_dt is a single ingest timestamp). Same "
            "resolution options as link_flight_aircraft."
        ),
    ),
    # sat_flight_summary has 'firstseen' in the monolith, but T-08 writes
    # new sat_flight_summary partitions keyed by BRONZE FILE MONTH (the
    # ym_from_filename of the flightlist), NOT by firstseen. Migrating
    # with firstseen would split 2022-06's flights into {2022-05, 2022-06}
    # (12.7k rows cross the month boundary in the production data), so a
    # subsequent per-ym rewind+rebuild of 2022-06 would duplicate those
    # 12.7k rows. Flag as unsupported until the data-engineer formalizes
    # one key for both migration and T-08 (candidates: lastseen if it
    # aligns 100% with file-month, or a hub_flight.hk_flight -> day
    # lookup). Smoke exercises every other path.
    "sat_flight_summary": TableSpec(
        "sat_flight_summary", "vault", "silver/vault", "firstseen", _ym_from_date_or_ts,
        unsupported_reason=(
            "sat_flight_summary monolith's firstseen crosses month boundaries "
            "(dry-run: 6,152 of 2022-06's 2,454,119 rows land in 2022-05 and "
            "6,533 of 2022-12's 2,146,527 land in 2022-11). T-08 writes new "
            "sat_flight_summary partitions keyed by the flightlist bronze "
            "file month, not firstseen, so a split-brain partitioning would "
            "hide those rows from any per-ym mart join and duplicate them "
            "on a rewind-and-rebuild. Data-engineer must pick one key "
            "(lastseen if it aligns 100% with file-month, or a hub_flight "
            "join) before this table is migratable."
        ),
    ),
    "sat_state_observation": TableSpec(
        "sat_state_observation", "vault", "silver/vault", "time", _ym_from_epoch,
    ),
    "fact_flight": TableSpec(
        "fact_flight", "marts", "gold/marts", "day", _ym_from_date_or_ts,
    ),
    "fact_trajectory_enriched": TableSpec(
        "fact_trajectory_enriched", "marts", "gold/marts", "time", _ym_from_epoch,
    ),
}


@dataclass
class MigrationResult:
    table: str
    status: str                 # migrated | already_migrated | no_monolith | schema_mismatch | row_count_mismatch
    monolith_rows: int = 0
    written_rows: int = 0
    per_ym_counts: dict[str, int] = None  # type: ignore[assignment]
    detail: str = ""

    def __post_init__(self) -> None:
        if self.per_ym_counts is None:
            self.per_ym_counts = {}


def _table_dir(spec: TableSpec, vault_dir: Path, marts_dir: Path) -> Path:
    return (vault_dir if spec.layer == "vault" else marts_dir) / spec.name


def _monolith_rows(monolith: Path) -> int:
    return pq.ParquetFile(monolith).metadata.num_rows


def _ym_partition_row_count(table_dir: Path) -> tuple[int, dict[str, int]]:
    """Footer-only row count across every ``ym=*/part.parquet`` under ``table_dir``.

    Returns ``(total, per_ym)``. ``per_ym`` includes only ym dirs that
    carry a readable ``part.parquet``; ``.tmp`` and empty dirs are ignored.
    """
    total = 0
    per_ym: dict[str, int] = {}
    if not table_dir.is_dir():
        return 0, per_ym
    for ym_dir in sorted(table_dir.glob("ym=*")):
        if not ym_dir.is_dir():
            continue
        fp = ym_dir / "part.parquet"
        if not fp.is_file():
            continue
        n = pq.ParquetFile(fp).metadata.num_rows
        per_ym[ym_dir.name.split("=", 1)[1]] = n
        total += n
    return total, per_ym


def _validate_schema(monolith: Path, spec: TableSpec) -> Optional[str]:
    """None when the monolith carries ``spec.ym_column``; else an error message."""
    schema = pq.ParquetFile(monolith).schema_arrow
    names = {f.name for f in schema}
    if spec.ym_column not in names:
        return (
            "monolith for %s lacks column %r (schema: %s); "
            "architecture section 6 assumes it is present; cannot derive ym"
            % (spec.name, spec.ym_column, sorted(names))
        )
    return None


def _derive_ym(batch: pa.RecordBatch, spec: TableSpec) -> pd.Series:
    """Compute a ``YYYY-MM`` string Series aligned with the batch rows."""
    series = batch.column(spec.ym_column).to_pandas()
    return spec.helper(series)


def _cleanup_tmp_writers(writers: dict) -> None:
    """Close + unlink every writer's tmp file. Called on abort.

    Windows holds a read lock while a ``pq.ParquetWriter`` is open; close
    before unlink or the delete raises PermissionError.
    """
    for mw in writers.values():
        mw.abort()


class _MonthWriter:
    __slots__ = ("path", "tmp", "writer", "rows", "schema")

    def __init__(self, path: Path, schema: pa.Schema, compression: str):
        self.path = path
        self.tmp = path.with_name(".part.parquet.%d.tmp" % (os.getpid(),))
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.schema = schema
        self.writer = pq.ParquetWriter(self.tmp, schema, compression=compression)
        self.rows = 0

    def write(self, table: pa.Table) -> None:
        self.writer.write_table(table)
        self.rows += table.num_rows

    def close(self) -> None:
        if self.writer is not None:
            self.writer.close()
            self.writer = None

    def commit(self) -> None:
        self.close()
        os.replace(self.tmp, self.path)

    def abort(self) -> None:
        self.close()
        try:
            if self.tmp.exists():
                self.tmp.unlink()
        except OSError:
            pass


def _plan_only(monolith: Path, spec: TableSpec) -> tuple[int, dict[str, int], int]:
    """Dry-run pass: count rows per ym + NA count. No writes.

    Reads only ``spec.ym_column`` from each batch so a 20 GB monolith
    costs a few seconds, not minutes.
    """
    counts: dict[str, int] = {}
    na = 0
    total = 0
    pf = pq.ParquetFile(monolith)
    for batch in pf.iter_batches(batch_size=BATCH_SIZE, columns=[spec.ym_column]):
        ym = _derive_ym(batch, spec)
        s = ym.value_counts(dropna=False)
        for k, v in s.items():
            v = int(v)
            if pd.isna(k):
                na += v
            else:
                counts[str(k)] = counts.get(str(k), 0) + v
            total += v
    return total, counts, na


def migrate_one(
    spec: TableSpec,
    vault_dir: Path,
    marts_dir: Path,
    *,
    dry_run: bool,
    compression: str = "snappy",
) -> MigrationResult:
    """Rechunk one monolith into ym=YYYY-MM partitions.

    Branches:

    * no monolith on disk  -> status ``no_monolith`` (not an error).
    * ym tree already matches monolith footer -> ``already_migrated``;
      byte-identical no-op; mtime preserved.
    * schema missing spec column -> ``schema_mismatch``; the operator
      resolves (architecture section 6 gap on link_flight_aircraft /
      link_flight_route, which carry no natural month column).
    * dry_run                -> ``migrated`` (planned); per-ym counts
      populated; no file touched.
    * row-count gate fails   -> ``row_count_mismatch``; every tmp
      partition cleaned up; monolith untouched; no watermark advance.
    * row-count gate passes  -> ``migrated``; every ym tmp renamed to
      ``part.parquet`` atomically.
    """
    tbl = _table_dir(spec, vault_dir, marts_dir)
    monolith = tbl / "part.parquet"

    if not monolith.is_file():
        return MigrationResult(spec.name, "no_monolith", detail=str(monolith))

    expected = _monolith_rows(monolith)
    existing_total, existing_per_ym = _ym_partition_row_count(tbl)

    if existing_per_ym and existing_total == expected:
        return MigrationResult(
            spec.name, "already_migrated",
            monolith_rows=expected, written_rows=existing_total,
            per_ym_counts=existing_per_ym,
            detail="already migrated: ym tree sum matches monolith",
        )
    if existing_per_ym and existing_total != expected:
        return MigrationResult(
            spec.name, "row_count_mismatch",
            monolith_rows=expected, written_rows=existing_total,
            per_ym_counts=existing_per_ym,
            detail=(
                "row-count mismatch: ym dirs present (sum=%d) disagree "
                "with monolith (%d); manual cleanup required before re-run"
                % (existing_total, expected)
            ),
        )

    if spec.unsupported_reason:
        return MigrationResult(
            spec.name, "schema_mismatch",
            monolith_rows=expected, detail=spec.unsupported_reason,
        )

    err = _validate_schema(monolith, spec)
    if err is not None:
        return MigrationResult(
            spec.name, "schema_mismatch",
            monolith_rows=expected, detail=err,
        )

    if dry_run:
        total, counts, na = _plan_only(monolith, spec)
        if na > 0:
            return MigrationResult(
                spec.name, "row_count_mismatch",
                monolith_rows=expected, written_rows=total - na,
                per_ym_counts=counts,
                detail=(
                    "row-count mismatch: would drop %d rows with NULL %s"
                    % (na, spec.ym_column)
                ),
            )
        return MigrationResult(
            spec.name, "migrated",
            monolith_rows=expected, written_rows=total,
            per_ym_counts=counts,
            detail="dry_run plan",
        )

    pf = pq.ParquetFile(monolith)
    base_schema = pf.schema_arrow
    # Output schema = monolith schema + ym string column so the migrated
    # partitions are schema-compatible with T-08's writer (raw_vault.py
    # line 543 appends frame["ym"]). Without this, read_table would
    # concat two schemas (monolith-shape and T-08-shape) and hand the
    # caller NaN ym for every migrated row.
    out_schema = base_schema.append(pa.field("ym", pa.string()))
    writers: dict[str, _MonthWriter] = {}
    written = 0
    load_dt = datetime.now(timezone.utc)
    try:
        for batch in pf.iter_batches(batch_size=BATCH_SIZE):
            ym = _derive_ym(batch, spec)
            na_mask = ym.isna()
            if na_mask.any():
                raise ValueError(
                    "row-count mismatch: monolith has NULL %s "
                    "(%d NA rows in a %d-row batch)"
                    % (spec.ym_column, int(na_mask.sum()), batch.num_rows)
                )
            # Vectorized grouping via pandas factorize (CLAUDE.md section
            # 3.2) -- no per-row python loop over the batch.
            codes, uniques = pd.factorize(ym.to_numpy(), sort=False)
            for i, ym_str in enumerate(uniques):
                ym_str = str(ym_str)
                idx = np.flatnonzero(codes == i)
                sub = batch.take(pa.array(idx, type=pa.int64()))
                ym_col = pa.array([ym_str] * sub.num_rows, type=pa.string())
                sub = pa.RecordBatch.from_arrays(
                    list(sub.columns) + [ym_col],
                    schema=out_schema,
                )
                mw = writers.get(ym_str)
                if mw is None:
                    out = tbl / ("ym=" + ym_str) / "part.parquet"
                    mw = _MonthWriter(out, out_schema, compression)
                    writers[ym_str] = mw
                mw.write(pa.Table.from_batches([sub], out_schema))
                written += sub.num_rows
    except Exception as exc:
        _cleanup_tmp_writers(writers)
        return MigrationResult(
            spec.name, "row_count_mismatch",
            monolith_rows=expected, written_rows=written,
            detail="row-count mismatch: write pass aborted: %s" % (exc,),
        )

    if written != expected:
        _cleanup_tmp_writers(writers)
        return MigrationResult(
            spec.name, "row_count_mismatch",
            monolith_rows=expected, written_rows=written,
            detail=(
                "row-count mismatch: post-stream count %d != monolith "
                "footer %d; no partitions committed"
                % (written, expected)
            ),
        )

    per_ym: dict[str, int] = {}
    for ym_str, mw in sorted(writers.items()):
        per_ym[ym_str] = mw.rows
        mw.commit()
        _write_done_sentinel(tbl / ("ym=" + ym_str), load_dt)

    return MigrationResult(
        spec.name, "migrated",
        monolith_rows=expected, written_rows=written,
        per_ym_counts=per_ym,
        detail="row-count gate passed; %d ym partitions committed" % (len(per_ym),),
    )


def _write_done_sentinel(ym_dir: Path, load_dt: datetime) -> None:
    """Drop the ``_DONE`` sentinel beside ``part.parquet``.

    Mirrors ``raw_vault._write_done`` so T-08 and T-10 treat migrated
    partitions as resumed (``_partition_done`` checks for both files).
    Without the sentinel, the next vault run with a cleared watermark
    would rebuild over the migrated partition.
    """
    (ym_dir / DONE_SENTINEL).write_text(
        "load_dt=%s\nsource=migrate_partition_ym\n"
        % (load_dt.isoformat(),)
    )


def seed_watermarks(
    specs: list[TableSpec],
    results: dict[str, MigrationResult],
    vault_dir: Path,
    marts_dir: Path,
    *,
    dsn: str,
) -> int:
    """Set pipeline_watermarks for every table that is now partitioned.

    Walks the on-disk ym dirs via ``vault_io.list_ym`` so a re-run after
    a partial crash (writes committed but watermark not) still converges.

    Returns the number of watermark rows written / refreshed.
    """
    import psycopg
    rows_written = 0
    with psycopg.connect(dsn) as conn:
        for spec in specs:
            tbl = _table_dir(spec, vault_dir, marts_dir)
            yms = vault_io.list_ym(
                spec.name,
                root=(vault_dir if spec.layer == "vault" else marts_dir),
            )
            if not yms:
                continue
            last_ym = yms[-1]
            total, _ = _ym_partition_row_count(tbl)
            watermark.set(spec.layer, spec.name, last_ym, total, conn)
            rows_written += 1
        conn.commit()
    return rows_written


POSTGRES_SQL_BLOCK = """\
-- IMPR-01 T-12 Postgres-side migration (operator-applied).
-- Adds ym to fact_trajectory_enriched + backfill + index.
-- The pipeline_runs.ym column + constraint already live in
-- etl4/infra/create_tables.sql (T-01B); not re-emitted here.
--
-- Review before piping to psql; this script does NOT run it.
BEGIN;
SET LOCAL TIME ZONE 'UTC';
ALTER TABLE fact_trajectory_enriched ADD COLUMN IF NOT EXISTS ym TEXT;
UPDATE fact_trajectory_enriched
   SET ym = to_char(to_timestamp(time), 'YYYY-MM')
 WHERE ym IS NULL;
ALTER TABLE fact_trajectory_enriched ALTER COLUMN ym SET NOT NULL;
CREATE INDEX IF NOT EXISTS idx_fact_traj_ym ON fact_trajectory_enriched(ym);
COMMIT;
"""


def _parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="etl4.scripts.migrate_partition_ym",
        description="One-shot zero-reprocess migration: monolith -> ym partitions",
    )
    p.add_argument(
        "--tables",
        default=None,
        help=(
            "comma-separated list of table names to migrate "
            "(default: all seven in TABLE_SPECS)"
        ),
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="plan only; print projected per-ym row counts; no files touched; no DB access",
    )
    p.add_argument(
        "--vault-dir",
        default="data/silver/vault",
        help="root for silver vault tables (default: data/silver/vault)",
    )
    p.add_argument(
        "--marts-dir",
        default="data/gold/marts",
        help="root for gold marts tables (default: data/gold/marts)",
    )
    p.add_argument(
        "--skip-watermark",
        action="store_true",
        help="skip the pipeline_watermarks upsert (for offline smokes)",
    )
    p.add_argument(
        "--dsn",
        default=DSN_DEFAULT,
        help="Postgres DSN for the watermark upsert (default: $POSTGRES_DSN or local)",
    )
    return p.parse_args(argv)


def _resolve_specs(selector: Optional[str]) -> list[TableSpec]:
    if selector is None:
        return list(TABLE_SPECS.values())
    names = [n.strip() for n in selector.split(",") if n.strip()]
    unknown = [n for n in names if n not in TABLE_SPECS]
    if unknown:
        raise SystemExit(
            "unknown table(s): %s; known: %s"
            % (unknown, sorted(TABLE_SPECS))
        )
    return [TABLE_SPECS[n] for n in names]


def _resolve_dirs(ns: argparse.Namespace) -> tuple[Path, Path]:
    def _abs(p: str) -> Path:
        q = Path(p)
        return q if q.is_absolute() else (Path.cwd() / q).resolve()
    return _abs(ns.vault_dir), _abs(ns.marts_dir)


def _print_result(r: MigrationResult) -> None:
    head = "[%s] %s" % (r.table, r.status)
    if r.monolith_rows:
        head += " monolith=%d" % r.monolith_rows
    if r.written_rows:
        head += " written=%d" % r.written_rows
    if r.per_ym_counts:
        head += " ym_count=%d" % (len(r.per_ym_counts),)
    if r.detail:
        head += " :: " + r.detail
    print(head, flush=True)
    if r.per_ym_counts and len(r.per_ym_counts) <= 60:
        for ym, n in sorted(r.per_ym_counts.items()):
            print("    ym=%s rows=%d" % (ym, n), flush=True)


def main(argv: Optional[list] = None) -> int:
    ns = _parse_args(argv)
    specs = _resolve_specs(ns.tables)
    vault_dir, marts_dir = _resolve_dirs(ns)
    print(
        "migrate_partition_ym: vault=%s marts=%s dry_run=%s"
        % (vault_dir, marts_dir, ns.dry_run),
        flush=True,
    )

    results: dict[str, MigrationResult] = {}
    for spec in specs:
        r = migrate_one(spec, vault_dir, marts_dir, dry_run=ns.dry_run)
        results[spec.name] = r
        _print_result(r)

    # Exit policy:
    #   dry_run:  schema_mismatch is informational (WARN, exit 0). The
    #             operator wants to see the plan surface. row-count
    #             mismatch is fatal (exit 1) because it is a data-loss
    #             risk even on paper.
    #   real run: BOTH schema_mismatch and row_count_mismatch are fatal
    #             (exit 1, no watermark). Running default with a mix of
    #             supported and unsupported tables would seed a half-
    #             baked state; the operator must pass --tables to opt
    #             in to the supported subset.
    fatal_counts = [r for r in results.values() if r.status == "row_count_mismatch"]
    schema_fail = [r for r in results.values() if r.status == "schema_mismatch"]
    if fatal_counts:
        print(
            "FAIL: %d table(s) failed row-count gate: %s"
            % (len(fatal_counts), [r.table for r in fatal_counts]),
            flush=True,
        )
    if schema_fail:
        tag = "WARN" if ns.dry_run else "FAIL"
        print(
            "%s: %d table(s) have a schema / key gap; operator must "
            "resolve before PR3 reads: %s"
            % (tag, len(schema_fail), [r.table for r in schema_fail]),
            flush=True,
        )

    fatal = fatal_counts + ([] if ns.dry_run else schema_fail)

    if not ns.dry_run and not ns.skip_watermark and not fatal:
        import_err: Optional[Exception] = None
        try:
            n = seed_watermarks(specs, results, vault_dir, marts_dir, dsn=ns.dsn)
            print("pipeline_watermarks rows upserted: %d" % n, flush=True)
        except Exception as e:
            import_err = e
        if import_err is not None:
            print(
                "pipeline_watermarks upsert skipped (%s); "
                "re-run with Postgres reachable to seed"
                % (import_err,),
                flush=True,
            )

    print("", flush=True)
    print("---- Postgres-side migration SQL (operator applies) ----", flush=True)
    print(POSTGRES_SQL_BLOCK, flush=True)
    print("---- end SQL ----", flush=True)

    return 1 if fatal else 0


if __name__ == "__main__":
    raise SystemExit(main())
