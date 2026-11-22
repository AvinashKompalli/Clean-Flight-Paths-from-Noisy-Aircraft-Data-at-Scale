"""Postgres sink for pipeline run metrics and deviation events.

Writes one row to ``pipeline_runs`` (per (ym, source, method, noise) cell),
one row per metric to ``eval_results``, and (optionally) one row per
deviation event to ``deviation_events``.

Semantics after IMPR-01 (architecture section 4.4):

* ``pipeline_runs`` carries a ``ym`` column with
  ``UNIQUE (ym, source, method, noise)``. INSERT uses
  ``ON CONFLICT ... DO NOTHING`` so idempotent reruns are a no-op; the
  only re-measurement path is ``etl4/orchestration/rewind.py --layer matrix``.
* On conflict the function returns the pre-existing ``run_id`` via a
  follow-up SELECT so callers can log it; ``eval_results`` and
  ``deviation_events`` are only inserted when the row was NEWLY created,
  otherwise metrics would accumulate against a stale run.
* Unreachable DSN (dev running without Postgres up) is caught and
  logged; schema-level errors (missing ``ym`` column, missing UNIQUE
  constraint) propagate so the operator sees that the IMPR-01 migration
  has not run.

Schema reference: etl4/infra/create_tables.sql
Dashboard reference: etl4/dashboard/03_pipeline_results.json
"""
from __future__ import annotations

import os
from typing import Optional

import pandas as pd


# metric keys this sink persists as (run_id, metric, value) rows
_PERSISTED_METRICS = (
    "position_rmse_m",
    "seg_f1",
    "seg_precision",
    "seg_recall",
    "coverage_ratio",
    "continuity",
    "boundary_err_s",
    "n_flights",
    "n_deviations",
)


def write_run(
    metrics: dict,
    deviations: Optional[pd.DataFrame] = None,
    dsn: Optional[str] = None,
    ym: Optional[str] = None,
) -> Optional[int]:
    """Insert one pipeline run plus its metrics and deviation events.

    Returns the ``run_id`` (newly created, OR pre-existing on a conflict
    skip) so callers can log the association. Returns ``None`` only when
    the Postgres DSN is unreachable or the CTE finds no row at all.

    Idempotence contract (FR-10, architecture section 4.4): calling this
    function twice with the same ``(ym, source, method, noise)`` touches
    ``pipeline_runs`` exactly once. The second call's ``eval_results``
    and ``deviation_events`` writes are deliberately skipped -- use
    ``rewind --layer matrix`` to re-measure.
    """
    try:
        import psycopg
    except ImportError:
        print("  [postgres sink skipped: psycopg not installed]", flush=True)
        return None

    dsn = dsn or os.getenv(
        "POSTGRES_DSN",
        "postgresql://etl4:change_me@localhost:5432/etl4",
    )

    ym = _resolve_ym(ym, metrics)
    source = metrics.get("source")
    method = metrics.get("method")
    noise  = metrics.get("noise")

    try:
        conn = psycopg.connect(dsn, connect_timeout=3)
    except psycopg.OperationalError as e:
        print(f"  [postgres sink skipped: {e}]", flush=True)
        return None

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH new_run AS (
                        INSERT INTO pipeline_runs
                            (run_date, source, method, noise, ym,
                             rows_in, rows_out, wall_secs)
                        VALUES (CURRENT_DATE, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (ym, source, method, noise) DO NOTHING
                        RETURNING run_id
                    )
                    SELECT run_id, TRUE AS created FROM new_run
                    UNION ALL
                    SELECT run_id, FALSE
                      FROM pipeline_runs
                     WHERE ym = %s AND source = %s AND method = %s AND noise = %s
                       AND NOT EXISTS (SELECT 1 FROM new_run)
                    """,
                    (
                        source, method, noise, ym,
                        _as_int(metrics.get("rows_in")),
                        _as_int(metrics.get("rows_out")),
                        _as_float(metrics.get("wall_s")),
                        ym, source, method, noise,
                    ),
                )
                row = (cur.fetchone() or (None, False))
                run_id, created = row[0], bool(row[1])

                if run_id is None:
                    print("  [postgres sink: pipeline_runs insert returned no row]",
                          flush=True)
                    return None

                if not created:
                    print(
                        f"  [postgres sink: cell "
                        f"(ym={ym}, source={source}, method={method}, noise={noise}) "
                        f"already materialized as run_id={run_id}; "
                        f"eval_results + deviation_events skipped. "
                        f"Use rewind --layer matrix to re-measure.",
                        flush=True,
                    )
                    return run_id

                metric_rows = []
                for key in _PERSISTED_METRICS:
                    val = _as_float(metrics.get(key))
                    if val is not None:
                        metric_rows.append((run_id, key, val))
                if metric_rows:
                    cur.executemany(
                        "INSERT INTO eval_results (run_id, metric, value) "
                        "VALUES (%s, %s, %s)",
                        metric_rows,
                    )

                if deviations is not None and len(deviations) > 0:
                    dev_rows = _deviation_rows(run_id, deviations)
                    if dev_rows:
                        cur.executemany(
                            """
                            INSERT INTO deviation_events
                                (run_id, icao24, flight_id, start_time, end_time,
                                 type, max_distance, peak_residual)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            """,
                            dev_rows,
                        )
        return run_id
    finally:
        conn.close()


def _resolve_ym(ym: Optional[str], metrics: dict) -> str:
    """Pick the partition month for the row.

    Precedence: explicit ``ym`` arg > ``metrics['ym']`` > month of
    ``metrics['run_date']`` > today. Synthetic-source callers should
    pass the sentinel ``ym='SYN-<seed_hex>'`` (architecture section 2.3,
    T-20); this helper does NOT invent a sentinel.
    """
    if ym:
        return str(ym)
    m_ym = metrics.get("ym")
    if m_ym:
        return str(m_ym)
    run_date = metrics.get("run_date")
    ts = pd.Timestamp(run_date) if run_date is not None else pd.Timestamp.utcnow()
    return ts.strftime("%Y-%m")


def _as_float(x) -> Optional[float]:
    if x is None:
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    if pd.isna(v):
        return None
    return v


def _as_int(x) -> Optional[int]:
    v = _as_float(x)
    return int(v) if v is not None else None


def _deviation_rows(run_id: int, deviations: pd.DataFrame) -> list[tuple]:
    rows = []
    for r in deviations.itertuples(index=False):
        d = r._asdict() if hasattr(r, "_asdict") else dict(zip(deviations.columns, r))
        rows.append(
            (
                run_id,
                str(d.get("icao24")) if d.get("icao24") is not None else None,
                str(d.get("flight_id")) if d.get("flight_id") is not None else None,
                _as_int(d.get("t_start") or d.get("start_time")),
                _as_int(d.get("t_end") or d.get("end_time")),
                str(d.get("cause") or d.get("type")) if (d.get("cause") or d.get("type")) else None,
                _as_float(d.get("max_cross_track_m") or d.get("max_distance")),
                _as_float(d.get("peak_residual")),
            )
        )
    return rows
