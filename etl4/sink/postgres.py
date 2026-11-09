"""Postgres sink for pipeline run metrics and deviation events.

Writes one row to `pipeline_runs`, one row per metric to `eval_results`,
and (optionally) one row per deviation event to `deviation_events`. The
sink is non-fatal: if the DSN is unreachable, write_run logs and returns
None rather than raising — a dev running the matrix without the Docker
stack up should not hit a hard failure.

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
) -> Optional[int]:
    """Insert one pipeline run plus its metrics and deviation events.

    Returns the inserted run_id, or None if the Postgres connection
    is unreachable. Callers should treat None as 'sink skipped, not
    failed' — matrix execution continues regardless of DB state.
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

    try:
        with psycopg.connect(dsn, connect_timeout=3) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO pipeline_runs
                        (run_date, source, method, noise, rows_in, rows_out, wall_secs)
                    VALUES (CURRENT_DATE, %s, %s, %s, %s, %s, %s)
                    RETURNING run_id
                    """,
                    (
                        metrics.get("source"),
                        metrics.get("method"),
                        metrics.get("noise"),
                        _as_int(metrics.get("rows_in")),
                        _as_int(metrics.get("rows_out")),
                        _as_float(metrics.get("wall_s")),
                    ),
                )
                run_id = cur.fetchone()[0]

                metric_rows = []
                for key in _PERSISTED_METRICS:
                    val = _as_float(metrics.get(key))
                    if val is not None:
                        metric_rows.append((run_id, key, val))
                if metric_rows:
                    cur.executemany(
                        "INSERT INTO eval_results (run_id, metric, value) VALUES (%s, %s, %s)",
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
    except Exception as e:
        print(f"  [postgres sink skipped: {e}]", flush=True)
        return None


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
