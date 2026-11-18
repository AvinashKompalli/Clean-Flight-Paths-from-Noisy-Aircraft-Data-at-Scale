"""Watermark get/set for incremental refresh (ADR 2026-11-13, IMPR-01).

This module is the minimal client for the ``pipeline_watermarks`` registry
created in T-01 (etl4/infra/create_tables.sql). It is deliberately thin
because the contract is load-bearing: the watermark write MUST happen
inside the caller's transaction, alongside the layer write it is
announcing (architecture.md section 4.1 staged UPSERT pattern). A crash
between a successful INSERT ON CONFLICT and a separate watermark commit
would leave the sink disagreeing with the registry, which is the exact
scenario watermarks exist to prevent.

Contract:

* ``get(layer, table_name)`` is a SELECT; it opens its own autocommit
  connection and returns the stored ``last_ym`` or ``None`` when no row
  exists. Callers treat ``None`` as "process everything from the
  earliest bronze available" (bootstrap).

* ``set(layer, table_name, last_ym, row_count, conn)`` REQUIRES an
  existing ``psycopg.Connection`` and does NOT commit. The caller owns
  the transaction so the watermark advance is atomic with the layer
  INSERT (and rolls back together on failure).

``set`` shadows the Python builtin ``set`` at module scope. Import the
module, not the names, when consuming:

    from etl4.vault import watermark
    watermark.set(layer, table_name, last_ym, row_count, conn)

A ``from etl4.vault.watermark import set`` will clobber the builtin in
the caller's namespace. Don't do that.
"""
from __future__ import annotations

import os
from typing import Optional

import psycopg

__all__ = ["get", "set"]

DSN_DEFAULT = os.getenv(
    "POSTGRES_DSN",
    "postgresql://etl4:change_me@localhost:5432/etl4",
)


def get(layer: str, table_name: str, dsn: Optional[str] = None) -> Optional[str]:
    """Return the ``last_ym`` recorded for ``(layer, table_name)``.

    Returns ``None`` when no row exists; callers bootstrap off that
    signal by processing every ``ym`` present in bronze. Opens its own
    autocommit connection so read-after-write across txn boundaries
    sees the committed state.
    """
    dsn = dsn or DSN_DEFAULT
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT last_ym FROM pipeline_watermarks "
                "WHERE layer = %s AND table_name = %s",
                (layer, table_name),
            )
            row = cur.fetchone()
    return row[0] if row else None


def set(
    layer: str,
    table_name: str,
    last_ym: str,
    row_count: int,
    conn: "psycopg.Connection",
) -> None:
    """Upsert the watermark inside the caller's transaction.

    Does NOT commit; the caller commits (or rolls back) so the watermark
    advance travels with the layer write. ``updated_at`` is refreshed on
    every call. A malformed ``last_ym`` (e.g. ``'2022-7'``) raises
    ``psycopg.errors.CheckViolation`` from the DDL CHECK regex.
    """
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO pipeline_watermarks (layer, table_name, last_ym, row_count) "
            "VALUES (%s, %s, %s, %s) "
            "ON CONFLICT (layer, table_name) DO UPDATE "
            "   SET last_ym   = EXCLUDED.last_ym, "
            "       row_count = EXCLUDED.row_count, "
            "       updated_at = now()",
            (layer, table_name, last_ym, row_count),
        )
