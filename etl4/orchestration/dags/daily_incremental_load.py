"""Daily incremental ingest -> vault -> marts -> Postgres.

Chain:
  1. daily_batch   one OpenSky REST poll (1 credit, ~12k state vectors)
  2. raw_vault     rebuild hubs/links/sats from accumulated bronze
  3. marts         rebuild dim_aircraft, dim_airport, fact_flight,
                   fact_trajectory_enriched from the vault
  4. postgres_sink COPY marts into Postgres (full refresh - each table
                   is small enough, and the sink pattern already handles
                   DROP+COPY idempotently)

Why full-refresh downstream of an incremental ingest? The vault is
designed to accrete (hubs are insert-only on new keys, sats version by
load_dt), so each daily poll genuinely adds data. Marts and Postgres are
projections of that growing vault; rebuilding them from scratch is
simpler than per-row upsert and completes in < 10 min.

One run per day stays well inside the 4000 credits/day OAuth quota.
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT = "/opt/project"

with DAG(
    dag_id="daily_incremental_load",
    description="Daily OpenSky poll -> vault -> marts -> Postgres.",
    start_date=datetime(2026, 11, 1),
    schedule="0 1 * * *",
    catchup=False,
    tags=["ingest", "batch", "incremental"],
) as dag:
    ingest = BashOperator(
        task_id="daily_batch_poll",
        bash_command=(
            f"cd {PROJECT} && "
            "python -m etl4.ingest.daily_batch --date {{ ds }}"
        ),
    )
    vault = BashOperator(
        task_id="rebuild_raw_vault",
        bash_command=f"cd {PROJECT} && python -m etl4.vault.raw_vault",
    )
    marts = BashOperator(
        task_id="rebuild_marts",
        bash_command=f"cd {PROJECT} && python -m etl4.vault.marts",
    )
    sink = BashOperator(
        task_id="load_postgres",
        bash_command=f"cd {PROJECT} && python -m etl4.vault.postgres_sink",
    )
    # GE suite over bronze + marts. Non-zero exit fails the DAG run.
    qa = BashOperator(
        task_id="data_quality_checks",
        bash_command=(
            f"cd {PROJECT} && "
            "python -m etl4.qa.validate --sample 250000"
        ),
    )
    ingest >> vault >> marts >> sink >> qa
