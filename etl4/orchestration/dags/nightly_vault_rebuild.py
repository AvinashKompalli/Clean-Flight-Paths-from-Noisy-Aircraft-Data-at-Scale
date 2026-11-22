"""Nightly rebuild: bronze flightlists -> vault -> marts -> Postgres."""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT = "/opt/project"

with DAG(
    dag_id="nightly_vault_rebuild",
    description="Rebuild the Data Vault 2.0 raw vault, marts, and reload Postgres.",
    start_date=datetime(2026, 11, 1),
    schedule="30 2 * * *",
    catchup=False,
    tags=["vault", "batch"],
) as dag:
    bronze = BashOperator(
        task_id="build_bronze_flightlists",
        bash_command=f"cd {PROJECT} && python -m etl4.vault.bronze_flightlists",
    )
    vault = BashOperator(
        task_id="build_raw_vault",
        bash_command=f"cd {PROJECT} && python -m etl4.vault.raw_vault",
    )
    marts = BashOperator(
        task_id="build_marts",
        bash_command=f"cd {PROJECT} && python -m etl4.vault.marts",
    )
    load = BashOperator(
        task_id="load_postgres",
        bash_command=f"cd {PROJECT} && python -m etl4.vault.postgres_sink",
    )
    bronze >> vault >> marts >> load
