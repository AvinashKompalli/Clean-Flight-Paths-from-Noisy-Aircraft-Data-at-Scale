"""Monthly pull of the latest Zenodo flightlist release."""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT = "/opt/project"

with DAG(
    dag_id="zenodo_versioned_pull",
    description="Pull the latest Zenodo flightlist release (concept DOI 3737101).",
    start_date=datetime(2026, 11, 1),
    schedule="0 7 1 * *",
    catchup=False,
    tags=["ingest", "batch", "zenodo"],
) as dag:
    pull = BashOperator(
        task_id="pull_latest",
        bash_command=(
            f"cd {PROJECT} && "
            "python -m etl4.orchestration.ingest_runner "
            "--source zenodo --latest"
        ),
    )
