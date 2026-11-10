"""Daily batch pull of the prior day's OpenSky S3 slice."""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT = "/opt/project"

with DAG(
    dag_id="opensky_s3_batch",
    description="Pull the prior day's state-vector slice from OpenSky S3.",
    start_date=datetime(2026, 11, 1),
    schedule="0 5 * * *",
    catchup=False,
    tags=["ingest", "batch", "s3"],
) as dag:
    pull = BashOperator(
        task_id="pull_prior_day",
        bash_command=(
            f"cd {PROJECT} && "
            "python -m etl4.orchestration.ingest_runner "
            "--source s3 --date {{ ds }}"
        ),
    )
