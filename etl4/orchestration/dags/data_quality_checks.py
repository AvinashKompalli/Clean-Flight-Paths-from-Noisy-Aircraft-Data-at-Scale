"""Standalone data-quality DAG - runs the full expectation suite on demand.

Separate DAG so it can be triggered without re-running ingest+vault+marts, e.g.
for an ad-hoc audit between scheduled daily loads.
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT = "/opt/project"

with DAG(
    dag_id="data_quality_checks",
    description="Great Expectations validation over bronze + marts.",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    tags=["qa", "great_expectations"],
) as dag:
    BashOperator(
        task_id="validate_all_suites",
        bash_command=(
            f"cd {PROJECT} && "
            "python -m etl4.qa.validate "
            "--sample 250000 "
            "--json /opt/project/data/qa/report.json"
        ),
    )
