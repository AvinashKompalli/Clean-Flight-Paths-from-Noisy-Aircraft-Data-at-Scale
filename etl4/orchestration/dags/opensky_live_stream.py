"""On-demand live-REST ingest demo.

Streams 2000 state vectors through live_producer -> Kafka, then drains Kafka
into bronze parquet via kafka_bronze_consumer. The streaming path lands in
the lake; it no longer writes to the void.

No schedule - cron would burn the 4000/day OAuth credit budget for no gain.
Zenodo is the batch primary; this is the streaming secondary.
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT = "/opt/project"

with DAG(
    dag_id="opensky_live_stream",
    description="Stream 2000 live OpenSky REST records: Kafka -> bronze.",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    tags=["ingest", "stream", "live"],
) as dag:
    stream = BashOperator(
        task_id="live_producer_demo",
        bash_command=(
            f"cd {PROJECT} && "
            "python -m etl4.orchestration.ingest_runner "
            "--source live --limit 2000 --interval 5"
        ),
    )
    drain = BashOperator(
        task_id="drain_kafka_to_bronze",
        bash_command=(
            f"cd {PROJECT} && "
            "python -m etl4.ingest.kafka_bronze_consumer "
            "--from-beginning --exit-on-idle --idle-seconds 15 --batch-size 2000"
        ),
    )
    stream >> drain
