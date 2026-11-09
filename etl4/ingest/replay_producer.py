"""Replay bronze Parquet into Kafka topic adsb.raw in event-time order.

Key = icao24, so one aircraft stays in one partition (order is kept).
Speed factor: 1 = real time, 10 = 10x faster, max = no sleep.
"""
import argparse
import json
import math
import os
import time
from pathlib import Path

import pandas as pd
import yaml
from dotenv import load_dotenv
from prometheus_client import Counter, Histogram, start_http_server

load_dotenv()
ROOT = Path(__file__).resolve().parents[1]
CFG = yaml.safe_load((ROOT / "config" / "config.yaml").read_text())

RECORDS_SENT = Counter("records_sent_total", "Records produced to Kafka")
PRODUCE_LATENCY = Histogram(
    "kafka_produce_latency_seconds",
    "Latency of Kafka produce calls (seconds)",
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)


def load_bronze(path):
    df = pd.read_parquet(path)
    return df.sort_values("time").reset_index(drop=True)


def _safe(v):
    """Replace NaN / inf / numpy types so json.dumps always succeeds."""
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    if hasattr(v, "item"):      # numpy scalars
        return v.item()
    return v


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--path", default=str(ROOT / CFG["paths"]["bronze_raw_states"]))
    p.add_argument("--speed", default="10", help="1, 10, 100 or max")
    p.add_argument("--limit", type=int, default=0, help="stop after N records (0 = all)")
    p.add_argument("--metrics-port", type=int, default=8000)
    a = p.parse_args()

    from confluent_kafka import Producer
    producer = Producer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")})
    topic = CFG["kafka"]["topic_raw"]

    df = load_bronze(a.path)
    if a.limit:
        df = df.head(a.limit)
    speed = None if a.speed == "max" else float(a.speed)

    start_http_server(a.metrics_port)
    t_first = df["time"].iloc[0]
    wall_start = time.time()
    sent = 0

    for row in df.itertuples(index=False):
        if speed is not None:
            target = wall_start + (row.time - t_first) / speed
            sleep_for = target - time.time()
            if sleep_for > 0:
                time.sleep(sleep_for)

        record = {k: _safe(v) for k, v in row._asdict().items()}
        value = json.dumps(record)
        with PRODUCE_LATENCY.time():
            producer.produce(topic, key=str(row.icao24), value=value)
            producer.poll(0)
        RECORDS_SENT.inc()
        sent += 1

    producer.flush()
    elapsed = time.time() - wall_start
    print(f"sent {sent} records in {elapsed:.1f} s ({sent / elapsed:,.0f} rec/s)")


if __name__ == "__main__":
    main()
