"""Kafka -> bronze parquet sink.

Drains `adsb.raw` messages produced by `live_producer.py` and lands them in
the same partitioned bronze layout that daily_batch and historical ingests
write to:

    data/bronze/raw_states/date=YYYY-MM-DD/hour=HH/part-<unix>.parquet

Semantics
---------
* at-least-once: offsets are only committed after a successful parquet flush.
* buffer flushes when it hits `--batch-size` records (default 5000) OR after
  `--idle-seconds` of no new messages (default 10s) OR when the process is
  told to stop.
* one-shot drain: pass `--idle-seconds 10 --exit-on-idle` and the consumer
  exits once the topic has been quiet for ten seconds. This is how the
  `opensky_live_stream` DAG uses it: producer writes N, consumer drains, done.

Usage
-----
    python -m etl4.ingest.kafka_bronze_consumer                 # run forever
    python -m etl4.ingest.kafka_bronze_consumer --exit-on-idle  # one-shot drain
    python -m etl4.ingest.kafka_bronze_consumer --max-messages 2000
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml
from dotenv import load_dotenv

from etl4.orchestration.ingest_metrics import ingest_run

load_dotenv()
ROOT = Path(__file__).resolve().parents[2]
CFG = yaml.safe_load((Path(__file__).resolve().parents[1] / "config" / "config.yaml").read_text())
BRONZE = ROOT / "data" / "bronze" / "raw_states"

# REST field -> bronze column (same map as daily_batch.py; kept in sync so the
# two writers land identical schemas in the same partition tree).
_REST_TO_BRONZE = {
    "icao24":        "icao24",
    "callsign":      "callsign",
    "longitude":     "lon",
    "latitude":      "lat",
    "baro_altitude": "baroaltitude",
    "geo_altitude":  "geoaltitude",
    "on_ground":     "onground",
    "velocity":      "velocity",
    "true_track":    "heading",
    "vertical_rate": "vertrate",
    "last_contact":  "lastcontact",
    "time_position": "lastposupdate",
    "squawk":        "squawk",
    "spi":           "spi",
}
_BRONZE_COLS = ["time", "icao24", "lat", "lon", "velocity", "heading",
                "vertrate", "callsign", "onground", "alert", "spi", "squawk",
                "baroaltitude", "geoaltitude", "lastposupdate", "lastcontact"]
# hour-of-day is a partition-layout key (directory name); never written into
# the parquet file. clean.py recomputes a UNIX hour-start when downstream
# needs it, so the file keeps a single source of truth for hour.


def _record_to_bronze_row(rec: dict) -> dict | None:
    """Project one Kafka JSON message to a bronze-schema row."""
    snap = rec.get("snapshot_time")
    icao = rec.get("icao24")
    if snap is None or icao is None:
        return None
    row = {"time": int(snap), "alert": None}
    for rest_name, bronze_name in _REST_TO_BRONZE.items():
        row[bronze_name] = rec.get(rest_name)
    row["icao24"] = str(row["icao24"]).lower()
    if row.get("callsign") is not None:
        row["callsign"] = str(row["callsign"]).strip()
    # hour-of-day stays as a transient key for the per-hour groupby in _flush;
    # it is not stored in the parquet (see _BRONZE_COLS).
    row["_hour_partition"] = datetime.fromtimestamp(int(snap), timezone.utc).hour
    return row


def _flush(buf: list[dict], consumer) -> tuple[int, int]:
    """Write buffered rows to bronze partition(s) and commit offsets."""
    if not buf:
        return 0, 0
    raw = pd.DataFrame(buf)
    raw["icao24"]   = raw["icao24"].astype("string")
    raw["callsign"] = raw["callsign"].astype("string")

    total_bytes = 0
    partition_date = pd.to_datetime(raw["time"], unit="s", utc=True).dt.strftime("%Y-%m-%d")
    for (date, hour), part in raw.groupby([partition_date, raw["_hour_partition"]]):
        out = BRONZE / f"date={date}" / f"hour={hour}" / f"part-{int(time.time()*1000)}.parquet"
        out.parent.mkdir(parents=True, exist_ok=True)
        # Store only the bronze schema columns; hour is in the path, not the file.
        part[_BRONZE_COLS].to_parquet(out, index=False)
        total_bytes += out.stat().st_size

    consumer.commit(asynchronous=False)
    return len(buf), total_bytes


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"))
    p.add_argument("--group", default="etl4-bronze-consumer")
    p.add_argument("--batch-size", type=int, default=5000,
                   help="flush when the buffer reaches this many messages")
    p.add_argument("--idle-seconds", type=float, default=10.0,
                   help="flush (and maybe exit) after this long with no new messages")
    p.add_argument("--exit-on-idle", action="store_true",
                   help="exit after one idle flush (one-shot drain mode)")
    p.add_argument("--max-messages", type=int, default=0,
                   help="exit after this many messages (0 = unlimited)")
    p.add_argument("--from-beginning", action="store_true",
                   help="reset consumer-group offsets to earliest before consuming")
    a = p.parse_args()

    from confluent_kafka import Consumer, KafkaException, TopicPartition

    topic = CFG["kafka"]["topic_raw"]
    conf = {
        "bootstrap.servers":  a.bootstrap,
        "group.id":           a.group,
        "enable.auto.commit": False,
        "auto.offset.reset":  "earliest" if a.from_beginning else "latest",
    }
    consumer = Consumer(conf)

    def _on_assign(c, parts):
        if a.from_beginning:
            for pt in parts:
                pt.offset = 0
        c.assign(parts)
    consumer.subscribe([topic], on_assign=_on_assign)

    stopping = {"flag": False}
    def _stop(signum, _frame):
        stopping["flag"] = True
    for s in (signal.SIGINT, signal.SIGTERM):
        try: signal.signal(s, _stop)
        except (ValueError, AttributeError): pass  # non-main thread or Windows SIGTERM

    buf: list[dict] = []
    last_msg_wall = time.time()
    total_rows = total_bytes = 0

    print(f"[kafka->bronze] topic={topic} group={a.group} "
          f"batch_size={a.batch_size} idle_seconds={a.idle_seconds}")

    with ingest_run("kafka_bronze_consumer", "stream",
                    extras={"topic": topic, "group": a.group}) as rec:
        try:
            while not stopping["flag"]:
                msg = consumer.poll(timeout=1.0)
                now = time.time()

                if msg is None:
                    if buf and (now - last_msg_wall) >= a.idle_seconds:
                        n, b = _flush(buf, consumer); buf.clear()
                        total_rows += n; total_bytes += b
                        print(f"[flush-idle]  rows={n:,}  bytes={b:,}  total={total_rows:,}", flush=True)
                        if a.exit_on_idle:
                            break
                    elif not buf and a.exit_on_idle and (now - last_msg_wall) >= a.idle_seconds:
                        break
                    continue

                if msg.error():
                    raise KafkaException(msg.error())

                try:
                    rec_json = json.loads(msg.value().decode("utf-8"))
                except Exception as exc:
                    print(f"[WARN] skipping undecodable record: {exc}", flush=True)
                    continue
                row = _record_to_bronze_row(rec_json)
                if row is None:
                    continue
                buf.append(row)
                last_msg_wall = now

                if len(buf) >= a.batch_size:
                    n, b = _flush(buf, consumer); buf.clear()
                    total_rows += n; total_bytes += b
                    print(f"[flush-size]  rows={n:,}  bytes={b:,}  total={total_rows:,}", flush=True)

                if a.max_messages and total_rows + len(buf) >= a.max_messages:
                    n, b = _flush(buf, consumer); buf.clear()
                    total_rows += n; total_bytes += b
                    print(f"[flush-limit] rows={n:,}  bytes={b:,}  total={total_rows:,}", flush=True)
                    break
        finally:
            if buf:
                n, b = _flush(buf, consumer); buf.clear()
                total_rows += n; total_bytes += b
                print(f"[flush-final] rows={n:,}  bytes={b:,}  total={total_rows:,}", flush=True)
            consumer.close()
            rec["rows_out"]  = total_rows
            rec["bytes_out"] = total_bytes

    print(f"[kafka->bronze] done. rows={total_rows:,}  bytes={total_bytes:,}")


if __name__ == "__main__":
    main()
