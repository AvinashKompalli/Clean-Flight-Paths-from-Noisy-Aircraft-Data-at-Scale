"""Live smoke test: produce N bronze rows to adsb.raw and consume them back.
Prints throughput and asserts the shape round-trips.
"""
import json
import os
import sys
import time
from pathlib import Path

from confluent_kafka import Consumer, KafkaError, Producer
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")
BOOT  = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "adsb.raw"
N     = 500


def produce():
    import pandas as pd
    root = Path(__file__).resolve().parents[1] / "data" / "bronze" / "raw_states"
    files = sorted(root.rglob("*.parquet"))
    assert files, f"no bronze Parquet under {root} — run synthetic.py first"
    df = pd.read_parquet(files[0]).head(N)

    p = Producer({"bootstrap.servers": BOOT, "linger.ms": 10})
    t0 = time.time()
    for row in df.itertuples(index=False):
        payload = {k: (None if (isinstance(v, float) and v != v) else v)
                   for k, v in row._asdict().items()}
        p.produce(TOPIC, key=str(row.icao24), value=json.dumps(payload, default=str))
        p.poll(0)
    p.flush()
    print(f"produced {len(df):,} records in {time.time() - t0:.2f}s")
    return len(df)


def consume(expected):
    c = Consumer({
        "bootstrap.servers": BOOT,
        "group.id":          f"smoke-{int(time.time())}",
        "auto.offset.reset": "earliest",
    })
    c.subscribe([TOPIC])
    seen = 0
    t0 = time.time()
    while seen < expected and (time.time() - t0) < 20:
        msg = c.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            print(f"consume error: {msg.error()}", file=sys.stderr)
            break
        rec = json.loads(msg.value())
        assert "icao24" in rec, f"missing icao24 in {rec}"
        seen += 1
    c.close()
    print(f"consumed {seen:,} records in {time.time() - t0:.2f}s")
    return seen


if __name__ == "__main__":
    n = produce()
    got = consume(n)
    assert got >= n, f"expected {n}, got {got}"
    print("OK — Kafka round-trip verified")
