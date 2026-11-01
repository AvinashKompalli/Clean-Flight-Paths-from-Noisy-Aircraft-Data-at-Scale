"""Poll the OpenSky REST API and stream live state vectors into Kafka.

No Trino. No S3 download. Calls the public REST endpoint every --interval
seconds and pushes each state vector as a JSON record to adsb.raw.

Key = icao24 so one aircraft always lands in the same partition (order kept).
Schema matches synthetic.py COLUMNS so the downstream pipeline sees identical
field names whether the source is live, synthetic, or replayed Parquet.

Authentication (OAuth2 client credentials):
  Set OPENSKY_CLIENT_ID and OPENSKY_CLIENT_SECRET in .env.
  With credentials: poll every 5 s, 4000 credits/day.
  Without         : poll every 10 s,  400 credits/day (anonymous).

Usage:
  python ingest/live_producer.py                        # global, every 10 s
  python ingest/live_producer.py --interval 5           # with credentials
  python ingest/live_producer.py --bounds 68,8,97,37    # India only
  python ingest/live_producer.py --limit 500            # stop after 500 records
"""
import argparse
import json
import math
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
import yaml
from dotenv import load_dotenv

load_dotenv()
ROOT = Path(__file__).resolve().parents[1]
CFG = yaml.safe_load((ROOT / "config" / "config.yaml").read_text())

OPENSKY_URL   = "https://opensky-network.org/api/states/all"
OAUTH_URL     = ("https://auth.opensky-network.org/auth/realms/"
                 "opensky-network/protocol/openid-connect/token")
TOKEN_MARGIN  = 30   # refresh the token this many seconds before it expires

# Full 17-field raw state vector shape, in order, as returned by the REST API.
# Keep ALL fields raw — projection/renaming is a downstream pipeline concern.
_RAW_FIELDS = [
    "icao24",         # 0
    "callsign",       # 1
    "origin_country", # 2
    "time_position",  # 3
    "last_contact",   # 4
    "longitude",      # 5
    "latitude",       # 6
    "baro_altitude",  # 7
    "on_ground",      # 8
    "velocity",       # 9
    "true_track",     # 10 (OpenSky's term for heading)
    "vertical_rate",  # 11
    "sensors",        # 12
    "geo_altitude",   # 13
    "squawk",         # 14
    "spi",            # 15
    "position_source",# 16
]


class _TokenCache:
    """Fetch and cache an OAuth2 client-credentials bearer token."""

    def __init__(self, client_id, client_secret):
        self._client_id     = client_id
        self._client_secret = client_secret
        self._token         = None
        self._expires_at    = 0.0

    def header(self):
        """Return the Authorization header value, refreshing if needed."""
        if time.time() >= self._expires_at:
            self._refresh()
        return f"Bearer {self._token}"

    def _refresh(self):
        r = requests.post(OAUTH_URL, data={
            "grant_type":    "client_credentials",
            "client_id":     self._client_id,
            "client_secret": self._client_secret,
        }, timeout=15)
        r.raise_for_status()
        body = r.json()
        self._token      = body["access_token"]
        self._expires_at = time.time() + body.get("expires_in", 300) - TOKEN_MARGIN
        print("[auth] token refreshed")


def fetch_states(session, bounds=None, token_cache=None):
    """Call the REST API once. Returns (snapshot_unix_ts, list_of_rows)."""
    params = {}
    if bounds:
        params = {"lamin": bounds[1], "lomin": bounds[0],
                  "lamax": bounds[3], "lomax": bounds[2]}
    headers = {}
    if token_cache:
        headers["Authorization"] = token_cache.header()
    r = session.get(OPENSKY_URL, params=params, headers=headers, timeout=30)
    r.raise_for_status()
    body = r.json()
    return body.get("time", int(time.time())), body.get("states") or []


def _to_raw_record(row, snapshot_ts):
    """Preserve the raw REST state array verbatim as a dict.

    Only metadata we add is `snapshot_time` (the API's `time` field) so the
    downstream pipeline knows when the batch was observed. Field names use
    OpenSky's own vocabulary; downstream clean/project stage renames them.
    """
    rec = {name: row[i] for i, name in enumerate(_RAW_FIELDS)}
    rec["snapshot_time"] = snapshot_ts
    return rec


def _safe(v):
    """Replace NaN / inf with None so json.dumps never raises."""
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    return v


def main():
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--interval", type=float, default=10.0,
                   help="seconds between polls (default 10; minimum 5 with account)")
    p.add_argument("--limit", type=int, default=0,
                   help="stop after this many records total (0 = run forever)")
    p.add_argument("--bounds", default=None,
                   help="west,south,east,north e.g. 68,8,97,37 for India bounding box")
    a = p.parse_args()

    bounds = None
    if a.bounds:
        w, s, e, n = map(float, a.bounds.split(","))
        bounds = (w, s, e, n)

    client_id     = os.getenv("OPENSKY_CLIENT_ID")  or None
    client_secret = os.getenv("OPENSKY_CLIENT_SECRET") or None
    token_cache   = _TokenCache(client_id, client_secret) if client_id else None
    auth_label    = f"OAuth2 ({client_id})" if client_id else "anonymous"

    from confluent_kafka import Producer
    producer = Producer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
        "linger.ms": 50,
        "compression.type": "lz4",
    })
    topic_raw = CFG["kafka"]["topic_raw"]
    topic_dlq = CFG["kafka"]["topic_dlq"]

    http = requests.Session()
    http.headers["User-Agent"] = "etl4-iisc-live-producer/1.0 (academic)"

    total_sent = total_polls = 0
    print("Live producer started.")
    print(f"  topic    : {topic_raw}")
    print(f"  auth     : {auth_label}")
    print(f"  interval : {a.interval} s")
    print(f"  bounds   : {a.bounds or 'global'}")
    print(f"  limit    : {a.limit or 'unlimited'}")
    print("Press Ctrl+C to stop.\n")

    try:
        while True:
            t0 = time.time()

            try:
                snapshot_ts, rows = fetch_states(http, bounds, token_cache)
            except Exception as exc:
                print(f"[WARN] poll failed: {exc} — retrying after {a.interval} s")
                time.sleep(a.interval)
                continue

            batch_sent = batch_err = 0
            for row in rows:
                # Keep rows with missing lat/lon too — downstream decides.
                # The 17-element shape is contractual; only reject malformed arrays.
                if not isinstance(row, list) or len(row) < len(_RAW_FIELDS):
                    producer.produce(
                        topic_dlq, key="malformed",
                        value=json.dumps({"error": "bad_shape", "row": row}),
                    )
                    batch_err += 1
                    continue
                icao24 = row[0] or "unknown"
                try:
                    record = _to_raw_record(row, snapshot_ts)
                    value = json.dumps({k: _safe(v) for k, v in record.items()})
                    producer.produce(topic_raw, key=icao24, value=value)
                    batch_sent += 1
                except Exception as exc:
                    producer.produce(
                        topic_dlq, key=icao24,
                        value=json.dumps({"icao24": icao24, "error": str(exc)}),
                    )
                    batch_err += 1
                producer.poll(0)

            producer.flush()
            total_sent += batch_sent
            total_polls += 1

            ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
            print(f"[{ts}]  poll={total_polls:4d}  sent={batch_sent:6,}  "
                  f"err={batch_err}  cumulative={total_sent:,}")

            if a.limit and total_sent >= a.limit:
                print(f"Reached --limit {a.limit}. Stopping.")
                break

            time.sleep(max(0.0, a.interval - (time.time() - t0)))

    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        producer.flush()
        print(f"Total: {total_sent:,} records across {total_polls} polls.")


if __name__ == "__main__":
    main()
