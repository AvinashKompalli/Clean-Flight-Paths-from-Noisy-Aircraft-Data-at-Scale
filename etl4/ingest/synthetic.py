"""Make fake ADS-B state vectors. No OpenSky access needed.

Output: data/bronze/raw_states/date=YYYY-MM-DD/hour=H/part-0.parquet
Columns follow OpenSky state_vectors_data4.
Fake noise is added on purpose: gaps, position jumps, and duplicates.
Use it to test clean, reconstruct, and evaluate before real data arrives.
"""
import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

COLUMNS = [
    "time", "icao24", "lat", "lon", "velocity", "heading", "vertrate",
    "callsign", "onground", "alert", "spi", "squawk", "baroaltitude",
    "geoaltitude", "lastposupdate", "lastcontact", "hour",
]


def load_cfg():
    root = Path(__file__).resolve().parents[1]
    return root, yaml.safe_load((root / "config" / "config.yaml").read_text())


def make_flight(rng, icao24, t0, n_steps, step):
    lat0, lon0 = rng.uniform(8, 35), rng.uniform(68, 90)
    speed = rng.uniform(180, 250)            # m/s
    heading = rng.uniform(0, 360)
    alt = rng.uniform(8000, 11500)
    t = t0 + np.arange(n_steps) * step
    d = speed * step / 111_000.0             # degrees per step, rough
    lat = lat0 + np.cumsum(np.full(n_steps, d * np.cos(np.radians(heading))))
    lon = lon0 + np.cumsum(np.full(n_steps, d * np.sin(np.radians(heading))))
    return pd.DataFrame({
        "time": t,
        "icao24": icao24,
        "lat": lat,
        "lon": lon,
        "velocity": speed + rng.normal(0, 1, n_steps),
        "heading": heading + rng.normal(0, 0.5, n_steps),
        "vertrate": rng.normal(0, 0.5, n_steps),
        "callsign": f"SYN{int(icao24[-3:], 16) % 1000:03d}",
        "onground": False,
        "alert": False,
        "spi": False,
        "squawk": "1200",
        "baroaltitude": alt + rng.normal(0, 5, n_steps),
        "geoaltitude": alt + 30 + rng.normal(0, 5, n_steps),
        "lastposupdate": t - 0.5,
        "lastcontact": t,
    })


def add_noise(df, rng):
    """Gaps, position jumps, duplicates. Keep a clean copy for ground truth."""
    df = df.copy()
    n = len(df)
    jump = rng.random(n) < 0.002
    df.loc[jump, "lat"] += rng.uniform(-1, 1, jump.sum())
    df.loc[jump, "lon"] += rng.uniform(-1, 1, jump.sum())
    df = df[rng.random(n) > 0.03]            # 3 % gaps
    dup = df.sample(frac=0.005, random_state=int(rng.integers(1e6)))
    return pd.concat([df, dup], ignore_index=True)


def main():
    root, cfg = load_cfg()
    p = argparse.ArgumentParser()
    p.add_argument("--hours", type=int, default=1)
    p.add_argument("--date", default="2026-01-01")
    p.add_argument("--out", default=str(root / cfg["paths"]["bronze_raw_states"]))
    a = p.parse_args()

    sc = cfg["synthetic"]
    rng = np.random.default_rng(sc["seed"])
    step = sc["step_seconds"]
    day0 = int(pd.Timestamp(a.date, tz="UTC").timestamp())

    # Real ADS-B: when one flight ends and another on the same airframe
    # begins, there is a dark period. A 45-min inter-flight gap trips every
    # segmenter's 30-min `break_time` so hours do not merge into one flight.
    BETWEEN_FLIGHT_GAP_S = 45 * 60
    clean_parts = []
    for h in range(a.hours):
        t0 = day0 + h * (3600 + BETWEEN_FLIGHT_GAP_S)
        n_steps = 3600 // step
        frames = []
        for i in range(sc["n_aircraft"]):
            icao = f"{0xA00000 + i:06x}"
            frames.append(make_flight(rng, icao, t0, n_steps, step))
        truth = pd.concat(frames, ignore_index=True)
        truth["hour"] = t0
        noisy = add_noise(truth, rng)[COLUMNS]
        d = Path(a.out) / f"date={a.date}" / f"hour={h}"
        d.mkdir(parents=True, exist_ok=True)
        noisy.sort_values("time").to_parquet(d / "part-0.parquet", index=False)
        clean_parts.append(truth)

    # Ground truth for evaluation tests.
    gt_dir = root / "data" / "reference" / "synthetic_truth"
    gt_dir.mkdir(parents=True, exist_ok=True)
    pd.concat(clean_parts).to_parquet(gt_dir / f"{a.date}.parquet", index=False)
    print(f"wrote {a.hours} h of synthetic data under {a.out}")


if __name__ == "__main__":
    main()
