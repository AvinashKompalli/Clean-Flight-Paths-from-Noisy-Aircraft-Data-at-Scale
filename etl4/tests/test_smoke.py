import subprocess
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


def test_synthetic_generates_valid_bronze(tmp_path):
    out = tmp_path / "raw_states"
    subprocess.run(
        [sys.executable, str(ROOT / "ingest" / "synthetic.py"),
         "--hours", "1", "--date", "2026-01-01", "--out", str(out)],
        check=True,
    )
    f = out / "date=2026-01-01" / "hour=0" / "part-0.parquet"
    assert f.exists()
    df = pd.read_parquet(f)
    for col in ["time", "icao24", "lat", "lon", "baroaltitude", "hour"]:
        assert col in df.columns
    assert df["icao24"].nunique() == 20
    assert len(df) > 20 * 300          # 360 steps per aircraft minus gaps
    assert df["lat"].between(-90, 90).all()
