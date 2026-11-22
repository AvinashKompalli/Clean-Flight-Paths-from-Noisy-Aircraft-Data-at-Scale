"""Debug probe: why does kinematic_smoothing N1 yield ~0 RMSE?

Load silver, truth parquet, gold reconstruction; join on __truth_id;
print lat/lon deltas on the raw silver, smoothed gold, and the
inject_noise-applied silver rows vs the clean truth.
"""
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

root = Path(__file__).resolve().parents[4]
silver = pd.read_parquet(root / "data/silver/cleaned_states/synthetic/N1/part.parquet")
truth  = pd.read_parquet(root / "data/reference/synthetic_truth/t003_cell.parquet")
gold   = pd.read_parquet(root / "data/gold/marts/reconstructions/synthetic/N1/method=kinematic_smoothing/part.parquet")

print("silver cols lat/lon:", [c for c in silver.columns if "lat" in c or "lon" in c])
print("gold cols lat/lon:", [c for c in gold.columns if "lat" in c or "lon" in c])
print("rows: silver", len(silver), "gold", len(gold), "truth", len(truth))

m = silver.merge(truth[["__truth_id", "lat", "lon"]], on="__truth_id", suffixes=("_s", "_t"))
print("silver merge rows:", len(m))
print("silver vs truth lat diff max:", (m["lat_s"] - m["lat_t"]).abs().max())
print("silver vs truth lon diff max:", (m["lon_s"] - m["lon_t"]).abs().max())

if "smoothed_lat" in gold.columns:
    g = gold[["__truth_id", "smoothed_lat", "smoothed_lon"]].merge(
        truth[["__truth_id", "lat", "lon"]], on="__truth_id")
    print("gold smoothed vs truth lat diff max:", (g["smoothed_lat"] - g["lat"]).abs().max())
    print("gold smoothed vs truth lon diff max:", (g["smoothed_lon"] - g["lon"]).abs().max())
    print("sample smoothed vs truth:")
    print(g.head(10))
