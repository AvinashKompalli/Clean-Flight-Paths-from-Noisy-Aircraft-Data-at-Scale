# pip install pyopensky
# pip install 'pyopensky[rs1090]'   # only if you use the rs1090 decoder

from pathlib import Path

from pyopensky.trino import Trino
from pyopensky.rebuild import Rebuild

# ---- Settings: change these ----
START = "2023-01-03 16:00"
STOP = "2023-01-03 20:00"
ICAO24 = "400A0E"          # one code, a list of codes, or None
BOUNDS = None              # (west, south, east, north) or None
DECODER = None             # None = OpenSky values, "pymodes", or "rs1090"
INCLUDE_ROLLCALL = False   # True adds squawk data
OUT_FILE = Path("data/opensky_state_vectors.parquet")

# ---- Run ----
# Trino needs an approved OpenSky account.
# Set up credentials first: https://mode-s.org/pyopensky/credentials.html
trino = Trino()
rebuild = Rebuild(trino)

df = rebuild.rebuild(
    start=START,
    stop=STOP,
    icao24=ICAO24,
    bounds=BOUNDS,
    decoder=DECODER,
    include_rollcall=INCLUDE_ROLLCALL,
    cached=True,           # reuse earlier downloads
)

if df is None or df.empty:
    raise SystemExit("No data returned. Check time window, icao24, and access.")

print(df.shape)
print(df.head())

# ---- Save ----
OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
df.to_parquet(OUT_FILE, index=False)
print(f"Saved to {OUT_FILE}")