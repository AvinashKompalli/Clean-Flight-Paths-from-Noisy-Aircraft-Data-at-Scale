import pandas as pd

FILE = "opensky_live_data.csv"

df = pd.read_csv(FILE)

print("\n========== DATASET OVERVIEW ==========")
print("Total records:", len(df))
print("Total columns:", len(df.columns))
print("Columns:", df.columns.tolist())

print("\n========== MISSING VALUES ==========")
print(df.isnull().sum())

print("\n========== DUPLICATE RECORDS ==========")
print("Exact duplicate rows:", df.duplicated().sum())

print("\n========== UNIQUE AIRCRAFT ==========")
print("Unique aircraft:", df["icao24"].nunique())

print("\n========== REPEATED AIRCRAFT ==========")
counts = df["icao24"].value_counts()
print(counts.head(10))

print("\n========== POSITION TIMESTAMPS ==========")
print("Missing position timestamps:",
      df["time_position"].isnull().sum())

print("\n========== POSITION VALIDATION ==========")
print("Missing latitude:", df["latitude"].isnull().sum())
print("Missing longitude:", df["longitude"].isnull().sum())

print("\n========== SPEED AND ALTITUDE ==========")
print(df[[
    "velocity",
    "baro_altitude",
    "true_track",
    "vertical_rate"
]].describe())

print("\n========== OBSERVATION SAMPLE ==========")
print(df.head(10).to_string(index=False))

print("\n========== AIRCRAFT WITH MULTIPLE OBSERVATIONS ==========")
repeated = counts[counts > 1]
print("Aircraft observed more than once:", len(repeated))