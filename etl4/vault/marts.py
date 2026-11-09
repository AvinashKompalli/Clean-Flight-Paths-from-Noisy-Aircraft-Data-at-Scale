"""Build the business marts (gold) from the Data Vault raw vault.

Reads  data/silver/vault/*/part.parquet
Writes data/gold/marts/dim_aircraft/part.parquet
       data/gold/marts/dim_airport/part.parquet
       data/gold/marts/fact_flight/part.parquet
       data/gold/marts/fact_trajectory_enriched/part.parquet

Marts are rebuilt from scratch each load — they are small enough to full-
refresh, and the vault is the system of record anyway. Schema: see
etl4/docs/data_vault_design.md §5.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

ROOT      = Path(__file__).resolve().parents[2]
VAULT_DIR = ROOT / "data" / "silver" / "vault"
MARTS_DIR = ROOT / "data" / "gold" / "marts"


def _read(name: str) -> pd.DataFrame:
    """Read one vault table (handles the empty case)."""
    p = VAULT_DIR / name / "part.parquet"
    if not p.exists():
        return pd.DataFrame()
    return pd.read_parquet(p)


def _write(df: pd.DataFrame, name: str, out_dir: Path) -> int:
    dst = out_dir / name / "part.parquet"
    dst.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(dst, index=False)
    return len(df)


def _latest_per(df: pd.DataFrame, pk_cols: list[str]) -> pd.DataFrame:
    """Keep the latest row per primary key, by load_dt desc.

    The satellite convention is to version by load_dt. Marts pull the current
    truth. Expresses `ROW_NUMBER() OVER (PARTITION BY pk ORDER BY load_dt DESC) = 1`.
    """
    if df.empty:
        return df
    return (df.sort_values("load_dt", ascending=False)
              .drop_duplicates(subset=pk_cols, keep="first")
              .reset_index(drop=True))


def build_dim_aircraft() -> pd.DataFrame:
    hub = _read("hub_aircraft")
    sat = _read("sat_aircraft_identity")
    if hub.empty:
        return pd.DataFrame(columns=["hk_aircraft", "icao24", "registration", "typecode"])
    latest = _latest_per(sat, ["hk_aircraft"])[["hk_aircraft", "registration", "typecode"]]
    out = hub[["hk_aircraft", "icao24"]].merge(latest, on="hk_aircraft", how="left")
    return out


def build_dim_airport() -> pd.DataFrame:
    hub = _read("hub_airport")
    if hub.empty:
        return pd.DataFrame(columns=["hk_airport", "icao_code"])
    return hub[["hk_airport", "icao_code"]].reset_index(drop=True)


def build_fact_flight() -> pd.DataFrame:
    hub   = _read("hub_flight")
    lfa   = _read("link_flight_aircraft")
    lfr   = _read("link_flight_route")
    sfs   = _read("sat_flight_summary")
    cols = ["hk_flight", "hk_aircraft", "hk_airport_origin", "hk_airport_destination",
            "callsign", "day", "firstseen", "lastseen", "duration_s",
            "altitude_1", "altitude_2"]
    if hub.empty:
        return pd.DataFrame(columns=cols)
    out = hub[["hk_flight", "callsign", "day"]].copy()
    if not lfa.empty:
        out = out.merge(_latest_per(lfa, ["hk_flight"])[["hk_flight", "hk_aircraft"]],
                        on="hk_flight", how="left")
    if not lfr.empty:
        out = out.merge(_latest_per(lfr, ["hk_flight"])[
                            ["hk_flight", "hk_airport_origin", "hk_airport_destination"]],
                        on="hk_flight", how="left")
    if not sfs.empty:
        summ = _latest_per(sfs, ["hk_flight"])[
            ["hk_flight", "firstseen", "lastseen", "duration_s", "altitude_1", "altitude_2"]]
        out = out.merge(summ, on="hk_flight", how="left")
    # ensure every expected column exists even when a source satellite is missing
    for c in cols:
        if c not in out.columns:
            out[c] = pd.NA
    return out[cols]


def build_fact_trajectory_enriched(dim_aircraft: pd.DataFrame) -> pd.DataFrame:
    """Left-join state observations onto dim_aircraft so every row carries
    typecode/registration when available from the flightlist join.

    Keeps callsign from the state observation so downstream UIs can filter
    the trajectory by the live radio callsign (changes mid-day per aircraft).
    """
    sat = _read("sat_state_observation")
    cols = ["hk_aircraft", "icao24", "time", "lat", "lon", "baroaltitude",
            "velocity", "heading", "callsign", "typecode", "registration"]
    if sat.empty:
        return pd.DataFrame(columns=cols)
    if dim_aircraft.empty:
        dim_aircraft = pd.DataFrame(columns=["hk_aircraft", "icao24", "registration", "typecode"])
    enriched = sat.merge(
        dim_aircraft[["hk_aircraft", "icao24", "registration", "typecode"]],
        on="hk_aircraft", how="left",
    )
    # Normalise callsign spacing so filter dropdowns don't show padding dupes.
    enriched["callsign"] = enriched["callsign"].astype("string").str.strip()
    return enriched[cols]


def build_marts(out_dir: Path = MARTS_DIR) -> dict:
    """Build every mart. Returns row counts."""
    counts = {}

    print("building dim_aircraft ...")
    dim_a = build_dim_aircraft()
    counts["dim_aircraft"] = _write(dim_a, "dim_aircraft", out_dir)
    enriched_pct = 100.0 * dim_a["typecode"].notna().mean() if len(dim_a) else 0.0
    print(f"  rows={counts['dim_aircraft']:,}  "
          f"typecode filled: {enriched_pct:.1f}%")

    print("building dim_airport ...")
    counts["dim_airport"] = _write(build_dim_airport(), "dim_airport", out_dir)
    print(f"  rows={counts['dim_airport']:,}")

    print("building fact_flight ...")
    fact_f = build_fact_flight()
    counts["fact_flight"] = _write(fact_f, "fact_flight", out_dir)
    print(f"  rows={counts['fact_flight']:,}")

    print("building fact_trajectory_enriched ...")
    fact_t = build_fact_trajectory_enriched(dim_a)
    counts["fact_trajectory_enriched"] = _write(fact_t, "fact_trajectory_enriched", out_dir)
    if len(fact_t):
        hit_rate = 100.0 * fact_t["typecode"].notna().mean()
        print(f"  rows={counts['fact_trajectory_enriched']:,}  "
              f"state-vector rows enriched with typecode: {hit_rate:.1f}%")
    else:
        print("  rows=0")

    return counts


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--out", default=str(MARTS_DIR))
    a = p.parse_args()
    build_marts(Path(a.out))


if __name__ == "__main__":
    main()
