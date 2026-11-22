"""Build the business marts (gold) from the Data Vault raw vault.

Reads partitioned (silver vault):
  data/silver/vault/hub_flight/ym=YYYY-MM/part.parquet
  data/silver/vault/link_flight_aircraft/ym=YYYY-MM/part.parquet
  data/silver/vault/link_flight_route/ym=YYYY-MM/part.parquet
  data/silver/vault/sat_flight_summary/ym=YYYY-MM/part.parquet
  data/silver/vault/sat_state_observation/ym=YYYY-MM/part.parquet
Reads non-partitioned (silver vault):
  data/silver/vault/hub_aircraft/part.parquet
  data/silver/vault/hub_airport/part.parquet
  data/silver/vault/sat_aircraft_identity/part.parquet

Writes non-partitioned (dims, full-refresh, small):
  data/gold/marts/dim_aircraft/part.parquet
  data/gold/marts/dim_airport/part.parquet
Writes partitioned (facts, per-ym incremental):
  data/gold/marts/fact_flight/ym=YYYY-MM/part.parquet
  data/gold/marts/fact_trajectory_enriched/ym=YYYY-MM/part.parquet

Incremental contract (ADR 2026-11-13, IMPR-01):
  * `watermark.get('marts', 'fact_trajectory_enriched')` returns the last
    fully-processed ym; partitions with ym <= last_ym are skipped.
  * Each per-ym iteration writes both fact partitions, then advances
    pipeline_watermarks for both tables in one psycopg transaction.

The per-ym loop is driven by `vault_io.list_ym('hub_flight')` -- the
flightlist is the calendar authority. Months present in
sat_state_observation but not in hub_flight are not materialized as
fact_trajectory_enriched here; that reconciliation lives in the
run_e2e pre-flight check (T-22, FR-17).

Schema: see etl4/docs/data_vault_design.md section 5.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path

import pandas as pd
import psycopg

from etl4.vault import vault_io, watermark

ROOT              = Path(__file__).resolve().parents[2]
SILVER_VAULT_ROOT = ROOT / "data" / "silver" / "vault"
MARTS_DIR         = ROOT / "data" / "gold" / "marts"

DSN_DEFAULT = os.getenv(
    "POSTGRES_DSN",
    "postgresql://etl4:change_me@localhost:5432/etl4",
)

# Column pushdown for the state-vector satellite: fact_trajectory_enriched
# consumes nine of the twelve columns. Dropping vertrate / onground / load_dt
# / rec_src at the parquet reader cuts peak memory roughly 30% on the 333M-row
# tree (reader_conversion_checklist.md section 1 row 9).
_STATE_COLS = ["hk_aircraft", "time", "lat", "lon", "baroaltitude",
               "velocity", "heading", "callsign"]

_FACT_F_COLS = ["hk_flight", "hk_aircraft", "hk_airport_origin",
                "hk_airport_destination", "callsign", "day",
                "firstseen", "lastseen", "duration_s",
                "altitude_1", "altitude_2"]

_FACT_T_COLS = ["hk_aircraft", "icao24", "time", "lat", "lon", "baroaltitude",
                "velocity", "heading", "callsign", "typecode", "registration",
                "ym"]


def _write_monolith(df: pd.DataFrame, name: str, out_dir: Path) -> int:
    """Write a non-partitioned mart (dim_*) as a single part.parquet."""
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


def _latest_sfs(sfs: pd.DataFrame) -> pd.DataFrame:
    """Keep the latest row per `hk_flight` from sat_flight_summary.

    Cross-month flights (departed month M, arrived M+1) split into two ym=
    partitions with identical `hk_flight` but different `lastseen` / `load_dt`.
    Pick the row with the later `lastseen` (= end-of-flight row), which carries
    the full route info. Tie-break on `load_dt DESC` for the rare case of
    identical `lastseen` (reruns of the same ingest). Reader-side implementation
    of the ADR 2026-11-13 Risk 1 cross-month rule; `_latest_per`'s
    `load_dt`-only sort is insufficient because the earlier-month partition
    can carry a newer `load_dt` if that ingest was rerun.
    """
    if sfs.empty:
        return sfs
    return (sfs.sort_values(
                ["hk_flight", "lastseen", "load_dt"],
                ascending=[True, False, False],
                kind="stable",
              )
              .drop_duplicates(subset=["hk_flight"], keep="first")
              .reset_index(drop=True))


# --------------------------------------------------------------------------
# Dimensions -- full-refresh (small)
# --------------------------------------------------------------------------
def build_dim_aircraft(root: Path = SILVER_VAULT_ROOT) -> pd.DataFrame:
    hub = vault_io.read_table("hub_aircraft", root=root)
    sat = vault_io.read_table("sat_aircraft_identity", root=root)
    if hub.empty:
        return pd.DataFrame(columns=["hk_aircraft", "icao24", "registration", "typecode"])
    latest = _latest_per(sat, ["hk_aircraft"])[["hk_aircraft", "registration", "typecode"]] \
             if not sat.empty else pd.DataFrame(columns=["hk_aircraft", "registration", "typecode"])
    out = hub[["hk_aircraft", "icao24"]].merge(latest, on="hk_aircraft", how="left")
    return out


def build_dim_airport(root: Path = SILVER_VAULT_ROOT) -> pd.DataFrame:
    hub = vault_io.read_table("hub_airport", root=root)
    if hub.empty:
        return pd.DataFrame(columns=["hk_airport", "icao_code"])
    return hub[["hk_airport", "icao_code"]].reset_index(drop=True)


# --------------------------------------------------------------------------
# Facts -- partitioned per ym
# --------------------------------------------------------------------------
def build_fact_flight_for_ym(ym: str, root: Path = SILVER_VAULT_ROOT) -> pd.DataFrame:
    hub = vault_io.read_table("hub_flight",           ym_filter=ym, root=root)
    lfa = vault_io.read_table("link_flight_aircraft", ym_filter=ym, root=root)
    lfr = vault_io.read_table("link_flight_route",    ym_filter=ym, root=root)
    sfs = vault_io.read_table("sat_flight_summary",   ym_filter=ym, root=root)
    if hub.empty:
        return pd.DataFrame(columns=_FACT_F_COLS)
    out = hub[["hk_flight", "callsign", "day"]].copy()
    if not lfa.empty:
        out = out.merge(_latest_per(lfa, ["hk_flight"])[["hk_flight", "hk_aircraft"]],
                        on="hk_flight", how="left")
    if not lfr.empty:
        out = out.merge(_latest_per(lfr, ["hk_flight"])[
                            ["hk_flight", "hk_airport_origin", "hk_airport_destination"]],
                        on="hk_flight", how="left")
    if not sfs.empty:
        summ = _latest_sfs(sfs)[
            ["hk_flight", "firstseen", "lastseen", "duration_s",
             "altitude_1", "altitude_2"]]
        out = out.merge(summ, on="hk_flight", how="left")
    for c in _FACT_F_COLS:
        if c not in out.columns:
            out[c] = pd.NA
    return out[_FACT_F_COLS].reset_index(drop=True)


def build_fact_trajectory_enriched_for_ym(ym: str,
                                          dim_aircraft: pd.DataFrame,
                                          root: Path = SILVER_VAULT_ROOT) -> pd.DataFrame:
    """Left-join state observations onto dim_aircraft so every row carries
    typecode/registration when available. Keeps callsign from the state
    observation (changes mid-day per aircraft). The output `ym` column is
    derived from the state epoch, which matches the partition name by
    construction (sat_state_observation is ym-partitioned on the same
    epoch helper, architecture.md section 2.5).
    """
    sat = vault_io.read_table("sat_state_observation",
                              ym_filter=ym,
                              columns=_STATE_COLS,
                              root=root)
    if sat.empty:
        return pd.DataFrame(columns=_FACT_T_COLS)
    if dim_aircraft.empty:
        dim_aircraft = pd.DataFrame(
            columns=["hk_aircraft", "icao24", "registration", "typecode"]
        )
    enriched = sat.merge(
        dim_aircraft[["hk_aircraft", "icao24", "registration", "typecode"]],
        on="hk_aircraft", how="left",
    )
    enriched["callsign"] = enriched["callsign"].astype("string").str.strip()
    enriched = enriched.assign(ym=vault_io.ym_from_epoch(enriched["time"]))
    return enriched[_FACT_T_COLS].reset_index(drop=True)


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------
def _yms_to_process(vault_root: Path, last_ym: str | None) -> list[str]:
    yms = list(vault_io.list_ym("hub_flight", root=vault_root))
    if last_ym is None:
        return yms
    return [y for y in yms if y > last_ym]


def build_marts(out_dir: Path = MARTS_DIR,
                vault_root: Path = SILVER_VAULT_ROOT,
                dsn: str = DSN_DEFAULT) -> dict:
    """Build dims (full-refresh) + facts (per-ym incremental). Returns row
    counts: dim_* are cumulative, fact_* reflect the sum across newly
    processed partitions for this invocation."""
    counts: dict[str, int] = {
        "dim_aircraft": 0, "dim_airport": 0,
        "fact_flight": 0, "fact_trajectory_enriched": 0,
    }

    print("building dim_aircraft ...", flush=True)
    dim_a = build_dim_aircraft(root=vault_root)
    counts["dim_aircraft"] = _write_monolith(dim_a, "dim_aircraft", out_dir)
    enriched_pct = 100.0 * dim_a["typecode"].notna().mean() if len(dim_a) else 0.0
    print(f"  rows={counts['dim_aircraft']:,}  "
          f"typecode filled: {enriched_pct:.1f}%", flush=True)

    print("building dim_airport ...", flush=True)
    counts["dim_airport"] = _write_monolith(
        build_dim_airport(root=vault_root), "dim_airport", out_dir,
    )
    print(f"  rows={counts['dim_airport']:,}", flush=True)

    last_ym = watermark.get("marts", "fact_trajectory_enriched", dsn=dsn)
    if last_ym is not None:
        print(f"  resuming from watermark: last_ym={last_ym}", flush=True)
    else:
        print("  no marts watermark; processing every vault ym", flush=True)

    yms = _yms_to_process(vault_root, last_ym)
    if not yms:
        print("  no new vault months to process", flush=True)
        return counts

    print(f"  will process {len(yms)} new month(s): {yms[0]} .. {yms[-1]}",
          flush=True)

    for idx, ym in enumerate(yms):
        print(f"  [{idx + 1}/{len(yms)}] ym={ym}", flush=True)

        fact_f = build_fact_flight_for_ym(ym, root=vault_root)
        fact_t = build_fact_trajectory_enriched_for_ym(
            ym, dim_a, root=vault_root,
        )

        vault_io.write_partition(fact_f, "fact_flight", ym, out_dir=out_dir)
        vault_io.write_partition(
            fact_t, "fact_trajectory_enriched", ym, out_dir=out_dir,
        )
        print(f"    fact_flight/ym={ym}: {len(fact_f):,}", flush=True)
        print(f"    fact_trajectory_enriched/ym={ym}: {len(fact_t):,}",
              flush=True)

        counts["fact_flight"]              += len(fact_f)
        counts["fact_trajectory_enriched"] += len(fact_t)

        with psycopg.connect(dsn) as conn:
            watermark.set(
                "marts", "fact_flight", ym,
                vault_io.row_count("fact_flight", ym_filter=ym, root=out_dir),
                conn,
            )
            watermark.set(
                "marts", "fact_trajectory_enriched", ym,
                vault_io.row_count(
                    "fact_trajectory_enriched", ym_filter=ym, root=out_dir,
                ),
                conn,
            )
            conn.commit()

    return counts


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--out", default=str(MARTS_DIR))
    p.add_argument("--vault-root", default=str(SILVER_VAULT_ROOT))
    p.add_argument("--dsn", default=DSN_DEFAULT)
    a = p.parse_args()
    build_marts(Path(a.out), Path(a.vault_root), a.dsn)


if __name__ == "__main__":
    main()
