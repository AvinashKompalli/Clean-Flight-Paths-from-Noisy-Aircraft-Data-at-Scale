# Data Vault 2.0 Layer — Design

This document describes the Data Vault 2.0 layer this project adds *on top of*
the existing reconstruction medallion (`data/bronze`, `data/silver`,
`data/gold`). The reconstruction pipeline keeps its single-source medallion
unchanged — the vault adds a parallel structure under `data/silver/vault/` and
`data/gold/marts/` that integrates **two independent sources** on the `icao24`
business key.

## 1. Why Data Vault here (and why not everywhere)

**Added where it helps.** We have two source systems:

| Source | Shape | Carries |
|---|---|---|
| OpenSky S3 state vectors | 1 Hz per-aircraft time series | lat, lon, altitude, velocity, callsign |
| Zenodo flightlists (DOI 3737101) | 1 row per flight | icao24, registration, typecode, origin, destination, firstseen, lastseen |

They share `icao24` as the natural business key. Without a vault we'd re-plumb
the integration for every new question. With the vault we load each source
*once*, into hubs/links/sats, and build marts per use case.

Data Vault earns its place here because:
- **Multi-source integration** on a shared natural key — the canonical DV use case.
- **Insert-only load pattern** — reruns are idempotent; no update/delete surgery.
- **Hash keys** (`md5(icao24)`, `md5(callsign|day)`) parallelise cleanly for future Spark port.
- **History preservation** — monthly Zenodo versions will add new rows to `sat_aircraft_identity` without schema change; latest wins at mart build.
- **Schema isolation** — adding a third source (e.g. ADS-B Exchange) is a new set of hubs/sats, no mart change.

Data Vault is explicitly **not applied** to:
- The reconstruction medallion (`data/silver/<source>/<noise>/part.parquet`) — single source per run, trajectory computations in-memory. DV would add overhead for no integration benefit.
- Short-lived eval artifacts (`matrix.csv`, `runs.csv`). These are audit outputs, not warehouse facts.

## 2. Layout on disk

```
data/
  bronze/
    raw_states/                                          ← S3 state vectors (unchanged)
    synthetic/                                           ← generator output (unchanged)
    flightlists/                                         ← NEW: Zenodo .csv.gz → parquet
      v23.00/
        flightlist_20221201_20221231.parquet
  silver/
    s3/<noise>/part.parquet                              ← existing reconstruction canonical (unchanged)
    vault/                                               ← NEW: raw vault
      hub_aircraft/part.parquet
      hub_airport/part.parquet
      hub_flight/part.parquet
      link_flight_aircraft/part.parquet
      link_flight_route/part.parquet
      sat_aircraft_identity/part.parquet
      sat_flight_summary/part.parquet
      sat_state_observation/part.parquet
  gold/
    s3/<noise>/method=.../part.parquet                   ← existing reconstructed trajectories (unchanged)
    marts/                                               ← NEW: business marts
      dim_aircraft/part.parquet
      dim_airport/part.parquet
      fact_flight/part.parquet
      fact_trajectory_enriched/part.parquet
```

## 3. Hash-key convention

All hash keys are MD5 over a canonicalised business-key string. Source values
are stripped and uppercased so case/whitespace differences don't fork the key.

```
HK_AIRCRAFT = md5( upper(strip(icao24)) )
HK_AIRPORT  = md5( upper(strip(icao_airport_code)) )
HK_FLIGHT   = md5( upper(strip(callsign)) + "|" + day_yyyymmdd )
```

Business keys are 6-char hex ICAO 24-bit (`icao24`), 4-letter ICAO airport
codes, and (callsign, day) pairs. `day` is the UTC date the flight is
primarily attributed to in the Zenodo flightlist.

## 4. Raw vault — table-by-table

Every raw-vault table carries three audit columns:

- `load_dt` — timestamp (UTC) of the vault load that inserted this row.
- `rec_src` — record source; one of `opensky_s3`, `opensky_zenodo`, `synthetic`.
- `rec_hash` — md5 of the business-key-excluding attributes (satellites only; detects change-in-attributes).

### 4.1 Hubs

**`hub_aircraft`** — one row per unique `icao24` ever observed.

| col | type | notes |
|---|---|---|
| hk_aircraft | string | md5(icao24) |
| icao24      | string | 6-char hex, uppercased |
| load_dt     | timestamp |
| rec_src     | string |

**`hub_airport`** — one row per ICAO airport code seen as origin or destination.

| col | type | notes |
|---|---|---|
| hk_airport  | string | md5(code) |
| icao_code   | string | 4-letter ICAO, uppercased |
| load_dt     | timestamp |
| rec_src     | string |

**`hub_flight`** — one row per (callsign, day) tuple.

| col | type | notes |
|---|---|---|
| hk_flight   | string | md5(callsign + "|" + day) |
| callsign    | string | uppercased, whitespace-stripped |
| day         | date   | UTC |
| load_dt     | timestamp |
| rec_src     | string |

### 4.2 Links

**`link_flight_aircraft`** — a flight was operated by an aircraft.

| col | type | notes |
|---|---|---|
| hk_flight_aircraft | string | md5(hk_flight + "|" + hk_aircraft) |
| hk_flight          | string | FK |
| hk_aircraft        | string | FK |
| load_dt            | timestamp |
| rec_src            | string |

**`link_flight_route`** — a flight had an origin and destination airport.

| col | type | notes |
|---|---|---|
| hk_flight_route | string | md5(hk_flight + "|" + hk_airport_origin + "|" + hk_airport_destination) |
| hk_flight       | string | FK |
| hk_airport_origin      | string | FK (nullable — origin may be unknown) |
| hk_airport_destination | string | FK (nullable — destination may be unknown) |
| load_dt         | timestamp |
| rec_src         | string |

### 4.3 Satellites

**`sat_aircraft_identity`** — mutable aircraft metadata (registration, typecode).

| col | type | notes |
|---|---|---|
| hk_aircraft   | string | FK into hub_aircraft |
| load_dt       | timestamp | PK with hk_aircraft |
| rec_src       | string |
| rec_hash      | string | md5 of registration + typecode |
| registration  | string | e.g. "TC-JJU" |
| typecode      | string | ICAO aircraft type e.g. "B77W" |

Subsequent monthly loads from Zenodo add new rows only when `rec_hash` differs
from the previous load for that `hk_aircraft`. Marts pick the latest row by
`load_dt` per aircraft.

**`sat_flight_summary`** — per-flight summary attributes.

| col | type | notes |
|---|---|---|
| hk_flight   | string | FK |
| load_dt     | timestamp | PK with hk_flight |
| rec_src     | string |
| rec_hash    | string |
| firstseen   | timestamp | UTC |
| lastseen    | timestamp | UTC |
| latitude_1  | float64 | origin position |
| longitude_1 | float64 |
| altitude_1  | float64 |
| latitude_2  | float64 | destination position |
| longitude_2 | float64 |
| altitude_2  | float64 |
| duration_s  | float64 | lastseen − firstseen |

**`sat_state_observation`** — 1 Hz state-vector observations (the long-tail
satellite; one row per bronze state-vector row).

| col | type | notes |
|---|---|---|
| hk_aircraft    | string | FK |
| time           | int64  | unix seconds — PK with hk_aircraft |
| load_dt        | timestamp |
| rec_src        | string |
| lat            | float64 |
| lon            | float64 |
| baroaltitude   | float64 |
| velocity       | float64 |
| heading        | float64 |
| vertrate       | float64 |
| callsign       | string |
| onground       | boolean |

## 5. Business marts (gold)

Marts are built by joining the raw vault; the pattern is "latest satellite per
hub" via `ROW_NUMBER() OVER (PARTITION BY hk ORDER BY load_dt DESC)`.

**`dim_aircraft`** — current identity per aircraft.

| col | type | notes |
|---|---|---|
| hk_aircraft   | string | PK |
| icao24        | string |
| registration  | string | latest known |
| typecode      | string | latest known |

**`dim_airport`** — just the airport business key for now.

| col | type | notes |
|---|---|---|
| hk_airport | string | PK |
| icao_code  | string |

**`fact_flight`** — one row per flight with foreign keys to the dimensions.

| col | type | notes |
|---|---|---|
| hk_flight         | string | PK |
| hk_aircraft       | string | FK |
| hk_airport_origin | string | FK (nullable) |
| hk_airport_destination | string | FK (nullable) |
| callsign          | string |
| day               | date |
| firstseen         | timestamp |
| lastseen          | timestamp |
| duration_s        | float64 |
| altitude_1 / altitude_2 | float64 |

**`fact_trajectory_enriched`** — state-vector observations enriched with
aircraft typecode from the flightlist join. One row per original bronze state
vector; `typecode` is `null` when the aircraft isn't present in any ingested
flightlist.

| col | type | notes |
|---|---|---|
| hk_aircraft  | string | FK |
| icao24       | string | |
| time         | int64 | event time |
| lat / lon    | float64 |
| baroaltitude | float64 |
| velocity     | float64 |
| heading      | float64 |
| typecode     | string | enriched from `dim_aircraft` (left join; nullable) |
| registration | string | enriched (nullable) |

## 6. Load order

The vault runner (`etl4/vault/run_vault.py`) applies this deterministic order:

1. Bronze flightlists (`etl4/vault/bronze_flightlists.py`) — one-time
   decompress + typed-read per Zenodo file.
2. `hub_aircraft`, `hub_airport`, `hub_flight` — insert new business keys only.
3. `link_flight_aircraft`, `link_flight_route` — insert new relationship rows.
4. `sat_aircraft_identity`, `sat_flight_summary`, `sat_state_observation` —
   insert when `rec_hash` differs from the previous row for the same business key.
5. Marts are rebuilt from scratch each load (small enough to full-refresh).

Each step prints row counts in and out and is idempotent.

## 7. What the vault enables

- **Enriched reconstruction**: join the Kalman-smoothed trajectory to
  `dim_aircraft.typecode` to analyse smoothing error by aircraft type.
- **Flight-level KPIs**: count flights per airport pair per day, typecode
  utilisation, etc., without re-reading raw files.
- **History on aircraft identity**: when a monthly Zenodo load changes the
  `registration` for an `icao24`, both old and new rows live in
  `sat_aircraft_identity` and the mart carries the latest.

## 8. What the vault explicitly does NOT do

- Replace the reconstruction medallion. The in-memory clean → features →
  reconstruct path remains the pipeline's main artery.
- Provide SCD2 effective/expiry date columns (satellites carry only `load_dt`;
  queries pick the latest). Easy to extend later.
- Load into Postgres. The vault lives as parquet on disk. A Postgres sink
  (hubs/sats/marts as tables) is a one-round follow-up if needed for Grafana.

---

*See `etl4/vault/` for the implementation; `etl4/vault/run_vault.py` is the
entry point.*
