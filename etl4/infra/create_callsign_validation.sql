-- Callsign validation: compare the live-broadcast callsign in the state
-- vectors against the "expected" callsign from the Zenodo flightlist for
-- the same aircraft during the same time window.
--
-- A mismatch means the pilot broadcast a callsign that doesn't match what
-- the flightlist records for that aircraft at that time. Common causes:
-- stale callsign in the transponder, tail-number-as-callsign placeholder,
-- or flight-plan change not reflected in the broadcast.
--
-- Apply with:
--   docker exec -i etl4-postgres psql -U etl4 -d etl4 < etl4/infra/create_callsign_validation.sql

DROP MATERIALIZED VIEW IF EXISTS mv_callsign_validation;

CREATE MATERIALIZED VIEW mv_callsign_validation AS
WITH flight_windows AS (
    -- One row per (aircraft, flight-time-window) with the expected callsign
    -- and origin/destination codes brought in from the vault.
    SELECT
        lfa.hk_aircraft,
        hf.callsign                                             AS expected_callsign,
        EXTRACT(EPOCH FROM sfs.firstseen)::bigint               AS firstseen_s,
        EXTRACT(EPOCH FROM sfs.lastseen)::bigint                AS lastseen_s,
        hf.day,
        ao.icao_code                                            AS origin,
        ad.icao_code                                            AS destination
    FROM sat_flight_summary   sfs
    JOIN link_flight_aircraft lfa ON lfa.hk_flight       = sfs.hk_flight
    JOIN hub_flight           hf  ON hf.hk_flight        = sfs.hk_flight
    LEFT JOIN link_flight_route lfr ON lfr.hk_flight     = sfs.hk_flight
    LEFT JOIN hub_airport ao       ON ao.hk_airport      = lfr.hk_airport_origin
    LEFT JOIN hub_airport ad       ON ad.hk_airport      = lfr.hk_airport_destination
    WHERE sfs.firstseen IS NOT NULL AND sfs.lastseen IS NOT NULL
)
SELECT
    f.hk_aircraft,
    f.icao24,
    f.time,
    f.lat, f.lon,
    f.heading,
    f.baroaltitude,
    f.velocity,
    NULLIF(TRIM(f.callsign), '')                                AS observed_callsign,
    w.expected_callsign,
    w.origin,
    w.destination,
    w.day                                                       AS flight_day,
    CASE
      WHEN w.expected_callsign IS NULL                           THEN 'no_matching_flight'
      WHEN COALESCE(TRIM(f.callsign), '') = ''                   THEN 'no_callsign_broadcast'
      WHEN UPPER(TRIM(f.callsign)) = UPPER(TRIM(w.expected_callsign)) THEN 'match'
      ELSE                                                            'mismatch'
    END                                                         AS status
FROM fact_trajectory_enriched f
LEFT JOIN flight_windows w
       ON w.hk_aircraft = f.hk_aircraft
      AND f.time BETWEEN w.firstseen_s AND w.lastseen_s;

CREATE INDEX IF NOT EXISTS idx_mv_cv_status   ON mv_callsign_validation(status);
CREATE INDEX IF NOT EXISTS idx_mv_cv_icao24   ON mv_callsign_validation(icao24);
CREATE INDEX IF NOT EXISTS idx_mv_cv_observed ON mv_callsign_validation(observed_callsign);
CREATE INDEX IF NOT EXISTS idx_mv_cv_expected ON mv_callsign_validation(expected_callsign);
CREATE INDEX IF NOT EXISTS idx_mv_cv_time     ON mv_callsign_validation(time);
