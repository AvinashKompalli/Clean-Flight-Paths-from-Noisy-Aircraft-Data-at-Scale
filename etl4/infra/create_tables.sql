-- Pipeline run registry and evaluation results.
-- Apply with: docker exec -i etl4-postgres psql -U etl4 -d etl4 < infra/create_tables.sql

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id       SERIAL PRIMARY KEY,
    run_date     DATE,
    source       TEXT,            -- 'live' | 's3' | 'synthetic'
    method       TEXT,            -- 'time_gap' | 'kinematic' | 'kinematic_smoothing'
    noise        TEXT,            -- 'N0' | 'N1' | 'N2' | 'N3'
    rows_in      BIGINT,
    rows_out     BIGINT,
    wall_secs    DOUBLE PRECISION,
    cpu_secs     DOUBLE PRECISION,
    shuffle_mb   DOUBLE PRECISION,
    started_at   TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS eval_results (
    run_id       INTEGER REFERENCES pipeline_runs(run_id) ON DELETE CASCADE,
    metric       TEXT,
    value        DOUBLE PRECISION,
    recorded_at  TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS deviation_events (
    event_id       SERIAL PRIMARY KEY,
    run_id         INTEGER REFERENCES pipeline_runs(run_id) ON DELETE CASCADE,
    icao24         TEXT,
    flight_id      TEXT,
    start_time     BIGINT,
    end_time       BIGINT,
    type           TEXT,          -- 'cross_track' | 'altitude' | 'residual'
    max_distance   DOUBLE PRECISION,
    peak_residual  DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_runs_method    ON pipeline_runs(method);
CREATE INDEX IF NOT EXISTS idx_runs_source    ON pipeline_runs(source);
CREATE INDEX IF NOT EXISTS idx_eval_metric    ON eval_results(metric);
CREATE INDEX IF NOT EXISTS idx_dev_icao       ON deviation_events(icao24);
