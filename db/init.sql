
CREATE TABLE IF NOT EXISTS eurusd_s10 (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION
);
SELECT create_hypertable('eurusd_s10', 'timestamp', if_not_exists => TRUE);
