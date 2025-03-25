CREATE TABLE eurusd_s10 (
  timestamp TIMESTAMPTZ NOT NULL,
  open DOUBLE PRECISION,
  high DOUBLE PRECISION,
  low DOUBLE PRECISION,
  close DOUBLE PRECISION,
  volume DOUBLE PRECISION,
  PRIMARY KEY (timestamp)
);

SELECT create_hypertable('eurusd_s10', 'timestamp', if_not_exists => TRUE);
