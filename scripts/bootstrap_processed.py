from sqlalchemy import text
from nyc_taxi.db import engine

DDL = """
CREATE SCHEMA IF NOT EXISTS processed;

-- A small, stable subset of columns to start (we can add more later)
CREATE TABLE IF NOT EXISTS processed.yellow_trips (
  ingest_month DATE NOT NULL,
  pickup_ts    TIMESTAMP NOT NULL,
  dropoff_ts   TIMESTAMP NOT NULL,
  pu_location  INTEGER NOT NULL,
  do_location  INTEGER NOT NULL,
  trip_distance DOUBLE PRECISION,
  fare_amount   DOUBLE PRECISION,
  total_amount  DOUBLE PRECISION,
  PRIMARY KEY (ingest_month, pickup_ts, dropoff_ts, pu_location, do_location)
);

CREATE INDEX IF NOT EXISTS idx_processed_yellow_month ON processed.yellow_trips(ingest_month);
CREATE INDEX IF NOT EXISTS idx_processed_yellow_pickup ON processed.yellow_trips(pickup_ts);
"""
with engine.begin() as conn:
    conn.execute(text(DDL))

print("Bootstrapped processed schema/table.")
