from datetime import date
import argparse
from sqlalchemy import text
from nyc_taxi.db import engine

SQL = """
DELETE FROM processed.yellow_trips
WHERE ingest_month::date = CAST(:m AS DATE);

WITH src AS (
  SELECT
    CAST(ingest_month AS DATE) AS ingest_month_d,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    pulocationid,
    dolocationid,
    trip_distance,
    fare_amount,
    total_amount
  FROM raw.yellow_trips
  WHERE CAST(ingest_month AS DATE) = CAST(:m AS DATE)
),
clean AS (
  SELECT
    ingest_month_d AS ingest_month,

    CASE
      WHEN tpep_pickup_datetime IS NULL THEN NULL
      WHEN (tpep_pickup_datetime::text) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' THEN (tpep_pickup_datetime::timestamp)
      ELSE NULL
    END AS pickup_ts,

    CASE
      WHEN tpep_dropoff_datetime IS NULL THEN NULL
      WHEN (tpep_dropoff_datetime::text) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' THEN (tpep_dropoff_datetime::timestamp)
      ELSE NULL
    END AS dropoff_ts,

    CASE
      WHEN pulocationid IS NULL THEN NULL
      WHEN (pulocationid::text) ~ '^[0-9]+(\\.[0-9]+)?$' THEN (pulocationid::double precision)::int
      ELSE NULL
    END AS pu_location,

    CASE
      WHEN dolocationid IS NULL THEN NULL
      WHEN (dolocationid::text) ~ '^[0-9]+(\\.[0-9]+)?$' THEN (dolocationid::double precision)::int
      ELSE NULL
    END AS do_location,

    CASE
      WHEN trip_distance IS NULL THEN NULL
      WHEN (trip_distance::text) ~ '^[0-9]+(\\.[0-9]+)?$' THEN (trip_distance::double precision)
      ELSE NULL
    END AS trip_distance,

    CASE
      WHEN fare_amount IS NULL THEN NULL
      WHEN (fare_amount::text) ~ '^-?[0-9]+(\\.[0-9]+)?$' THEN (fare_amount::double precision)
      ELSE NULL
    END AS fare_amount,

    CASE
      WHEN total_amount IS NULL THEN NULL
      WHEN (total_amount::text) ~ '^-?[0-9]+(\\.[0-9]+)?$' THEN (total_amount::double precision)
      ELSE NULL
    END AS total_amount
  FROM src
)
INSERT INTO processed.yellow_trips (
  ingest_month, pickup_ts, dropoff_ts, pu_location, do_location,
  trip_distance, fare_amount, total_amount
)
SELECT
  ingest_month, pickup_ts, dropoff_ts, pu_location, do_location,
  trip_distance, fare_amount, total_amount
FROM clean
WHERE pickup_ts IS NOT NULL
  AND dropoff_ts IS NOT NULL
  AND pu_location IS NOT NULL
  AND do_location IS NOT NULL
  AND trip_distance IS NOT NULL AND trip_distance > 0
  AND total_amount IS NOT NULL AND total_amount > 0
ON CONFLICT (ingest_month, pickup_ts, dropoff_ts, pu_location, do_location)
DO NOTHING;
"""

def main(year: int, month: int):
    m = date(year, month, 1)   # <-- real date, best
    with engine.begin() as conn:
        conn.execute(text(SQL), {"m": m})
        conn.execute(text("ANALYZE processed.yellow_trips;"))
    print("Built processed.yellow_trips for", m)

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--month", type=int, required=True)
    args = p.parse_args()
    main(args.year, args.month)
