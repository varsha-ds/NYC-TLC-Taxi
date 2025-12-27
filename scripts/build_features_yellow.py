import argparse
from datetime import date
from sqlalchemy import text
from nyc_taxi.db import engine


SQL = """
DELETE FROM features.yellow_trips
WHERE ingest_month = CAST(:m AS DATE);

WITH base AS (
  SELECT
    ingest_month,
    pickup_ts,
    dropoff_ts,
    pu_location,
    do_location,
    trip_distance,
    total_amount,

    EXTRACT(EPOCH FROM (dropoff_ts - pickup_ts))::int AS trip_seconds
  FROM processed.yellow_trips
  WHERE ingest_month = CAST(:m AS DATE)
),
feat AS (
  SELECT
    ingest_month,
    pickup_ts,
    dropoff_ts,
    pu_location,
    do_location,
    trip_distance,
    total_amount,
    trip_seconds,
    (trip_seconds / 60.0) AS trip_minutes,

    CASE
      WHEN trip_seconds > 0 THEN trip_distance / (trip_seconds / 3600.0)
      ELSE NULL
    END AS speed_mph,

    EXTRACT(HOUR FROM pickup_ts)::smallint AS pickup_hour,
    EXTRACT(DOW  FROM pickup_ts)::smallint AS pickup_dow,
    (EXTRACT(DOW FROM pickup_ts) IN (0, 6)) AS is_weekend
  FROM base
  WHERE trip_seconds IS NOT NULL
)
INSERT INTO features.yellow_trips (
  ingest_month, pickup_ts, dropoff_ts, pu_location, do_location,
  trip_distance, total_amount,
  trip_seconds, trip_minutes, speed_mph,
  pickup_hour, pickup_dow, is_weekend
)
SELECT
  ingest_month, pickup_ts, dropoff_ts, pu_location, do_location,
  trip_distance, total_amount,
  trip_seconds, trip_minutes, speed_mph,
  pickup_hour, pickup_dow, is_weekend
FROM feat
WHERE
  -- Feature-layer sanity caps (tune later)
  trip_seconds > 0 AND trip_seconds <= 6 * 3600
  AND trip_distance > 0 AND trip_distance <= 100
  AND total_amount > 0 AND total_amount <= 500
  AND (speed_mph IS NULL OR speed_mph <= 80)
ON CONFLICT (ingest_month, pickup_ts, dropoff_ts, pu_location, do_location)
DO NOTHING;
"""

def main(year: int, month: int):
    m = date(year, month, 1)
    with engine.begin() as conn:
        conn.execute(text(SQL), {"m": m})
        conn.execute(text("ANALYZE features.yellow_trips;"))
    print("Built features.yellow_trips for", m)

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--month", type=int, required=True)
    args = p.parse_args()
    main(args.year, args.month)
