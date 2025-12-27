import argparse
from datetime import date
from sqlalchemy import text
from nyc_taxi.db import engine

SQL = """
DELETE FROM marts.yellow_demand_zone_hour
WHERE ingest_month = CAST(:m AS DATE);

WITH base AS (
  SELECT
    ingest_month,
    pickup_ts,
    pu_location,
    trip_distance,
    total_amount,
    EXTRACT(EPOCH FROM (dropoff_ts - pickup_ts)) / 60.0 AS trip_minutes
  FROM processed.yellow_trips
  WHERE ingest_month = CAST(:m AS DATE)
),
agg AS (
  SELECT
    ingest_month,
    date_trunc('hour', pickup_ts) AS hour_start,
    pu_location,

    COUNT(*)::int AS trip_count,

    AVG(trip_minutes) AS avg_trip_minutes,
    AVG(trip_distance) AS avg_trip_distance,
    AVG(total_amount) AS avg_total_amount
  FROM base
  WHERE pickup_ts IS NOT NULL
    AND pu_location IS NOT NULL
    AND trip_minutes IS NOT NULL
    AND trip_minutes > 0
    AND trip_minutes <= 360   -- <= 6 hours (cap; tune later)
    AND trip_distance > 0 AND trip_distance <= 100
    AND total_amount > 0 AND total_amount <= 500
  GROUP BY 1,2,3
)
INSERT INTO marts.yellow_demand_zone_hour (
  ingest_month, hour_start, pu_location,
  trip_count, avg_trip_minutes, avg_trip_distance, avg_total_amount
)
SELECT
  ingest_month, hour_start, pu_location,
  trip_count, avg_trip_minutes, avg_trip_distance, avg_total_amount
FROM agg
ON CONFLICT (ingest_month, hour_start, pu_location)
DO UPDATE SET
  trip_count = EXCLUDED.trip_count,
  avg_trip_minutes = EXCLUDED.avg_trip_minutes,
  avg_trip_distance = EXCLUDED.avg_trip_distance,
  avg_total_amount = EXCLUDED.avg_total_amount;
"""

def main(year: int, month: int):
    m = date(year, month, 1)
    with engine.begin() as conn:
        conn.execute(text(SQL), {"m": m})
        conn.execute(text("ANALYZE marts.yellow_demand_zone_hour;"))
    print("Built marts.yellow_demand_zone_hour for", m)

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--month", type=int, required=True)
    args = p.parse_args()
    main(args.year, args.month)
