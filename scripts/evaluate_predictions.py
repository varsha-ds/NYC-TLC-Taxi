import argparse
import pandas as pd
import numpy as np
from sqlalchemy import text

from nyc_taxi.db import engine

def main(run_ts: str):
    # IMPORTANT: use the exact run_ts as shown by your DB (no timezone)
    run_ts = pd.Timestamp(run_ts)

    sql = """
    WITH preds AS (
      SELECT run_ts, hour_start, pu_location, yhat
      FROM predictions.yellow_demand_zone_hour
      WHERE run_ts = :run_ts
    ),
    truth AS (
      SELECT hour_start, pu_location, trip_count
      FROM marts.yellow_demand_zone_hour
    )
    SELECT
      p.run_ts,
      p.hour_start,
      p.pu_location,
      p.yhat,
      t.trip_count
    FROM preds p
    JOIN truth t
      ON p.hour_start = t.hour_start
     AND p.pu_location = t.pu_location
    ORDER BY p.pu_location, p.hour_start;
    """

    df = pd.read_sql(text(sql), engine, params={"run_ts": run_ts})

    if df.empty:
        raise RuntimeError("No joined rows. Check run_ts and that truth data exists for those hours.")
    import matplotlib.pyplot as plt

    bad_zone = 79
    tmp = df[df["pu_location"] == bad_zone].copy()

    plt.figure(figsize=(14,4))
    plt.plot(tmp["hour_start"], tmp["trip_count"], label="actual")
    plt.plot(tmp["hour_start"], tmp["yhat"], label="predicted")
    plt.legend()
    plt.title(f"Zone {bad_zone}: predicted vs actual (this run)")
    plt.grid(True)
    plt.show()

    # Metrics
    y_true = df["trip_count"].astype(float)
    y_pred = df["yhat"].astype(float)

    mae = np.mean(np.abs(y_true - y_pred))
    rmse = np.sqrt(np.mean((y_true - y_pred) ** 2))

    print("Run:", run_ts)
    print("Rows:", len(df))
    print("MAE :", mae)
    print("RMSE:", rmse)

    # Per-zone MAE (top worst)
    per_zone = (
        df.assign(abs_err=(y_true - y_pred).abs())
          .groupby("pu_location")["abs_err"]
          .mean()
          .sort_values(ascending=False)
          .head(20)
    )

    print("\nWorst 20 zones by MAE:")
    print(per_zone.to_string())

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--run_ts", required=True, help="Exact run_ts as shown in Postgres, e.g. 2025-12-27 05:46:22.230944")
    args = p.parse_args()
    main(args.run_ts)
