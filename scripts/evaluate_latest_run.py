import argparse
import pandas as pd
import numpy as np
from sqlalchemy import text
from nyc_taxi.db import engine

def get_latest_run(model_name: str | None = None) -> pd.Timestamp:
    if model_name:
        sql = """
        SELECT run_ts
        FROM predictions.yellow_demand_zone_hour
        WHERE model_name = :model_name
        GROUP BY run_ts
        ORDER BY run_ts DESC
        LIMIT 1
        """
        df = pd.read_sql(text(sql), engine, params={"model_name": model_name})
    else:
        sql = """
        SELECT run_ts
        FROM predictions.yellow_demand_zone_hour
        GROUP BY run_ts
        ORDER BY run_ts DESC
        LIMIT 1
        """
        df = pd.read_sql(text(sql), engine)

    if df.empty:
        raise RuntimeError("No runs found in predictions.yellow_demand_zone_hour.")
    return pd.Timestamp(df.loc[0, "run_ts"])


def evaluate_run(run_ts: pd.Timestamp):
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
        raise RuntimeError(
            "No joined rows for this run_ts. "
            "Either truth data is missing for those hours or run_ts filter didn't match."
        )

    y_true = df["trip_count"].astype(float).to_numpy()
    y_pred = df["yhat"].astype(float).to_numpy()

    mae = float(np.mean(np.abs(y_true - y_pred)))
    rmse = float(np.sqrt(np.mean((y_true - y_pred) ** 2)))

    # Worst zones
    per_zone = (
        pd.DataFrame({"pu_location": df["pu_location"], "abs_err": np.abs(y_true - y_pred)})
        .groupby("pu_location")["abs_err"]
        .mean()
        .sort_values(ascending=False)
        .head(20)
    )

    print("Run:", run_ts)
    print("Rows joined:", len(df))
    print("MAE :", mae)
    print("RMSE:", rmse)
    print("\nWorst 20 zones by MAE:")
    print(per_zone.to_string())


def main(model_name: str | None):
    run_ts = get_latest_run(model_name=model_name)
    evaluate_run(run_ts)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--model_name", required=False, default=None,
                   help="Optional: evaluate latest run for a specific model_name")
    args = p.parse_args()
    main(args.model_name)
