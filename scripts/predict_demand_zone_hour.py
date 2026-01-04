import argparse
from datetime import datetime, timedelta
import pandas as pd

from sqlalchemy import text
from sklearn.ensemble import HistGradientBoostingRegressor

from nyc_taxi.db import engine

"""CREATE SCHEMA IF NOT EXISTS marts;

CREATE TABLE IF NOT EXISTS marts.yellow_demand_zone_hour (
  ingest_month      date NOT NULL,
  hour_start        timestamp NOT NULL,
  pu_location       integer NOT NULL,

  trip_count        integer NOT NULL,

  avg_trip_minutes  double precision,
  avg_trip_distance double precision,
  avg_total_amount  double precision,

  PRIMARY KEY (ingest_month, hour_start, pu_location)
);

CREATE INDEX IF NOT EXISTS idx_demand_hour ON marts.yellow_demand_zone_hour (hour_start);
CREATE INDEX IF NOT EXISTS idx_demand_zone ON marts.yellow_demand_zone_hour (pu_location);
"""

"""How many rows did we create?
SELECT ingest_month, COUNT(*) AS rows
FROM marts.yellow_demand_zone_hour
GROUP BY 1
ORDER BY 1;

Look at top-demand zones for a specific hour
SELECT hour_start, pu_location, trip_count
FROM marts.yellow_demand_zone_hour
WHERE ingest_month = DATE '2025-01-01'
ORDER BY trip_count DESC
LIMIT 20;

Check if you have continuous hours (sanity)
SELECT MIN(hour_start), MAX(hour_start)
FROM marts.yellow_demand_zone_hour
WHERE ingest_month = DATE '2025-01-01';

"""
MODEL_NAME = "histgb_poisson_v1"

FEATURES = [
    "lag_1h",
    "lag_24h",
    "lag_168h",
    "roll_6h",
    "roll_24h",
    "roll_168h",
    "pickup_hour",
    "pickup_dow",
    "is_weekend",
    "pu_location",
]
TARGET = "trip_count"


def train_model(df_train: pd.DataFrame) -> HistGradientBoostingRegressor:
    X_train = df_train[FEATURES].copy()
    y_train = df_train[TARGET].copy()

    model = HistGradientBoostingRegressor(
        loss="poisson",
        learning_rate=0.05,
        max_depth=8,
        max_leaf_nodes=63,
        min_samples_leaf=50,
        l2_regularization=1e-4,
        max_iter=300,
        random_state=42,
    )
    model.fit(X_train, y_train)
    return model


def main(train_end: str, horizon_hours: int):
    train_end_ts = pd.Timestamp(train_end)
    predict_end_ts = train_end_ts + pd.Timedelta(hours=horizon_hours)
    run_ts = pd.Timestamp.noe(tz="UTC")

    # 1) Pull training data (everything strictly before train_end)
    train_sql = """
    SELECT *
    FROM features.yellow_demand_zone_hour_features
    WHERE hour_start < :train_end
    ORDER BY pu_location, hour_start
    """
    df_train = pd.read_sql(text(train_sql), engine, params={"train_end": train_end_ts})

    # Drop rows with missing lags
    df_train = df_train.dropna(subset=FEATURES + [TARGET])

    if df_train.empty:
        raise RuntimeError("Training dataset is empty after filtering NaNs.")

    model = train_model(df_train)

    # 2) Pull prediction feature rows (must already exist in features table)
    pred_sql = f"""
    SELECT hour_start, {", ".join(FEATURES)}
    FROM features.yellow_demand_zone_hour_features
    WHERE hour_start >= :train_end
    AND hour_start <  :predict_end
    ORDER BY pu_location, hour_start
    """

    df_pred = pd.read_sql(
        text(pred_sql),
        engine,
        params={"train_end": train_end_ts, "predict_end": predict_end_ts},
    )

    df_pred = df_pred.dropna(subset=FEATURES)

    if df_pred.empty:
        raise RuntimeError(
            "No prediction rows found. Make sure your features table contains those hours."
        )

    X_pred = df_pred.reindex(columns=FEATURES)
    yhat = model.predict(X_pred).astype(float)


    out = pd.DataFrame(
        {
            "run_ts": run_ts,
            "hour_start": df_pred["hour_start"],
            "pu_location": df_pred["pu_location"].astype(int),
            "yhat": yhat,
            "model_name": MODEL_NAME,
            "train_end": train_end_ts,
        }
    )

    # 3) Write predictions (idempotent per run_ts)
    insert_sql = """
    INSERT INTO predictions.yellow_demand_zone_hour (
      run_ts, hour_start, pu_location, yhat, model_name, train_end
    )
    VALUES (
      :run_ts, :hour_start, :pu_location, :yhat, :model_name, :train_end
    )
    ON CONFLICT (run_ts, hour_start, pu_location)
    DO UPDATE SET
      yhat = EXCLUDED.yhat,
      model_name = EXCLUDED.model_name,
      train_end = EXCLUDED.train_end;
    """

    with engine.begin() as conn:
        conn.execute(text(insert_sql), out.to_dict(orient="records"))
        conn.execute(text("ANALYZE predictions.yellow_demand_zone_hour;"))

    print(
        f"Wrote {len(out):,} predictions to predictions.yellow_demand_zone_hour "
        f"(run_ts={run_ts}, horizon={horizon_hours}h, train_end={train_end_ts})"
    )


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--train_end", required=True, help="Timestamp like 2025-01-25 00:00:00")
    p.add_argument("--horizon_hours", type=int, default=24)
    args = p.parse_args()

    main(args.train_end, args.horizon_hours)

"""This script:

Loads feature rows needed for a prediction horizon

Trains HistGB on history up to train_end

Predicts for [train_end, train_end + 24h)

Writes predictions to predictions.yellow_demand_zone_hour

Important: this version predicts “next 24h” only when features exist (i.e., you already computed lags for those hours).
We’ll extend to true future recursive forecasting next if you want."""