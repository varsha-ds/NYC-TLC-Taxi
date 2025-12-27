import argparse
from collections import deque
from dataclasses import dataclass
import numpy as np
import pandas as pd
from sqlalchemy import text

from sklearn.ensemble import HistGradientBoostingRegressor
from nyc_taxi.db import engine

MODEL_NAME = "histgb_poisson_recursive_v1"
TARGET = "trip_count"

# Features we will compute in Python from rolling state
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

WINDOW_168 = 168
WINDOW_24 = 24
WINDOW_6 = 6


def train_model(train_end_ts: pd.Timestamp) -> HistGradientBoostingRegressor:
    """
    Train a HistGB model on historical feature rows strictly before train_end.
    Uses the features table (already contains lag/roll features and zero-filled trip_count).
    """
    train_sql = f"""
    SELECT {", ".join(FEATURES)}, {TARGET}
    FROM features.yellow_demand_zone_hour_features
    WHERE hour_start < :train_end
    ORDER BY pu_location, hour_start
    """

    df_train = pd.read_sql(text(train_sql), engine, params={"train_end": train_end_ts})

    # Drop rows with missing feature values (early history)
    df_train = df_train.dropna(subset=FEATURES + [TARGET])

    if df_train.empty:
        raise RuntimeError("Training data is empty after dropping NaNs.")

    X_train = df_train[FEATURES]
    y_train = df_train[TARGET]

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


def load_history_buffers(train_end_ts: pd.Timestamp) -> tuple[list[int], dict[int, deque]]:
    """
    Load the last 168 hours of actual trip_count per zone ending at train_end-1h.
    We use the features table because it's already "complete grid" with zeros.
    """
    start_ts = train_end_ts - pd.Timedelta(hours=WINDOW_168)
    end_ts = train_end_ts - pd.Timedelta(hours=1)

    hist_sql = """
    SELECT pu_location, hour_start, trip_count
    FROM features.yellow_demand_zone_hour_features
    WHERE hour_start >= :start_ts
      AND hour_start <= :end_ts
    ORDER BY pu_location, hour_start
    """

    hist = pd.read_sql(
        text(hist_sql),
        engine,
        params={"start_ts": start_ts, "end_ts": end_ts},
    )

    if hist.empty:
        raise RuntimeError(
            "No history returned. Ensure your features table has rows for the last 168 hours before train_end."
        )

    # Determine zone set from history
    zones = sorted(hist["pu_location"].unique().astype(int).tolist())

    # Build buffer per zone
    buffers: dict[int, deque] = {}
    for z in zones:
        series = hist.loc[hist["pu_location"] == z, "trip_count"].astype(float).to_list()

        # Safety: we expect exactly 168 points per zone if the grid is complete
        if len(series) < WINDOW_168:
            # pad on the left with zeros if needed (rare, early in dataset)
            series = [0.0] * (WINDOW_168 - len(series)) + series
        elif len(series) > WINDOW_168:
            series = series[-WINDOW_168:]

        buffers[z] = deque(series, maxlen=WINDOW_168)

    return zones, buffers


def make_features_for_hour(
    ts: pd.Timestamp,
    zones: list[int],
    buffers: dict[int, deque],
) -> pd.DataFrame:
    """
    Construct feature matrix for a single forecast timestamp `ts` across all zones.
    Uses per-zone rolling buffers for lag/roll; calendar features from ts.
    """
    pickup_hour = int(ts.hour)
    pickup_dow = int(ts.dayofweek)  # Monday=0
    is_weekend = pickup_dow in (5, 6)

    rows = []
    for z in zones:
        b = buffers[z]  # deque length 168
        # All features must use values strictly BEFORE ts, which b represents
        lag_1h = b[-1]
        lag_24h = b[-24]
        lag_168h = b[-168]

        # Rolling means up to ts-1h
        roll_6h = float(np.mean(list(b)[-WINDOW_6:]))
        roll_24h = float(np.mean(list(b)[-WINDOW_24:]))
        roll_168h = float(np.mean(list(b)))  # all 168

        rows.append(
            {
                "lag_1h": lag_1h,
                "lag_24h": lag_24h,
                "lag_168h": lag_168h,
                "roll_6h": roll_6h,
                "roll_24h": roll_24h,
                "roll_168h": roll_168h,
                "pickup_hour": pickup_hour,
                "pickup_dow": pickup_dow,
                "is_weekend": is_weekend,
                "pu_location": z,
            }
        )

    return pd.DataFrame(rows, columns=FEATURES)


def write_predictions(out: pd.DataFrame):
    insert_sql = """
        INSERT INTO predictions.yellow_demand_zone_hour (
        run_ts,
        model_name,
        train_end,
        horizon_hours,
        hour_start,
        pu_location,
        yhat
        )
        VALUES (
        :run_ts,
        :model_name,
        :train_end,
        :horizon_hours,
        :hour_start,
        :pu_location,
        :yhat
        )
        ON CONFLICT (model_name, train_end, horizon_hours, hour_start, pu_location)
        DO UPDATE SET
        yhat = EXCLUDED.yhat,
        run_ts = EXCLUDED.run_ts;
        """


    with engine.begin() as conn:
        conn.execute(text(insert_sql), out.to_dict(orient="records"))
        conn.execute(text("ANALYZE predictions.yellow_demand_zone_hour;"))

def already_ran(train_end_ts: pd.Timestamp, horizon_hours: int) -> bool:
    sql = """
    SELECT 1
    FROM predictions.yellow_demand_zone_hour
    WHERE train_end = :train_end
      AND model_name = :model_name
      AND horizon_hours = :horizon_hours
    LIMIT 1
    """
    df = pd.read_sql(text(sql), engine, params={"train_end": train_end_ts, "model_name": MODEL_NAME, "horizon_hours":horizon_hours})
    return not df.empty


def main(train_end: str | None, horizon_hours: int):
    

    train_end_ts = pd.Timestamp(train_end) if train_end else get_latest_train_end()
    # Use UTC for run timestamp (recommended)
    if already_ran(train_end_ts, horizon_hours):
        print(
            f"Skip: predictions already exist "
            f"(model={MODEL_NAME}, train_end={train_end_ts}, horizon={horizon_hours}h)"
        )
        return

    run_ts = pd.Timestamp.now(tz="UTC")

    model = train_model(train_end_ts)
    zones, buffers = load_history_buffers(train_end_ts)

    preds = []

    # Forecast recursively hour by hour
    for h in range(horizon_hours):
        ts = train_end_ts + pd.Timedelta(hours=h)

        X_t = make_features_for_hour(ts, zones, buffers)

        yhat = model.predict(X_t).astype(float)
        # Count constraints: non-negative
        yhat = np.clip(yhat, 0.0, None)

        # Save predictions
        for i, z in enumerate(zones):
            preds.append({
                "run_ts": run_ts.to_pydatetime(),
                "model_name": MODEL_NAME,
                "train_end": train_end_ts.to_pydatetime(),
                "horizon_hours": horizon_hours,
                "hour_start": ts.to_pydatetime(),
                "pu_location": int(z),
                "yhat": float(yhat[i]),
            })

        # Update buffers with the predicted values for this hour (recursive step)
        for i, z in enumerate(zones):
            buffers[z].append(float(yhat[i]))

    out = pd.DataFrame(preds)
    write_predictions(out)

    print(
        f"Wrote {len(out):,} recursive predictions to predictions.yellow_demand_zone_hour "
        f"(run_ts={run_ts}, horizon={horizon_hours}h, train_end={train_end_ts})"
    )

def get_latest_train_end() -> pd.Timestamp:
    """
    Returns the next hour after the latest hour_start present in the truth grid.
    Example: if latest hour_start is 2025-01-31 23:00, train_end is 2025-02-01 00:00.
    """
    sql = """
    SELECT MAX(hour_start) AS max_hour
    FROM features.yellow_demand_zone_hour_features
    """
    df = pd.read_sql(text(sql), engine)
    max_hour = df.loc[0, "max_hour"]
    if pd.isna(max_hour):
        raise RuntimeError("features table is empty.")
    max_hour = pd.Timestamp(max_hour)
    return max_hour + pd.Timedelta(hours=1)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--train_end", required=False, default=None,
               help="Optional. If omitted, script auto-detects latest available hour.")
    p.add_argument("--horizon_hours", type=int, default=24)
    args = p.parse_args()
    main(args.train_end, args.horizon_hours)
