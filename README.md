**NYC TLC Taxi — Zone-Hour Demand Forecasting (End-to-End ML System)**

End-to-end demand forecasting pipeline for NYC TLC Yellow Taxi data. The system ingests monthly trip records, cleans and types them into Postgres, aggregates demand by pickup zone and hour, engineers time-series features (lags + rolling means), trains a model that outperforms strong baselines, generates predictions into Postgres, and evaluates backtests.

## What this system predicts

- **Target:** `trip_count` = number of trips for each `(pickup_zone, hour_start)`
- **Granularity:** zone-hour
- **Forecast horizon:** 24 hours (configurable)
- **Output table:** `predictions.yellow_demand_zone_hour`

## Architecture overview

### Layers / schemas

- **raw:** Stores ingested trip data as delivered (minimal transformation).
- **processed:** Cleans + types raw columns into a consistent schema (timestamps, numeric types, ids).
- **marts:** Business-ready aggregates.
  - `marts.yellow_demand_zone_hour` = hourly demand per pickup zone (trip_count) on a complete grid (zones x hours).
- **features:** Model-ready features per zone-hour.
  - **lags:** `lag_1h`, `lag_24h`, `lag_168h`
  - **rolling means:** `roll_6h`, `roll_24h`, `roll_168h`
  - **calendar features:** hour-of-day, day-of-week, weekend flag
- **predictions:** Forecast outputs + metadata written by inference jobs.
  - `predictions.yellow_demand_zone_hour`

### Why the pipeline is mostly SQL/Postgres (and where notebooks fit)

SQL/Postgres is used for:
- Deterministic typing/cleaning
- Scalable aggregations (millions of trips -> zone-hour demand)
- Consistent feature generation on a complete grid

Jupyter is used for:
- Model development and comparison
- Visualization and diagnostics
- Experimentation (not production execution)

This mirrors real teams: DB for repeatable transforms, notebook for modeling, scripts for production runs.

## Modeling approach

### Baselines

- **Lag-24h baseline:** `yhat = trip_count(t-24h)`
- Strong because taxi demand has strong daily seasonality.

### Attempted model: Poisson regression (why it failed here)

Poisson regression is a natural idea for count data, but in this dataset:
- Demand is extremely sparse for many zones (median often near 0)
- Per-zone behavior is heterogeneous
- One-hot zone identity creates a wide sparse feature matrix

Result: the linear model underfit and collapsed toward an intercept-only solution.

### Final model: HistGradientBoostingRegressor (Poisson loss)

We use scikit-learn's `HistGradientBoostingRegressor(loss="poisson")` because it:
- Handles non-linear interactions
- Adapts to zone-specific and calendar-specific behavior without manual feature crosses
- Performs well on sparse + high-variance zones

Offline holdout validation showed large gains vs baselines.

## Forecasting modes

1) **Replay predictions (non-recursive)**
- Predicts for a window where features already exist in the features table.
- Useful for offline benchmarking and sanity checks.

2) **Recursive forecasting (real system)**
- Predicts hour-by-hour for the next 24 hours without requiring precomputed feature rows.
- Constructs lag/rolling features from a per-zone rolling buffer (last 168 hours)
- Predicts `t+1`
- Appends the prediction into the buffer
- Repeats for `t+2` ... `t+24`

This is realistic but introduces compounding error, especially for high-volatility zones.

## Tables

### `marts.yellow_demand_zone_hour`

Truth table for evaluation:
- `hour_start` (timestamp)
- `pu_location` (int)
- `trip_count` (int)

### `features.yellow_demand_zone_hour_features`

Model-ready data:
- `hour_start`
- `pu_location`
- `trip_count`
- lag/rolling features
- calendar features

### `predictions.yellow_demand_zone_hour`

Forecast outputs:
- `run_ts`
- `model_name`
- `train_end`
- `horizon_hours`
- `hour_start`
- `pu_location`
- `yhat`

Idempotency: Predictions are treated as a forecast identity:
`(model_name, train_end, horizon_hours, hour_start, pu_location)`. Rerunning the same forecast overwrites those rows instead of creating duplicates.

## How to run (end-to-end)

### 0) Setup

- Python venv + dependencies installed
- Postgres running and configured in `nyc_taxi/db.py`

### 1) Ingest (raw)

Downloads monthly yellow taxi parquet and loads into `raw.yellow_trips` (example):

```bash
python scripts/build_raw_yellow.py --year 2025 --month 1
```

### 2) Process (processed)

Cleans/types into `processed.yellow_trips`:

```bash
python scripts/build_processed_yellow.py --year 2025 --month 1
```

### 3) Build marts (zone-hour demand)

Creates/refreshes hourly demand grid:

```bash
python scripts/build_marts_yellow_demand.py --year 2025 --month 1
```

### 4) Build features

Creates lag/rolling/calendar features:

```bash
python scripts/build_features_yellow_demand.py --year 2025 --month 1
```

### 5) Train + evaluate in notebook

Notebook:
`notebooks/demand_modeling.ipynb`

Workflow:
- Compare lag baseline vs HistGB
- Inspect zone plots and errors

### 6) Production prediction (replay / non-recursive)

Writes next-24h predictions for hours already present in features:

```bash
python scripts/predict_demand_zone_hour.py --train_end "2025-01-25 00:00:00" --horizon_hours 24
```

### 7) Production prediction (recursive / real forecast)

Writes next-24h recursive predictions without future feature rows:

```bash
python scripts/predict_demand_recursive.py --train_end "2025-01-25 00:00:00" --horizon_hours 24
```

Optionally, `--train_end` can be omitted if your script auto-detects the latest available cutoff.

### 8) Evaluate latest run automatically

```bash
python scripts/evaluate_latest_run.py
```

## Evaluation philosophy (what "good" means here)

- Baseline `lag_24h` is hard to beat.
- Non-recursive evaluation is optimistic (features already exist).
- Recursive evaluation is realistic and typically worse due to compounding error.
- Errors concentrate in a small set of high-demand/high-variance zones (airports, hubs, event zones).

## Future work (optional improvements)

- Tiered models: separate model capacity for high-demand vs sparse zones
- Guardrails: blend recursive prediction with lag baseline for volatile zones
- Horizon-wise metrics: MAE at t+1, t+6, t+12, t+24
- Monitoring: drift detection on MAE and prediction distribution
- Scheduling: cron/Airflow/Prefect for daily runs
