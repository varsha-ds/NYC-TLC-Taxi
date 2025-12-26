# import os
# import io
# import pandas as pd
# import pyarrow.parquet as pq
# from sqlalchemy import create_engine, text
# from dotenv import load_dotenv

# from nyc_taxi.db import SQLALCHEMY_DATABASE_URL


# ENGINE = create_engine(SQLALCHEMY_DATABASE_URL, pool_pre_ping=True)

# PARQUET_PATH = "data/raw/yellow/yellow_tripdata_2025-01.parquet"
# TABLE = "raw.yellow_trips"
# INGEST_MONTH = "2025-01-01"

# def ensure_schema():
#     with ENGINE.begin() as conn:
#         conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw;"))

# def table_exists():
#     with ENGINE.begin() as conn:
#         return conn.execute(text("SELECT to_regclass(:t) IS NOT NULL;"), {"t": TABLE}).scalar()

# def create_table_from_df(df: pd.DataFrame):
#     # create table from df schema (fast, one-time)
#     df.head(0).to_sql("yellow_trips", ENGINE, schema="raw", if_exists="replace", index=False)

# def copy_dataframe(df: pd.DataFrame):
#     # COPY expects CSV from stdin
#     buf = io.StringIO()
#     df.to_csv(buf, index=False, header=False)  # CSV rows only
#     buf.seek(0)

#     # Use raw psycopg2 connection for COPY
#     raw_conn = ENGINE.raw_connection()
#     try:
#         cur = raw_conn.cursor()
#         cols = ",".join([f'"{c}"' for c in df.columns])
#         cur.copy_expert(f"COPY {TABLE} ({cols}) FROM STDIN WITH (FORMAT CSV)", buf)
#         raw_conn.commit()
#     finally:
#         raw_conn.close()

# def main():
#     ensure_schema()

#     pf = pq.ParquetFile(PARQUET_PATH)

#     # Read first batch to learn schema / create table
#     first = pf.read_row_group(0).to_pandas()
#     first["ingest_month"] = INGEST_MONTH

#     if not table_exists():
#         create_table_from_df(first)

#     # For idempotency (month partition): delete month first
#     with ENGINE.begin() as conn:
#         conn.execute(text("DELETE FROM raw.yellow_trips WHERE ingest_month = :m"), {"m": INGEST_MONTH})

#     # Load row-group by row-group (streaming)
#     for i in range(pf.num_row_groups):
#         batch = pf.read_row_group(i).to_pandas()
#         batch["ingest_month"] = INGEST_MONTH
#         copy_dataframe(batch)
#         print(f"loaded row_group {i+1}/{pf.num_row_groups} rows={len(batch):,}")

#     # indexes after load (optional)
#     with ENGINE.begin() as conn:
#         conn.execute(text("CREATE INDEX IF NOT EXISTS idx_yellow_ingest_month ON raw.yellow_trips(ingest_month);"))
#         conn.execute(text("CREATE INDEX IF NOT EXISTS idx_yellow_pickup ON raw.yellow_trips(tpep_pickup_datetime);"))

#     print("DONE")

# if __name__ == "__main__":
#     main()
