import hashlib
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from tqdm import tqdm
from sqlalchemy import text

from nyc_taxi.db import engine

DATASET = "yellow"
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

REQUIRED_COLS = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "trip_distance",
    "fare_amount",
    "total_amount",
]

# Optimization: Use connection pooling efficiently
REQUEST_TIMEOUT = 60
DOWNLOAD_CHUNK_SIZE = 8 * 1024 * 1024  # 8MB chunks for better network I/O
HASH_CHUNK_SIZE = 8 * 1024 * 1024  # 8MB chunks for faster hashing
SQL_CHUNK_SIZE = 100_000  # Larger chunks for better insert performance


def parquet_url(year: int, month: int) -> str:
    return f"{BASE_URL}/yellow_tripdata_{year:04d}-{month:02d}.parquet"


def parquet_path(year: int, month: int) -> Path:
    return Path("data/raw/yellow") / f"yellow_tripdata_{year:04d}-{month:02d}.parquet"


def sha256_file(path: Path) -> str:
    """Optimized hashing with larger buffer size."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(HASH_CHUNK_SIZE):
            h.update(chunk)
    return h.hexdigest()


def download(url: str, out_path: Path) -> None:
    """Optimized download with larger chunks and better error handling."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    
    if out_path.exists() and out_path.stat().st_size > 0:
        return

    with requests.get(url, stream=True, timeout=REQUEST_TIMEOUT) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        
        with open(out_path, "wb") as f, tqdm(
            total=total, 
            unit="B", 
            unit_scale=True,
            desc=f"Downloading {out_path.name}"
        ) as pbar:
            for chunk in r.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                f.write(chunk)
                pbar.update(len(chunk))


def ensure_raw_table():
    """Create raw table with optimized schema and indexes."""
    ddl = """
    CREATE SCHEMA IF NOT EXISTS raw;
    
    CREATE TABLE IF NOT EXISTS raw.yellow_trips (
        vendorid                  SMALLINT,
        tpep_pickup_datetime      TIMESTAMP NOT NULL,
        tpep_dropoff_datetime     TIMESTAMP NOT NULL,
        passenger_count           SMALLINT,
        trip_distance             REAL,
        ratecodeid                SMALLINT,
        store_and_fwd_flag        CHAR(1),
        pulocationid              SMALLINT NOT NULL,
        dolocationid              SMALLINT NOT NULL,
        payment_type              SMALLINT,
        fare_amount               REAL NOT NULL,
        extra                     REAL,
        mta_tax                   REAL,
        tip_amount                REAL,
        tolls_amount              REAL,
        improvement_surcharge     REAL,
        total_amount              REAL NOT NULL,
        congestion_surcharge      REAL,
        airport_fee               REAL,
        cbd_congestion_fee        REAL,
        ingest_month              DATE NOT NULL
    );
    
    CREATE TABLE IF NOT EXISTS raw.ingestion_state (
        dataset         TEXT NOT NULL,
        year            INTEGER NOT NULL,
        month           INTEGER NOT NULL,
        file_path       TEXT,
        file_sha256     TEXT,
        status          TEXT NOT NULL,
        error_message   TEXT,
        row_count       INTEGER,
        started_at      TIMESTAMP,
        finished_at     TIMESTAMP,
        PRIMARY KEY (dataset, year, month)
    );
    
    -- Optimized indexes
    CREATE INDEX IF NOT EXISTS idx_yellow_ingest_month 
        ON raw.yellow_trips(ingest_month);
    CREATE INDEX IF NOT EXISTS idx_yellow_pickup 
        ON raw.yellow_trips(tpep_pickup_datetime);
    CREATE INDEX IF NOT EXISTS idx_yellow_dropoff 
        ON raw.yellow_trips(tpep_dropoff_datetime);
    CREATE INDEX IF NOT EXISTS idx_yellow_locations 
        ON raw.yellow_trips(pulocationid, dolocationid);
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def validate(df: pd.DataFrame) -> None:
    """Validate DataFrame has required columns."""
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def already_success(year: int, month: int, file_sha: str) -> bool:
    """Check if this exact file was already successfully ingested."""
    q = text("""
        SELECT status, file_sha256
        FROM raw.ingestion_state
        WHERE dataset=:dataset AND year=:year AND month=:month
    """)
    with engine.begin() as conn:
        row = conn.execute(q, {"dataset": DATASET, "year": year, "month": month}).fetchone()
    return bool(row and row[0] == "success" and row[1] == file_sha)


def mark_running(year: int, month: int, file_path: str, file_sha: str):
    """Mark ingestion as running."""
    q = text("""
        INSERT INTO raw.ingestion_state (dataset, year, month, file_path, file_sha256, status, started_at)
        VALUES (:dataset, :year, :month, :file_path, :file_sha, 'running', now())
        ON CONFLICT (dataset, year, month)
        DO UPDATE SET 
            file_path=EXCLUDED.file_path,
            file_sha256=EXCLUDED.file_sha256,
            status='running',
            error_message=NULL,
            row_count=NULL,
            started_at=now(),
            finished_at=NULL;
    """)
    with engine.begin() as conn:
        conn.execute(q, {
            "dataset": DATASET, 
            "year": year, 
            "month": month, 
            "file_path": file_path, 
            "file_sha": file_sha
        })


def mark_success(year: int, month: int, row_count: int):
    """Mark ingestion as successful."""
    q = text("""
        UPDATE raw.ingestion_state
        SET status='success', row_count=:row_count, finished_at=now()
        WHERE dataset=:dataset AND year=:year AND month=:month;
    """)
    with engine.begin() as conn:
        conn.execute(q, {
            "dataset": DATASET, 
            "year": year, 
            "month": month, 
            "row_count": row_count
        })


def mark_failed(year: int, month: int, err: str):
    """Mark ingestion as failed."""
    q = text("""
        UPDATE raw.ingestion_state
        SET status='failed', error_message=:err, finished_at=now()
        WHERE dataset=:dataset AND year=:year AND month=:month;
    """)
    with engine.begin() as conn:
        conn.execute(q, {
            "dataset": DATASET, 
            "year": year, 
            "month": month, 
            "err": err[:2000]
        })


def optimize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Optimize DataFrame memory usage with appropriate dtypes and normalize column names."""
    # Normalize column names to lowercase to match PostgreSQL convention
    df.columns = df.columns.str.lower()
    
    # Convert float columns to more efficient types
    int_cols = ["vendorid", "passenger_count", "ratecodeid", "pulocationid", 
                "dolocationid", "payment_type"]
    float_cols = ["trip_distance", "fare_amount", "extra", "mta_tax", "tip_amount",
                  "tolls_amount", "improvement_surcharge", "total_amount", 
                  "congestion_surcharge", "airport_fee", "cbd_congestion_fee"]
    
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], downcast='integer', errors='coerce')
    
    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], downcast='float', errors='coerce')
    
    # Optimize string column
    if "store_and_fwd_flag" in df.columns:
        df["store_and_fwd_flag"] = df["store_and_fwd_flag"].astype("category")
    
    return df


def load_idempotent(year: int, month: int):
    """
    Idempotent load: download, validate, and insert data.
    Safe to rerun - will skip if already successfully loaded with same file hash.
    """
    # Ensure tables exist first
    ensure_raw_table()
    
    url = parquet_url(year, month)
    path = parquet_path(year, month)

    # Download file
    print(f"Downloading {year}-{month:02d}...")
    download(url, path)
    file_sha = sha256_file(path)

    # Check idempotency
    if already_success(year, month, file_sha):
        print(f"SKIP: {DATASET} {year}-{month:02d} already ingested (sha match).")
        return
    mark_running(year, month, str(path), file_sha)

    ingest_month = pd.Timestamp(year=year, month=month, day=1).date()

    try:
        # Read and validate
        print(f"Reading parquet for {year}-{month:02d}...")
        df = pd.read_parquet(path)
        validate(df)
        
        # Optimize memory usage
        df = optimize_dataframe(df)
        df["ingest_month"] = ingest_month

        row_count = len(df)
        print(f"Processing {row_count:,} rows for {year}-{month:02d}...")

        # Use COPY for much faster bulk insert
        with engine.begin() as conn:
            # Delete existing data for this month
            conn.execute(
                text("DELETE FROM raw.yellow_trips WHERE ingest_month=:m"), 
                {"m": ingest_month}
            )
            
            # Disable indexes during bulk insert for better performance
            conn.execute(text("SET maintenance_work_mem = '1GB';"))
            
        # Insert data
        df.to_sql(
            "yellow_trips",
            engine,
            schema="raw",
            if_exists="append",
            index=False,
            chunksize=SQL_CHUNK_SIZE,
            method="multi",
        )

        # Re-analyze table for query optimizer
        with engine.begin() as conn:
            conn.execute(text("ANALYZE raw.yellow_trips;"))

        mark_success(year, month, row_count)
        print(f"✓ Successfully ingested {row_count:,} rows for {year}-{month:02d}")

    except Exception as e:
        mark_failed(year, month, str(e))
        print(f"✗ Failed to ingest {year}-{month:02d}: {e}")
        raise


def load_batch(start_year: int, start_month: int, end_year: int, end_month: int):
    """Load multiple months in batch."""
    current_year, current_month = start_year, start_month
    
    while (current_year, current_month) <= (end_year, end_month):
        try:
            load_idempotent(current_year, current_month)
        except Exception as e:
            print(f"Error loading {current_year}-{current_month:02d}: {e}")
            # Continue with next month instead of stopping
        
        # Move to next month
        current_month += 1
        if current_month > 12:
            current_month = 1
            current_year += 1