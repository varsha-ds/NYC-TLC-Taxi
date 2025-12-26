# src/nyc_taxi/ingestion/yellow.py

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

# We validate case-insensitively by lowercasing df.columns first
REQUIRED_COLS_LOWER = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "pulocationid",
    "dolocationid",
    "trip_distance",
    "fare_amount",
    "total_amount",
]

# Tuning knobs
REQUEST_TIMEOUT = 60
DOWNLOAD_CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB
HASH_CHUNK_SIZE = 8 * 1024 * 1024      # 8 MB
SQL_CHUNK_SIZE = 100_000               # pandas.to_sql chunk size


def parquet_url(year: int, month: int) -> str:
    return f"{BASE_URL}/yellow_tripdata_{year:04d}-{month:02d}.parquet"


def parquet_path(year: int, month: int) -> Path:
    return Path("data/raw/yellow") / f"yellow_tripdata_{year:04d}-{month:02d}.parquet"


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(HASH_CHUNK_SIZE)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def download(url: str, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if out_path.exists() and out_path.stat().st_size > 0:
        # already downloaded
        return

    with requests.get(url, stream=True, timeout=REQUEST_TIMEOUT) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))

        with open(out_path, "wb") as f, tqdm(
            total=total,
            unit="B",
            unit_scale=True,
            desc=f"Downloading {out_path.name}",
        ) as pbar:
            for chunk in r.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                if chunk:
                    f.write(chunk)
                    pbar.update(len(chunk))


def ensure_bootstrap() -> None:
    """Create raw schema + ingestion_state table once (safe to rerun)."""
    ddl = """
    CREATE SCHEMA IF NOT EXISTS raw;

    CREATE TABLE IF NOT EXISTS raw.ingestion_state (
        dataset         TEXT NOT NULL,
        year            INTEGER NOT NULL,
        month           INTEGER NOT NULL,
        file_path       TEXT,
        file_sha256     TEXT,
        status          TEXT NOT NULL, -- running|success|failed
        error_message   TEXT,
        row_count       BIGINT,
        started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
        finished_at     TIMESTAMPTZ,
        PRIMARY KEY (dataset, year, month)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def validate_required_columns(df: pd.DataFrame) -> None:
    df_cols = set(df.columns)
    missing = [c for c in REQUIRED_COLS_LOWER if c not in df_cols]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def table_exists() -> bool:
    with engine.begin() as conn:
        return bool(conn.execute(text("SELECT to_regclass('raw.yellow_trips') IS NOT NULL;")).scalar())


def create_table_from_df_schema(df: pd.DataFrame) -> None:
    """
    Create raw.yellow_trips using df schema (no rows).
    This avoids schema drift issues (new columns like cbd_congestion_fee).
    """
    # create an empty table with correct columns
    df.head(0).to_sql(
        "yellow_trips",
        engine,
        schema="raw",
        if_exists="replace",
        index=False,
    )


def ensure_indexes() -> None:
    ddl = """
    CREATE INDEX IF NOT EXISTS idx_yellow_ingest_month ON raw.yellow_trips(ingest_month);
    CREATE INDEX IF NOT EXISTS idx_yellow_pickup ON raw.yellow_trips(tpep_pickup_datetime);
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def already_success(year: int, month: int, file_sha: str) -> bool:
    q = text("""
        SELECT status, file_sha256
        FROM raw.ingestion_state
        WHERE dataset=:dataset AND year=:year AND month=:month
    """)
    with engine.begin() as conn:
        row = conn.execute(q, {"dataset": DATASET, "year": year, "month": month}).fetchone()
    return bool(row and row[0] == "success" and row[1] == file_sha)


def mark_running(year: int, month: int, file_path: str, file_sha: str) -> None:
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
            "file_sha": file_sha,
        })


def mark_success(year: int, month: int, row_count: int) -> None:
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
            "row_count": row_count,
        })


def mark_failed(year: int, month: int, err: str) -> None:
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
            "err": err[:2000],
        })

import io

def _copy_from_csv_buffer(table_fqn: str, df: pd.DataFrame) -> None:
    """
    Fast bulk insert into Postgres using COPY FROM STDIN.
    Requires psycopg2 driver (already used by SQLAlchemy engine).
    """
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False)
    buf.seek(0)

    raw_conn = engine.raw_connection()
    try:
        cur = raw_conn.cursor()
        cols = ",".join([f'"{c}"' for c in df.columns])
        cur.copy_expert(
            f"COPY {table_fqn} ({cols}) FROM STDIN WITH (FORMAT CSV)",
            buf,
        )
        raw_conn.commit()
    finally:
        raw_conn.close()

def load_idempotent(year: int, month: int) -> None:
    ensure_bootstrap()

    url = parquet_url(year, month)
    path = parquet_path(year, month)

    print(f"Download/check file: {path.name}")
    download(url, path)

    file_sha = sha256_file(path)

    # If this exact file was already loaded successfully, skip
    if already_success(year, month, file_sha):
        print(f"SKIP: {DATASET} {year}-{month:02d} already ingested (sha match).")
        return

    mark_running(year, month, str(path), file_sha)

    ingest_month = pd.Timestamp(year=year, month=month, day=1).date()

    try:
        print(f"Reading parquet: {path.name}")
        df = pd.read_parquet(path)

        # Normalize column names (so required columns are stable)
        df.columns = df.columns.str.lower()

        validate_required_columns(df)

        # Add partition column for idempotent reload
        df["ingest_month"] = ingest_month.strftime("%Y-%m-%d")

        # Create table from df schema if missing
        if not table_exists():
            print("Creating raw.yellow_trips from parquet schema...")
            create_table_from_df_schema(df)

        # Idempotent month reload: delete then insert
        print(f"Deleting existing rows for ingest_month={ingest_month} ...")
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM raw.yellow_trips WHERE ingest_month=:m"), {"m": ingest_month.strftime("%Y-%m-%d")})

        print(f"Inserting rows: {len(df):,} (chunking process)")
       
        # COPY in chunks (fast). Chunking avoids huge memory spikes.
        table_fqn = "raw.yellow_trips"
        chunk_size = 200_000  # tune: 100k–300k works well locally

        total = len(df)
        for start in range(0, total, chunk_size):
            end = min(start + chunk_size, total)
            chunk = df.iloc[start:end]
            _copy_from_csv_buffer(table_fqn, chunk)
            print(f"COPY loaded rows {start:,}-{end:,} / {total:,}")


        ensure_indexes()

        with engine.begin() as conn:
            conn.execute(text("ANALYZE raw.yellow_trips;"))

        mark_success(year, month, int(len(df)))
        print(f"✓ Ingested {len(df):,} rows for {year}-{month:02d}")

    except Exception as e:
        mark_failed(year, month, str(e))
        print(f"✗ Failed ingest {year}-{month:02d}: {e}")
        raise


def load_batch(start_year: int, start_month: int, end_year: int, end_month: int) -> None:
    y, m = start_year, start_month
    while (y, m) <= (end_year, end_month):
        load_idempotent(y, m)
        m += 1
        if m > 12:
            m = 1
            y += 1
