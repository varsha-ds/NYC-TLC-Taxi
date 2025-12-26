from sqlalchemy import text
from nyc_taxi.db import engine

DDL = """
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.ingestion_state (
  dataset         TEXT NOT NULL,
  year            INT  NOT NULL,
  month           INT  NOT NULL,
  file_path       TEXT,
  file_sha256     TEXT,
  status          TEXT NOT NULL, -- running|success|failed
  row_count       BIGINT,
  error_message   TEXT,
  started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at     TIMESTAMPTZ,
  PRIMARY KEY (dataset, year, month)
);
"""

def main():
    with engine.begin() as conn:
        conn.execute(text(DDL))
    print("Bootstrapped: raw schema + raw.ingestion_state")

if __name__ == "__main__":
    main()
