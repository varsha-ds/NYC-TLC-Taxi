Batch model to predict next-day taxi trip demand per pickup zone

Built an end-to-end production-grade batch ML pipeline on NYC Taxi data (3.4M+ rows/month), implementing idempotent ingestion, schema-on-read raw layer, and high-performance Postgres loading using COPY. {Designed the system to run on-demand in AWS, minimizing idle compute costs.)

Designed ingestion-state tracking, partition-based reloads, and reproducible pipelines suitable for scheduled monthly backfills and cloud orchestration.


NYC Taxi End-to-End Production ML Pipeline

• Raw parquet ingestion (3.4M+ rows/month)
• Idempotent batch pipeline
• Postgres + COPY-based loading
• Schema-on-read raw layer
• Processed + feature-ready architecture


commands - 
PYTHONPATH=src python scripts/ingest_yellow.py --year 2025 --month 1

PYTHONPATH=src python  scripts/bootstrap_processed.py
PYTHONPATH=src python scripts/build_processed_yellow.py --year 2025 --month 1

