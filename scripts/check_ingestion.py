from sqlalchemy import text
from nyc_taxi.db import engine

with engine.begin() as conn:
    n = conn.execute(text("select count(*) from raw.yellow_trips")).scalar()
    s = conn.execute(text("select * from raw.ingestion_state order by started_at desc limit 5")).fetchall()
    print("raw.yellow_trips rows:", n)
    print("latest ingestion_state:", s[:2])
