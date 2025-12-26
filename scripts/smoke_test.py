from nyc_taxi.db import engine
from sqlalchemy import text

with engine.begin() as conn:
    print(conn.execute(text("SELECT 1")).scalar())

