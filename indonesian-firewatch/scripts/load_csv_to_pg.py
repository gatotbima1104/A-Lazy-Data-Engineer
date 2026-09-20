import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

# =========================
# Configuration
# =========================

POSTGRES_USER = os.getenv("POSTGRES_USER") or "postgres"
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD") or "postgres"
POSTGRES_DB = os.getenv("POSTGRES_DB") or "indonesia-firewatch"

# path = 'data/landing/fire_nrt_SV-C2_798802.csv'
path = 'data/landing/fire_archive_SV-C2_798802.csv'
# path = 'data/kaggle/province.csv'

CSV_PATH = Path(path)

POSTGRES_HOST = "localhost"
POSTGRES_PORT = 5433
TABLE_NAME = "fire_detections_archive"

# =========================
# PostgreSQL connection
# =========================

engine = create_engine(
    f"postgresql+psycopg2://"
    f"{POSTGRES_USER}:{POSTGRES_PASSWORD}@"
    f"{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)


# =========================
# Create table
# =========================

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    latitude NUMERIC,
    longitude NUMERIC,
    brightness NUMERIC,
    scan NUMERIC,
    track NUMERIC,
    acq_date DATE,
    acq_time INTEGER,
    satellite VARCHAR(50),
    instrument VARCHAR(50),
    confidence VARCHAR(50),
    version VARCHAR(50),
    bright_t31 NUMERIC,
    frp NUMERIC,
    daynight VARCHAR(10),
    type INTEGER
);
"""



print(f"Creating table: {TABLE_NAME}")

with engine.begin() as connection:
    connection.execute(text(CREATE_TABLE_SQL))

print("Table is ready.")


# =========================
# Read CSV
# =========================

print(f"Reading CSV: {CSV_PATH}")

df = pd.read_csv(CSV_PATH)

print(f"Rows read: {len(df):,}")


# =========================
# Transform data types
# =========================

# df["acq_date"] = pd.to_datetime(
#     df["acq_date"],
#     errors="coerce",
# ).dt.date

# df["acq_time"] = pd.to_numeric(
#     df["acq_time"],
#     errors="coerce",
# )


# =========================
# Insert into PostgreSQL
# =========================

print(f"Inserting into {TABLE_NAME}...")

df.to_sql(
    TABLE_NAME,
    engine,
    if_exists="replace",
    index=False,
    chunksize=5_000,
    method="multi",
)


print("Successfully loaded CSV into PostgreSQL.")


# CREATE_TABLE_SQL = f"""
# CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
#     latitude NUMERIC,
#     longitude NUMERIC,
#     brightness NUMERIC,
#     scan NUMERIC,
#     track NUMERIC,
#     acq_date DATE,
#     acq_time INTEGER,
#     satellite VARCHAR(50),
#     instrument VARCHAR(50),
#     confidence VARCHAR(50),
#     version VARCHAR(50),
#     bright_t31 NUMERIC,
#     frp NUMERIC,
#     daynight VARCHAR(10),
#     type INTEGER
# );
# """