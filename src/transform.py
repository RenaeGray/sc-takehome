import duckdb
import gdown
import os
import shutil

PARQUET_PATH = "data/processed/flights.parquet"
PARQUET_GDRIVE_URL = "https://drive.google.com/file/d/1Ix1akz6UjfTHkfY14r-dlRhN7JOdMWM3/view?usp=sharing" 

def download_parquet():
    if os.path.exists(PARQUET_PATH):
        return
    if not PARQUET_GDRIVE_URL:
        raise RuntimeError("PARQUET_GDRIVE_URL is not set in transform.py")
    os.makedirs(os.path.dirname(PARQUET_PATH), exist_ok=True)
    print("Downloading parquet from Google Drive...")
    gdown.download(PARQUET_GDRIVE_URL, PARQUET_PATH, fuzzy=True)

def transform_to_parquet():
    if os.path.exists(PARQUET_PATH):
        print("Parquet already exists -- skipping this step")
        return
    with duckdb.connect() as con:
        con.execute(f"""
        COPY (
            SELECT *
            FROM read_csv_auto('data/raw/**/*.csv', union_by_name=True)
            WHERE FlightDate BETWEEN '2018-01-01' AND '2025-01-31'
        )
        TO '{PARQUET_PATH}' (FORMAT PARQUET)
        """)

    print("Parquet created")

RAW_DIR = "data/raw"
RAW_GLOB = f"{RAW_DIR}/**/*.csv"

def validate_parquet():
    if not os.path.exists(PARQUET_PATH):
        print("Parquet not found - skipping validate_parquet()")
        return
    if not os.path.exists(RAW_DIR):
        print("Raw data already cleaned up")
        return
    con = duckdb.connect()

    raw_count = con.execute(f"""
        SELECT COUNT(*)
        FROM read_csv_auto('{RAW_GLOB}', union_by_name=True)
    """).fetchone()[0]
    print("Raw row count:", raw_count)

    parquet_count = con.execute(f"""
        SELECT COUNT(*)
        FROM '{PARQUET_PATH}'
    """).fetchone()[0]
    print("Parquet row count:", parquet_count)

    if raw_count == parquet_count:
        print("Counts match - deleting raw data...")
        shutil.rmtree(RAW_DIR)
        print("Raw data deleted")
    else:
        print("Counts don't match - raw data kept")

if __name__ == "__main__":
    transform_to_parquet()
    validate_parquet()