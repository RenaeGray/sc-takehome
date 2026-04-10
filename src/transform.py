import duckdb
import os
import shutil

PARQUET_PATH = "data/processed/flights.parquet"

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