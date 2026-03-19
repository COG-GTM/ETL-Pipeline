import psycopg2
import pandas as pd


# Shared SQL query used by both extraction functions
_VEHICLE_SALES_QUERY = """
SELECT
    v.vin,
    v.model,
    v.year,
    d.name AS dealership_name,
    d.region,
    s.sale_date,
    s.sale_price,
    s.buyer_name,
    COALESCE(sr.service_date, NULL) AS service_date,
    COALESCE(sr.service_type, 'Unknown') AS service_type,
    COALESCE(sr.service_cost, 0) AS service_cost
FROM vehicles v
JOIN dealerships d ON v.dealership_id = d.id
LEFT JOIN sales_transactions s ON v.vin = s.vin
LEFT JOIN service_records sr ON v.vin = sr.vin
"""


def connect_to_postgres(dbname, host, port, user, password):
    """Connects to a local or remote PostgreSQL database"""
    conn = psycopg2.connect(
        dbname=dbname,
        host=host,
        port=port,
        user=user,
        password=password
    )
    print("✅ Connected to PostgreSQL")
    return conn


def _convert_date_columns(df):
    """Convert date columns to datetime objects."""
    df['sale_date'] = pd.to_datetime(df['sale_date'], errors='coerce')
    df['service_date'] = pd.to_datetime(df['service_date'], errors='coerce')
    return df


def extract_vehicle_sales_data(dbname, host, port, user, password):
    """
    Extract and transform vehicle sales and service data.
    - Joins vehicles, dealerships, sales_transactions, and service_records
    - Replaces null service type/cost with defaults
    - Computes total sales revenue per transaction
    - Formats dates as datetime

    Returns a single DataFrame with all rows loaded into memory.
    """
    conn = connect_to_postgres(dbname, host, port, user, password)

    df = pd.read_sql(_VEHICLE_SALES_QUERY, conn)
    df = _convert_date_columns(df)

    print("🔍 Extracted rows:", df.shape[0])
    return df


def extract_vehicle_sales_data_chunked(dbname, host, port, user, password, chunk_size=10000):
    """
    Extract vehicle sales and service data in chunks using a generator.

    Uses pd.read_sql with chunksize to yield DataFrames in batches,
    avoiding loading the entire result set into memory at once.

    Parameters:
    - dbname, host, port, user, password: PostgreSQL connection parameters
    - chunk_size: number of rows per chunk (default: 10000)

    Yields:
    - DataFrame chunks, each with up to chunk_size rows
    """
    conn = connect_to_postgres(dbname, host, port, user, password)

    chunks = pd.read_sql(_VEHICLE_SALES_QUERY, conn, chunksize=chunk_size)

    for chunk in chunks:
        chunk = _convert_date_columns(chunk)
        yield chunk

    conn.close()
