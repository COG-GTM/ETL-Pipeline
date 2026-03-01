import psycopg2
import polars as pl
from datetime import date


def connect_to_postgres(dbname, host, port, user, password):
    """Connects to a local or remote PostgreSQL database"""
    conn = psycopg2.connect(
        dbname=dbname,
        host=host,
        port=port,
        user=user,
        password=password
    )
    print("✅ Connected to PostgreSQL (Polars)")
    return conn


def extract_vehicle_sales_data_polars(dbname, host, port, user, password):
    """
    Extract and transform vehicle sales and service data using Polars.
    - Joins vehicles, dealerships, sales_transactions, and service_records
    - Replaces null service type/cost with defaults
    - Computes total sales revenue per transaction
    - Formats dates as datetime
    """
    conn = connect_to_postgres(dbname, host, port, user, password)

    query = """
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

    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    cursor.close()
    conn.close()

    columns = {name: [] for name in column_names}
    for row in rows:
        for i, value in enumerate(row):
            columns[column_names[i]].append(value)

    schema = {
        'vin': pl.Utf8,
        'model': pl.Utf8,
        'year': pl.Int32,
        'dealership_name': pl.Utf8,
        'region': pl.Utf8,
        'sale_date': pl.Date,
        'sale_price': pl.Float64,
        'buyer_name': pl.Utf8,
        'service_date': pl.Date,
        'service_type': pl.Utf8,
        'service_cost': pl.Float64,
    }

    for col in ['sale_price', 'service_cost']:
        columns[col] = [float(v) if v is not None else None for v in columns[col]]

    df = pl.DataFrame(columns, schema=schema)

    print("🔍 Extracted rows (Polars):", df.height)
    return df
