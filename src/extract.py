import psycopg2
import pandas as pd


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


def extract_vehicle_sales_data(dbname, host, port, user, password):
    """
    Extract and transform vehicle sales and service data.
    - Joins vehicles, dealerships, sales_transactions
    - Aggregates service records to avoid duplicate rows
    - Replaces null service type/cost with defaults
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
        MAX(sr.service_date) AS latest_service_date,
        COUNT(sr.id) AS service_count,
        SUM(COALESCE(sr.service_cost, 0)) AS total_service_cost
    FROM vehicles v
    JOIN dealerships d ON v.dealership_id = d.id
    LEFT JOIN sales_transactions s ON v.vin = s.vin
    LEFT JOIN service_records sr ON v.vin = sr.vin
    GROUP BY v.vin, v.model, v.year, d.name, d.region, s.sale_date, s.sale_price, s.buyer_name
    """

    df = pd.read_sql(query, conn)
    
    conn.close()

    # Convert dates to datetime objects
    df['sale_date'] = pd.to_datetime(df['sale_date'], errors='coerce')
    df['latest_service_date'] = pd.to_datetime(df['latest_service_date'], errors='coerce')

    print("🔍 Extracted rows:", df.shape[0])
    return df
