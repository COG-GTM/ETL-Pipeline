import psycopg2
import polars as pl


def connect_to_postgres(dbname, host, port, user, password):
    """Connects to a local or remote PostgreSQL database"""
    conn = psycopg2.connect(
        dbname=dbname,
        host=host,
        port=port,
        user=user,
        password=password
    )
    print("Connected to PostgreSQL (Polars)")
    return conn


def extract_vehicle_sales_data_polars(dbname, host, port, user, password):
    """
    Extract and transform vehicle sales and service data using Polars.
    - Joins vehicles, dealerships, sales_transactions, and service_records
    - Replaces null service type/cost with defaults
    - Computes total sales revenue per transaction
    - Returns a Polars DataFrame
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

    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()

    data = {col: [row[i] for row in rows] for i, col in enumerate(columns)}

    df = pl.DataFrame(data)

    cursor.close()
    conn.close()

    print("Extracted rows (Polars):", df.height)
    return df
