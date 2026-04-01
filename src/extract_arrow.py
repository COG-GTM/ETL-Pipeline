import psycopg2
import pyarrow as pa


def connect_to_postgres(dbname, host, port, user, password):
    """Connects to a local or remote PostgreSQL database"""
    conn = psycopg2.connect(
        dbname=dbname,
        host=host,
        port=port,
        user=user,
        password=password
    )
    print("✅ Connected to PostgreSQL (Arrow)")
    return conn


def extract_vehicle_sales_data_arrow(dbname, host, port, user, password):
    """
    Extract and transform vehicle sales and service data using PyArrow.
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

    arrays = []
    for name in column_names:
        col_data = columns[name]
        if name in ('sale_date', 'service_date'):
            arrays.append(pa.array(col_data, type=pa.date32()))
        elif name == 'year':
            arrays.append(pa.array(col_data, type=pa.int32()))
        elif name in ('sale_price', 'service_cost'):
            float_data = [float(v) if v is not None else None for v in col_data]
            arrays.append(pa.array(float_data, type=pa.float64()))
        else:
            arrays.append(pa.array(col_data, type=pa.string()))

    table = pa.table(dict(zip(column_names, arrays)))

    print("🔍 Extracted rows (Arrow):", table.num_rows)
    return table
