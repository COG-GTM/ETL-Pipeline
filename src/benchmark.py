"""
Benchmark script to compare ETL performance across Pandas, PyArrow, and Polars.

This script measures:
- Extraction time from PostgreSQL
- Transformation time (deduplication)
- Memory usage for each library
"""

import os
import time
import tracemalloc
from datetime import datetime
from io import StringIO

import psycopg2
import pandas as pd
import pyarrow as pa
import pyarrow.csv as pa_csv
import polars as pl
from dotenv import load_dotenv


load_dotenv()

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "etl_db"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "mysecretpassword"),
}

QUERY = """
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


def generate_sample_data(num_vehicles=10000, num_sales_per_vehicle=2, num_services_per_vehicle=3):
    """Generate sample data for benchmarking."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    cur.execute("SELECT COUNT(*) FROM vehicles")
    existing_count = cur.fetchone()[0]
    
    if existing_count >= num_vehicles:
        print(f"Database already has {existing_count} vehicles, skipping data generation")
        cur.close()
        conn.close()
        return
    
    print(f"Generating {num_vehicles} vehicles with sales and service records...")
    
    cur.execute("SELECT id FROM dealerships")
    dealership_ids = [row[0] for row in cur.fetchall()]
    
    if not dealership_ids:
        print("No dealerships found, creating some...")
        cur.execute("""
            INSERT INTO dealerships (name, region) VALUES
            ('Test Dealer 1', 'North'),
            ('Test Dealer 2', 'South'),
            ('Test Dealer 3', 'East'),
            ('Test Dealer 4', 'West')
            RETURNING id
        """)
        dealership_ids = [row[0] for row in cur.fetchall()]
        conn.commit()
    
    models = ['Camry', 'Corolla', 'F-150', 'Civic', 'Accord', 'Mustang', 'Explorer', 'Escape', 'RAV4', 'CR-V']
    buyers = ['Alice Johnson', 'Bob Smith', 'Carlos Vega', 'Diana Prince', 'Edward Norton', 
              'Fiona Apple', 'George Lucas', 'Hannah Montana', 'Ivan Drago', 'Julia Roberts']
    service_types = ['Oil Change', 'Brake Pads', 'Transmission Flush', 'Tire Rotation', 
                     'Battery Replacement', 'Air Filter', 'Spark Plugs', 'Coolant Flush']
    
    import random
    from datetime import date, timedelta
    
    batch_size = 1000
    vehicles_to_insert = []
    sales_to_insert = []
    services_to_insert = []
    
    for i in range(num_vehicles - existing_count):
        vin = f"VIN{i:012d}"
        model = random.choice(models)
        year = random.randint(2015, 2024)
        dealership_id = random.choice(dealership_ids)
        vehicles_to_insert.append((vin, model, year, dealership_id))
        
        for _ in range(random.randint(1, num_sales_per_vehicle)):
            sale_date = date(2020, 1, 1) + timedelta(days=random.randint(0, 1500))
            sale_price = random.randint(15000, 60000)
            buyer = random.choice(buyers)
            sales_to_insert.append((vin, sale_date, sale_price, buyer))
        
        for _ in range(random.randint(0, num_services_per_vehicle)):
            service_date = date(2020, 1, 1) + timedelta(days=random.randint(0, 1500))
            service_type = random.choice(service_types)
            service_cost = random.randint(50, 2000)
            services_to_insert.append((vin, service_date, service_type, service_cost))
        
        if len(vehicles_to_insert) >= batch_size:
            cur.executemany(
                "INSERT INTO vehicles (vin, model, year, dealership_id) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
                vehicles_to_insert
            )
            cur.executemany(
                "INSERT INTO sales_transactions (vin, sale_date, sale_price, buyer_name) VALUES (%s, %s, %s, %s)",
                sales_to_insert
            )
            cur.executemany(
                "INSERT INTO service_records (vin, service_date, service_type, service_cost) VALUES (%s, %s, %s, %s)",
                services_to_insert
            )
            conn.commit()
            vehicles_to_insert = []
            sales_to_insert = []
            services_to_insert = []
            print(f"  Inserted {i + 1} vehicles...")
    
    if vehicles_to_insert:
        cur.executemany(
            "INSERT INTO vehicles (vin, model, year, dealership_id) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
            vehicles_to_insert
        )
        cur.executemany(
            "INSERT INTO sales_transactions (vin, sale_date, sale_price, buyer_name) VALUES (%s, %s, %s, %s)",
            sales_to_insert
        )
        cur.executemany(
            "INSERT INTO service_records (vin, service_date, service_type, service_cost) VALUES (%s, %s, %s, %s)",
            services_to_insert
        )
        conn.commit()
    
    cur.close()
    conn.close()
    print(f"Data generation complete!")


def add_duplicate_rows():
    """Add some duplicate rows to test deduplication."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    cur.execute("""
        INSERT INTO sales_transactions (vin, sale_date, sale_price, buyer_name)
        SELECT vin, sale_date, sale_price, buyer_name 
        FROM sales_transactions 
        LIMIT 500
        ON CONFLICT DO NOTHING
    """)
    
    cur.execute("""
        INSERT INTO service_records (vin, service_date, service_type, service_cost)
        SELECT vin, service_date, service_type, service_cost 
        FROM service_records 
        LIMIT 500
        ON CONFLICT DO NOTHING
    """)
    
    conn.commit()
    cur.close()
    conn.close()
    print("Added duplicate rows for testing deduplication")


class BenchmarkResult:
    """Store benchmark results for a single library."""
    def __init__(self, library_name):
        self.library_name = library_name
        self.extract_time = 0.0
        self.transform_time = 0.0
        self.total_time = 0.0
        self.peak_memory_mb = 0.0
        self.row_count = 0
        self.row_count_after_dedup = 0


def benchmark_pandas():
    """Benchmark extraction and transformation using Pandas."""
    result = BenchmarkResult("Pandas")
    
    tracemalloc.start()
    
    conn = psycopg2.connect(**DB_CONFIG)
    
    start_time = time.perf_counter()
    df = pd.read_sql(QUERY, conn)
    df['sale_date'] = pd.to_datetime(df['sale_date'], errors='coerce')
    df['service_date'] = pd.to_datetime(df['service_date'], errors='coerce')
    result.extract_time = time.perf_counter() - start_time
    result.row_count = len(df)
    
    start_time = time.perf_counter()
    df_deduped = df.drop_duplicates(keep='first')
    result.transform_time = time.perf_counter() - start_time
    result.row_count_after_dedup = len(df_deduped)
    
    current, peak = tracemalloc.get_traced_memory()
    result.peak_memory_mb = peak / (1024 * 1024)
    tracemalloc.stop()
    
    result.total_time = result.extract_time + result.transform_time
    
    conn.close()
    return result


def benchmark_pyarrow():
    """Benchmark extraction and transformation using PyArrow."""
    result = BenchmarkResult("PyArrow")
    
    tracemalloc.start()
    
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    start_time = time.perf_counter()
    cur.execute(QUERY)
    columns = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    
    column_data = {col: [] for col in columns}
    for row in rows:
        for i, col in enumerate(columns):
            column_data[col].append(row[i])
    
    table = pa.table(column_data)
    
    sale_date_col = pa.array([
        datetime.combine(d, datetime.min.time()) if d else None 
        for d in column_data['sale_date']
    ], type=pa.timestamp('us'))
    service_date_col = pa.array([
        datetime.combine(d, datetime.min.time()) if d else None 
        for d in column_data['service_date']
    ], type=pa.timestamp('us'))
    
    table = table.set_column(
        table.schema.get_field_index('sale_date'), 
        'sale_date', 
        sale_date_col
    )
    table = table.set_column(
        table.schema.get_field_index('service_date'), 
        'service_date', 
        service_date_col
    )
    
    result.extract_time = time.perf_counter() - start_time
    result.row_count = table.num_rows
    
    start_time = time.perf_counter()
    df_temp = table.to_pandas()
    df_deduped = df_temp.drop_duplicates(keep='first')
    table_deduped = pa.Table.from_pandas(df_deduped)
    result.transform_time = time.perf_counter() - start_time
    result.row_count_after_dedup = table_deduped.num_rows
    
    current, peak = tracemalloc.get_traced_memory()
    result.peak_memory_mb = peak / (1024 * 1024)
    tracemalloc.stop()
    
    result.total_time = result.extract_time + result.transform_time
    
    cur.close()
    conn.close()
    return result


def benchmark_polars():
    """Benchmark extraction and transformation using Polars."""
    result = BenchmarkResult("Polars")
    
    tracemalloc.start()
    
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    start_time = time.perf_counter()
    cur.execute(QUERY)
    columns = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    
    column_data = {col: [] for col in columns}
    for row in rows:
        for i, col in enumerate(columns):
            column_data[col].append(row[i])
    
    df = pl.DataFrame(column_data)
    
    df = df.with_columns([
        pl.col('sale_date').cast(pl.Date),
        pl.col('service_date').cast(pl.Date),
    ])
    
    result.extract_time = time.perf_counter() - start_time
    result.row_count = df.height
    
    start_time = time.perf_counter()
    df_deduped = df.unique()
    result.transform_time = time.perf_counter() - start_time
    result.row_count_after_dedup = df_deduped.height
    
    current, peak = tracemalloc.get_traced_memory()
    result.peak_memory_mb = peak / (1024 * 1024)
    tracemalloc.stop()
    
    result.total_time = result.extract_time + result.transform_time
    
    cur.close()
    conn.close()
    return result


def print_results(results):
    """Print benchmark results in a formatted table."""
    print("\n" + "=" * 80)
    print("BENCHMARK RESULTS")
    print("=" * 80)
    
    print(f"\n{'Library':<12} {'Extract (s)':<14} {'Transform (s)':<14} {'Total (s)':<12} {'Memory (MB)':<14} {'Rows':<10} {'After Dedup':<12}")
    print("-" * 98)
    
    for r in results:
        print(f"{r.library_name:<12} {r.extract_time:<14.4f} {r.transform_time:<14.4f} {r.total_time:<12.4f} {r.peak_memory_mb:<14.2f} {r.row_count:<10} {r.row_count_after_dedup:<12}")
    
    print("\n" + "=" * 80)
    print("PERFORMANCE COMPARISON (relative to Pandas)")
    print("=" * 80)
    
    pandas_result = next(r for r in results if r.library_name == "Pandas")
    
    print(f"\n{'Library':<12} {'Extract':<14} {'Transform':<14} {'Total':<12} {'Memory':<14}")
    print("-" * 66)
    
    for r in results:
        extract_ratio = pandas_result.extract_time / r.extract_time if r.extract_time > 0 else 0
        transform_ratio = pandas_result.transform_time / r.transform_time if r.transform_time > 0 else 0
        total_ratio = pandas_result.total_time / r.total_time if r.total_time > 0 else 0
        memory_ratio = pandas_result.peak_memory_mb / r.peak_memory_mb if r.peak_memory_mb > 0 else 0
        
        print(f"{r.library_name:<12} {extract_ratio:<14.2f}x {transform_ratio:<14.2f}x {total_ratio:<12.2f}x {memory_ratio:<14.2f}x")
    
    print("\n" + "=" * 80)


def run_benchmarks(num_iterations=3):
    """Run benchmarks multiple times and average results."""
    print(f"\nRunning benchmarks ({num_iterations} iterations each)...")
    
    all_results = {
        "Pandas": [],
        "PyArrow": [],
        "Polars": [],
    }
    
    for i in range(num_iterations):
        print(f"\n--- Iteration {i + 1}/{num_iterations} ---")
        
        print("  Running Pandas benchmark...")
        all_results["Pandas"].append(benchmark_pandas())
        
        print("  Running PyArrow benchmark...")
        all_results["PyArrow"].append(benchmark_pyarrow())
        
        print("  Running Polars benchmark...")
        all_results["Polars"].append(benchmark_polars())
    
    averaged_results = []
    for library, results in all_results.items():
        avg = BenchmarkResult(library)
        avg.extract_time = sum(r.extract_time for r in results) / len(results)
        avg.transform_time = sum(r.transform_time for r in results) / len(results)
        avg.total_time = sum(r.total_time for r in results) / len(results)
        avg.peak_memory_mb = sum(r.peak_memory_mb for r in results) / len(results)
        avg.row_count = results[0].row_count
        avg.row_count_after_dedup = results[0].row_count_after_dedup
        averaged_results.append(avg)
    
    return averaged_results


def main():
    """Main entry point for the benchmark script."""
    print("=" * 80)
    print("ETL PIPELINE BENCHMARK: Pandas vs PyArrow vs Polars")
    print("=" * 80)
    
    generate_sample_data(num_vehicles=10000)
    add_duplicate_rows()
    
    results = run_benchmarks(num_iterations=3)
    
    print_results(results)
    
    print("\nBenchmark complete!")


if __name__ == "__main__":
    main()
