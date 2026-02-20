#!/usr/bin/env python3
"""
ETL Performance Comparison Script
Compares pandas, PyArrow, and Polars for ETL operations including:
- Extract: PostgreSQL data extraction
- Transform: Duplicate removal
- Load: CSV generation (S3 upload simulation)

Measures execution time and memory usage for each step and library.
"""

import os
import time
import psutil
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
from pyarrow import csv
import psycopg2
from dotenv import load_dotenv
from io import StringIO
import gc
from memory_profiler import profile
from contextlib import contextmanager

load_dotenv()

DB_CONFIG = {
    'dbname': os.getenv("DB_NAME"),
    'host': os.getenv("DB_HOST"),
    'port': os.getenv("DB_PORT"),
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASSWORD")
}

EXTRACT_QUERY = """
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

@contextmanager
def measure_performance(operation_name, library_name):
    """Context manager to measure execution time and memory usage"""
    process = psutil.Process()
    
    gc.collect()
    
    memory_before = process.memory_info().rss / 1024 / 1024  # MB
    start_time = time.time()
    
    try:
        yield
    finally:
        end_time = time.time()
        memory_after = process.memory_info().rss / 1024 / 1024  # MB
        
        execution_time = end_time - start_time
        memory_used = memory_after - memory_before
        
        print(f"📊 {library_name} - {operation_name}:")
        print(f"   ⏱️  Execution Time: {execution_time:.4f} seconds")
        print(f"   🧠 Memory Usage: {memory_used:.2f} MB")
        print(f"   📈 Peak Memory: {memory_after:.2f} MB")
        print("-" * 50)
        
        if not hasattr(measure_performance, 'results'):
            measure_performance.results = []
        
        measure_performance.results.append({
            'library': library_name,
            'operation': operation_name,
            'execution_time': execution_time,
            'memory_used': memory_used,
            'peak_memory': memory_after
        })

def connect_to_postgres():
    """Create PostgreSQL connection"""
    return psycopg2.connect(**DB_CONFIG)


def pandas_extract():
    """Extract data using pandas"""
    with measure_performance("Extract", "Pandas"):
        conn = connect_to_postgres()
        df = pd.read_sql(EXTRACT_QUERY, conn)
        
        df['sale_date'] = pd.to_datetime(df['sale_date'], errors='coerce')
        df['service_date'] = pd.to_datetime(df['service_date'], errors='coerce')
        
        conn.close()
        print(f"🔍 Pandas extracted {df.shape[0]} rows")
        return df

def pandas_transform(df):
    """Transform data using pandas (remove duplicates)"""
    with measure_performance("Transform", "Pandas"):
        duplicate_count = df.duplicated().sum()
        if duplicate_count > 0:
            df_cleaned = df.drop_duplicates(keep='first')
            print(f"🧹 Pandas removed {duplicate_count} duplicates")
        else:
            df_cleaned = df.copy()
            print("✅ Pandas found no duplicates")
        
        return df_cleaned

def pandas_load(df):
    """Load data using pandas (simulate S3 upload with CSV generation)"""
    with measure_performance("Load", "Pandas"):
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_data = csv_buffer.getvalue()
        print(f"📤 Pandas generated CSV with {len(df)} rows ({len(csv_data)} bytes)")
        return len(csv_data)


def pyarrow_extract():
    """Extract data using PyArrow"""
    with measure_performance("Extract", "PyArrow"):
        conn = connect_to_postgres()
        
        df_pandas = pd.read_sql(EXTRACT_QUERY, conn)
        
        df_pandas['sale_date'] = pd.to_datetime(df_pandas['sale_date'], errors='coerce')
        df_pandas['service_date'] = pd.to_datetime(df_pandas['service_date'], errors='coerce')
        
        table = pa.Table.from_pandas(df_pandas)
        
        conn.close()
        print(f"🔍 PyArrow extracted {table.num_rows} rows")
        return table

def pyarrow_transform(table):
    """Transform data using PyArrow (remove duplicates)"""
    with measure_performance("Transform", "PyArrow"):
        df = table.to_pandas()
        duplicate_count = df.duplicated().sum()
        
        if duplicate_count > 0:
            df_cleaned = df.drop_duplicates(keep='first')
            table_cleaned = pa.Table.from_pandas(df_cleaned)
            print(f"🧹 PyArrow removed {duplicate_count} duplicates")
        else:
            table_cleaned = table
            print("✅ PyArrow found no duplicates")
        
        return table_cleaned

def pyarrow_load(table):
    """Load data using PyArrow (simulate S3 upload with CSV generation)"""
    with measure_performance("Load", "PyArrow"):
        df = table.to_pandas()
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_data = csv_buffer.getvalue()
        print(f"📤 PyArrow generated CSV with {table.num_rows} rows ({len(csv_data)} bytes)")
        return len(csv_data)


def polars_extract():
    """Extract data using Polars"""
    with measure_performance("Extract", "Polars"):
        conn = connect_to_postgres()
        df_pandas = pd.read_sql(EXTRACT_QUERY, conn)
        
        df_pandas['sale_date'] = pd.to_datetime(df_pandas['sale_date'], errors='coerce')
        df_pandas['service_date'] = pd.to_datetime(df_pandas['service_date'], errors='coerce')
        
        conn.close()
        
        df = pl.from_pandas(df_pandas)
        
        print(f"🔍 Polars extracted {df.height} rows")
        return df

def polars_transform(df):
    """Transform data using Polars (remove duplicates)"""
    with measure_performance("Transform", "Polars"):
        original_count = df.height
        df_cleaned = df.unique()
        duplicate_count = original_count - df_cleaned.height
        
        if duplicate_count > 0:
            print(f"🧹 Polars removed {duplicate_count} duplicates")
        else:
            print("✅ Polars found no duplicates")
        
        return df_cleaned

def polars_load(df):
    """Load data using Polars (simulate S3 upload with CSV generation)"""
    with measure_performance("Load", "Polars"):
        csv_buffer = StringIO()
        df.write_csv(csv_buffer)
        csv_data = csv_buffer.getvalue()
        print(f"📤 Polars generated CSV with {df.height} rows ({len(csv_data)} bytes)")
        return len(csv_data)


def run_etl_comparison():
    """Run ETL comparison across all three libraries"""
    print("🚀 Starting ETL Performance Comparison")
    print("=" * 60)
    
    measure_performance.results = []
    
    libraries = [
        ("Pandas", pandas_extract, pandas_transform, pandas_load),
        ("PyArrow", pyarrow_extract, pyarrow_transform, pyarrow_load),
        ("Polars", polars_extract, polars_transform, polars_load)
    ]
    
    for lib_name, extract_func, transform_func, load_func in libraries:
        print(f"\n🔧 Testing {lib_name} Implementation")
        print("-" * 40)
        
        try:
            data = extract_func()
            
            cleaned_data = transform_func(data)
            
            csv_size = load_func(cleaned_data)
            
            print(f"✅ {lib_name} ETL pipeline completed successfully")
            
        except Exception as e:
            print(f"❌ {lib_name} ETL pipeline failed: {str(e)}")
            continue
        
        gc.collect()
        time.sleep(1)  # Brief pause between tests
    
    generate_comparison_report()

def generate_comparison_report():
    """Generate a detailed comparison report"""
    if not hasattr(measure_performance, 'results') or not measure_performance.results:
        print("❌ No results to compare")
        return
    
    print("\n" + "=" * 60)
    print("📊 PERFORMANCE COMPARISON REPORT")
    print("=" * 60)
    
    operations = {}
    for result in measure_performance.results:
        op = result['operation']
        if op not in operations:
            operations[op] = []
        operations[op].append(result)
    
    for operation, results in operations.items():
        print(f"\n🔍 {operation.upper()} OPERATION COMPARISON:")
        print("-" * 40)
        
        results.sort(key=lambda x: x['execution_time'])
        
        fastest = results[0]
        print(f"🥇 Fastest: {fastest['library']} ({fastest['execution_time']:.4f}s)")
        
        for i, result in enumerate(results):
            speedup = fastest['execution_time'] / result['execution_time'] if result['execution_time'] > 0 else 1
            memory_efficiency = min(r['memory_used'] for r in results) / result['memory_used'] if result['memory_used'] > 0 else 1
            
            print(f"   {i+1}. {result['library']:<8} - "
                  f"Time: {result['execution_time']:.4f}s "
                  f"({speedup:.2f}x slower), "
                  f"Memory: {result['memory_used']:.2f}MB "
                  f"({memory_efficiency:.2f}x efficient)")
    
    print(f"\n📈 OVERALL SUMMARY:")
    print("-" * 40)
    
    library_totals = {}
    for result in measure_performance.results:
        lib = result['library']
        if lib not in library_totals:
            library_totals[lib] = {'time': 0, 'memory': 0}
        library_totals[lib]['time'] += result['execution_time']
        library_totals[lib]['memory'] += result['memory_used']
    
    sorted_libs = sorted(library_totals.items(), key=lambda x: x[1]['time'])
    
    print("Total ETL Pipeline Performance:")
    for i, (lib, totals) in enumerate(sorted_libs):
        print(f"   {i+1}. {lib:<8} - Total Time: {totals['time']:.4f}s, Total Memory: {totals['memory']:.2f}MB")
    
    winner = sorted_libs[0][0]
    print(f"\n🏆 OVERALL WINNER: {winner}")
    print(f"   Completed full ETL pipeline in {sorted_libs[0][1]['time']:.4f} seconds")
    print(f"   Used {sorted_libs[0][1]['memory']:.2f} MB of memory")

if __name__ == "__main__":
    if not all(DB_CONFIG.values()):
        print("❌ Database credentials not found in environment variables")
        print("Please ensure .env file contains: DB_NAME, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD")
        exit(1)
    
    run_etl_comparison()
