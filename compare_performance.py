import os
import time
import psutil
import tracemalloc
from dotenv import load_dotenv

from src.extract import extract_vehicle_sales_data
from src.extract_arrow import extract_vehicle_sales_data_arrow
from src.extract_polars import extract_vehicle_sales_data_polars
from src.transform import identify_and_remove_duplicated_data
from src.transform_arrow import identify_and_remove_duplicated_data_arrow
from src.transform_polars import identify_and_remove_duplicated_data_polars


def measure_performance(operation_name, func, *args, **kwargs):
    """Measure execution time and memory usage for a function call"""
    tracemalloc.start()
    start_time = time.time()
    start_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB

    result = func(*args, **kwargs)

    end_time = time.time()
    end_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    metrics = {
        'operation': operation_name,
        'execution_time': end_time - start_time,
        'memory_used': end_memory - start_memory,
        'peak_memory': peak / 1024 / 1024  # MB
    }

    print(f"\nPerformance for {operation_name}:")
    print(f"   Time: {metrics['execution_time']:.4f}s")
    print(f"   Memory: {metrics['memory_used']:.2f}MB (Peak: {metrics['peak_memory']:.2f}MB)")

    return result, metrics


def run_comparison():
    """Run performance comparison for Pandas, PyArrow, and Polars"""
    load_dotenv()

    dbname = os.getenv("DB_NAME")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")

    results = []

    print("=" * 60)
    print("PANDAS EXTRACTION AND TRANSFORMATION")
    print("=" * 60)

    pandas_df, pandas_extract_metrics = measure_performance(
        "Pandas Extraction",
        extract_vehicle_sales_data,
        dbname, host, port, user, password
    )
    results.append(pandas_extract_metrics)

    pandas_cleaned, pandas_transform_metrics = measure_performance(
        "Pandas Transformation",
        identify_and_remove_duplicated_data,
        pandas_df
    )
    results.append(pandas_transform_metrics)

    print("\n" + "=" * 60)
    print("PYARROW EXTRACTION AND TRANSFORMATION")
    print("=" * 60)

    arrow_table, arrow_extract_metrics = measure_performance(
        "PyArrow Extraction",
        extract_vehicle_sales_data_arrow,
        dbname, host, port, user, password
    )
    results.append(arrow_extract_metrics)

    arrow_cleaned, arrow_transform_metrics = measure_performance(
        "PyArrow Transformation",
        identify_and_remove_duplicated_data_arrow,
        arrow_table
    )
    results.append(arrow_transform_metrics)

    print("\n" + "=" * 60)
    print("POLARS EXTRACTION AND TRANSFORMATION")
    print("=" * 60)

    polars_df, polars_extract_metrics = measure_performance(
        "Polars Extraction",
        extract_vehicle_sales_data_polars,
        dbname, host, port, user, password
    )
    results.append(polars_extract_metrics)

    polars_cleaned, polars_transform_metrics = measure_performance(
        "Polars Transformation",
        identify_and_remove_duplicated_data_polars,
        polars_df
    )
    results.append(polars_transform_metrics)

    generate_comparison_report(results)


def generate_comparison_report(results):
    """Generate and print a comparison report from the results"""
    print("\n" + "=" * 60)
    print("PERFORMANCE COMPARISON REPORT")
    print("=" * 60)

    print("\n{:<25} {:>15} {:>15} {:>15}".format(
        "Operation", "Time (s)", "Memory (MB)", "Peak Mem (MB)"
    ))
    print("-" * 70)

    for r in results:
        print("{:<25} {:>15.4f} {:>15.2f} {:>15.2f}".format(
            r['operation'],
            r['execution_time'],
            r['memory_used'],
            r['peak_memory']
        ))

    extraction_results = [r for r in results if 'Extraction' in r['operation']]
    transform_results = [r for r in results if 'Transformation' in r['operation']]

    print("\n" + "-" * 70)
    print("EXTRACTION COMPARISON:")

    fastest_extract = min(extraction_results, key=lambda x: x['execution_time'])
    print(f"  Fastest: {fastest_extract['operation']} ({fastest_extract['execution_time']:.4f}s)")

    lowest_mem_extract = min(extraction_results, key=lambda x: x['peak_memory'])
    print(f"  Lowest Memory: {lowest_mem_extract['operation']} ({lowest_mem_extract['peak_memory']:.2f}MB)")

    print("\nTRANSFORMATION COMPARISON:")

    fastest_transform = min(transform_results, key=lambda x: x['execution_time'])
    print(f"  Fastest: {fastest_transform['operation']} ({fastest_transform['execution_time']:.4f}s)")

    lowest_mem_transform = min(transform_results, key=lambda x: x['peak_memory'])
    print(f"  Lowest Memory: {lowest_mem_transform['operation']} ({lowest_mem_transform['peak_memory']:.2f}MB)")

    print("\n" + "=" * 60)


if __name__ == "__main__":
    run_comparison()
