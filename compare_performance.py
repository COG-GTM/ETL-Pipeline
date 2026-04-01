import os
import pandas as pd
from dotenv import load_dotenv

from src.extract import extract_vehicle_sales_data
from src.extract_arrow import extract_vehicle_sales_data_arrow
from src.extract_polars import extract_vehicle_sales_data_polars
from src.transform import identify_and_remove_duplicated_data
from src.transform_arrow import identify_and_remove_duplicated_data_arrow
from src.transform_polars import identify_and_remove_duplicated_data_polars
from src.performance_utils import measure_performance


def run_comparison():
    """Run performance comparison for Pandas, PyArrow, and Polars"""
    load_dotenv()

    dbname = os.getenv("DB_NAME")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")

    results = []

    print("\n" + "=" * 60)
    print("PANDAS EXTRACTION AND TRANSFORMATION")
    print("=" * 60)
    with measure_performance("Pandas Extraction") as pandas_extract_perf:
        pandas_df = extract_vehicle_sales_data(dbname, host, port, user, password)
    pandas_extract_results = {
        'library': 'Pandas',
        'operation': 'Extraction',
        'rows': len(pandas_df),
    }
    results.append(pandas_extract_results)

    with measure_performance("Pandas Transformation") as pandas_transform_perf:
        pandas_deduped = identify_and_remove_duplicated_data(pandas_df)
    pandas_transform_results = {
        'library': 'Pandas',
        'operation': 'Transformation',
        'rows': len(pandas_deduped),
    }
    results.append(pandas_transform_results)

    print("\n" + "=" * 60)
    print("PYARROW EXTRACTION AND TRANSFORMATION")
    print("=" * 60)
    with measure_performance("PyArrow Extraction") as arrow_extract_perf:
        arrow_table = extract_vehicle_sales_data_arrow(dbname, host, port, user, password)
    arrow_extract_results = {
        'library': 'PyArrow',
        'operation': 'Extraction',
        'rows': arrow_table.num_rows,
    }
    results.append(arrow_extract_results)

    with measure_performance("PyArrow Transformation") as arrow_transform_perf:
        arrow_deduped = identify_and_remove_duplicated_data_arrow(arrow_table)
    arrow_transform_results = {
        'library': 'PyArrow',
        'operation': 'Transformation',
        'rows': arrow_deduped.num_rows,
    }
    results.append(arrow_transform_results)

    print("\n" + "=" * 60)
    print("POLARS EXTRACTION AND TRANSFORMATION")
    print("=" * 60)
    with measure_performance("Polars Extraction") as polars_extract_perf:
        polars_df = extract_vehicle_sales_data_polars(dbname, host, port, user, password)
    polars_extract_results = {
        'library': 'Polars',
        'operation': 'Extraction',
        'rows': polars_df.height,
    }
    results.append(polars_extract_results)

    with measure_performance("Polars Transformation") as polars_transform_perf:
        polars_deduped = identify_and_remove_duplicated_data_polars(polars_df)
    polars_transform_results = {
        'library': 'Polars',
        'operation': 'Transformation',
        'rows': polars_deduped.height,
    }
    results.append(polars_transform_results)

    generate_comparison_report(results)


def generate_comparison_report(results):
    """Generate and print a comparison report"""
    print("\n" + "=" * 60)
    print("PERFORMANCE COMPARISON REPORT")
    print("=" * 60)

    df = pd.DataFrame(results)
    print("\n📊 Results Summary:")
    print(df.to_string(index=False))

    print("\n" + "-" * 60)
    print("DATA CONSISTENCY CHECK")
    print("-" * 60)

    extraction_rows = df[df['operation'] == 'Extraction']['rows'].tolist()
    transform_rows = df[df['operation'] == 'Transformation']['rows'].tolist()

    if len(set(extraction_rows)) == 1:
        print(f"✅ All libraries extracted the same number of rows: {extraction_rows[0]}")
    else:
        print(f"⚠️  Row count mismatch in extraction: {extraction_rows}")

    if len(set(transform_rows)) == 1:
        print(f"✅ All libraries produced the same number of rows after deduplication: {transform_rows[0]}")
    else:
        print(f"⚠️  Row count mismatch after transformation: {transform_rows}")

    print("\n" + "=" * 60)
    print("COMPARISON COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    run_comparison()
