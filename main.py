import os
import argparse
from datetime import datetime
from dotenv import load_dotenv

from src.extract import extract_vehicle_sales_data
from src.transform import identify_and_remove_duplicated_data
from src.load_data_to_s3 import df_to_s3


def run_standard_pipeline(dbname, host, port, user, password, aws_access_key_id, aws_secret_access_key):
    """Run the standard ETL pipeline."""
    start_time = datetime.now()

    print("\n🚗 Extracting and transforming vehicle sales + service data...")
    vehicle_sales_df = extract_vehicle_sales_data(dbname, host, port, user, password)
    print("✅ Extraction complete")

    print("\n🧹 Removing duplicated rows...")
    vehicle_sales_deduped = identify_and_remove_duplicated_data(vehicle_sales_df)
    print("✅ Deduplication complete")

    print("\n☁️ Uploading cleaned data to S3...")
    s3_bucket = 'cognition-devin'
    key = 'auto_oem/etl/vehicle_sales_deduped.csv'

    df_to_s3(vehicle_sales_deduped, key, s3_bucket, aws_access_key_id, aws_secret_access_key)
    print("✅ Data successfully uploaded to S3")

    execution_time = datetime.now() - start_time
    print(f"\n⏱️ Total execution time: {execution_time}")


def run_benchmark(dbname, host, port, user, password, aws_access_key_id, aws_secret_access_key,
                  iterations=5, implementations=None, skip_s3_upload=False):
    """Run the benchmark framework to compare implementations."""
    from src.benchmark.runner import BenchmarkRunner
    from src.benchmark.reporter import BenchmarkReporter

    print("\n" + "=" * 60)
    print("ETL PIPELINE PERFORMANCE BENCHMARK")
    print("=" * 60)
    print(f"Iterations per implementation: {iterations}")
    print(f"Implementations: {implementations or ['pandas', 'pyarrow', 'polars']}")
    print(f"Skip S3 upload: {skip_s3_upload}")

    runner = BenchmarkRunner()

    s3_bucket = 'cognition-devin'
    s3_key = 'auto_oem/etl/benchmark/vehicle_sales'

    results = runner.run_benchmark(
        dbname=dbname,
        host=host,
        port=port,
        user=user,
        password=password,
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        iterations=iterations,
        implementations=implementations,
        skip_s3_upload=skip_s3_upload
    )

    reporter = BenchmarkReporter(results)
    report = reporter.generate_full_report()
    print(report)

    print("\n" + "=" * 60)
    print("BENCHMARK WINNERS")
    print("=" * 60)
    print(f"  Fastest (total time): {reporter.get_winner('total_time')}")
    print(f"  Highest throughput:   {reporter.get_winner('throughput')}")
    print(f"  Lowest memory usage:  {reporter.get_winner('memory')}")
    print("=" * 60)

    return results


def main():
    load_dotenv()

    dbname = os.getenv("DB_NAME")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    aws_access_key_id = os.getenv("aws_access_key_id")
    aws_secret_access_key = os.getenv("aws_secret_access_key_id")

    parser = argparse.ArgumentParser(
        description="ETL Pipeline for vehicle sales data with optional benchmarking"
    )
    parser.add_argument(
        "--benchmark",
        action="store_true",
        help="Run benchmark mode to compare Pandas, PyArrow, and Polars implementations"
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=5,
        help="Number of iterations per implementation in benchmark mode (default: 5)"
    )
    parser.add_argument(
        "--implementations",
        type=str,
        nargs="+",
        choices=["pandas", "pyarrow", "polars"],
        help="Specific implementations to benchmark (default: all)"
    )
    parser.add_argument(
        "--skip-s3-upload",
        action="store_true",
        help="Skip actual S3 upload in benchmark mode (for testing)"
    )

    args = parser.parse_args()

    if args.benchmark:
        run_benchmark(
            dbname=dbname,
            host=host,
            port=port,
            user=user,
            password=password,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            iterations=args.iterations,
            implementations=args.implementations,
            skip_s3_upload=args.skip_s3_upload
        )
    else:
        run_standard_pipeline(
            dbname=dbname,
            host=host,
            port=port,
            user=user,
            password=password,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )


if __name__ == "__main__":
    main()
