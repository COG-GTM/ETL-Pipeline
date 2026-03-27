import gc
from typing import Dict, List, Optional

from src.implementations.pandas_impl import PandasImpl
from src.implementations.pyarrow_impl import PyArrowImpl
from src.implementations.polars_impl import PolarsImpl
from src.benchmark.metrics import MetricsCollector, BenchmarkResult


class BenchmarkRunner:
    """Orchestrates benchmark runs across different ETL implementations."""

    def __init__(self):
        self.implementations = {
            'pandas': PandasImpl(),
            'pyarrow': PyArrowImpl(),
            'polars': PolarsImpl()
        }
        self.metrics_collector = MetricsCollector()
        self.results: List[BenchmarkResult] = []

    def run_single_benchmark(
        self,
        impl_name: str,
        iteration: int,
        dbname: str,
        host: str,
        port: str,
        user: str,
        password: str,
        s3_bucket: str,
        s3_key: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        region_name: str = 'us-west-2',
        skip_s3_upload: bool = False
    ) -> BenchmarkResult:
        """Run a single benchmark iteration for a specific implementation."""
        impl = self.implementations[impl_name]

        gc.collect()

        self.metrics_collector.start_total_measurement()

        extract_metrics = None
        transform_metrics = None
        load_metrics = None
        total_rows = 0
        error = None

        try:
            with self.metrics_collector.measure_stage('extract') as extract_metrics:
                data = impl.extract_data(dbname, host, port, user, password)
                extract_metrics.rows_processed = impl.get_row_count(data)
                total_rows = extract_metrics.rows_processed

            with self.metrics_collector.measure_stage('transform') as transform_metrics:
                transformed_data = impl.transform_data(data)
                transform_metrics.rows_processed = impl.get_row_count(transformed_data)
                total_rows = transform_metrics.rows_processed

            with self.metrics_collector.measure_stage('load') as load_metrics:
                if skip_s3_upload:
                    from io import StringIO
                    csv_buffer = StringIO()
                    if impl_name == 'pandas':
                        transformed_data.to_csv(csv_buffer, index=False)
                    elif impl_name == 'pyarrow':
                        import pyarrow.csv as pa_csv
                        from io import BytesIO
                        byte_buffer = BytesIO()
                        pa_csv.write_csv(transformed_data, byte_buffer)
                        csv_data = byte_buffer.getvalue().decode('utf-8')
                        csv_buffer.write(csv_data)
                    elif impl_name == 'polars':
                        transformed_data.write_csv(csv_buffer)
                    csv_data = csv_buffer.getvalue()
                else:
                    csv_data = impl.load_data(
                        transformed_data,
                        f"{s3_key}_{impl_name}_{iteration}.csv",
                        s3_bucket,
                        aws_access_key_id,
                        aws_secret_access_key,
                        region_name
                    )
                load_metrics.rows_processed = total_rows
                load_metrics.output_size_bytes = impl.get_csv_size(csv_data)

        except Exception as e:
            error = str(e)

        total_time = self.metrics_collector.end_total_measurement()

        result = self.metrics_collector.create_benchmark_result(
            implementation_name=impl_name,
            iteration=iteration,
            extract_metrics=extract_metrics,
            transform_metrics=transform_metrics,
            load_metrics=load_metrics,
            total_time=total_time,
            total_rows=total_rows,
            error=error
        )

        return result

    def run_benchmark(
        self,
        dbname: str,
        host: str,
        port: str,
        user: str,
        password: str,
        s3_bucket: str,
        s3_key: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        region_name: str = 'us-west-2',
        iterations: int = 5,
        implementations: Optional[List[str]] = None,
        skip_s3_upload: bool = False
    ) -> List[BenchmarkResult]:
        """
        Run benchmarks for all implementations.

        Args:
            dbname: Database name
            host: Database host
            port: Database port
            user: Database user
            password: Database password
            s3_bucket: S3 bucket name
            s3_key: S3 key prefix
            aws_access_key_id: AWS access key
            aws_secret_access_key: AWS secret key
            region_name: AWS region
            iterations: Number of iterations per implementation
            implementations: List of implementations to benchmark (default: all)
            skip_s3_upload: If True, skip actual S3 upload (for testing)

        Returns:
            List of BenchmarkResult objects
        """
        self.results = []

        if implementations is None:
            implementations = list(self.implementations.keys())

        for impl_name in implementations:
            if impl_name not in self.implementations:
                print(f"Warning: Unknown implementation '{impl_name}', skipping")
                continue

            print(f"\n{'='*60}")
            print(f"Benchmarking: {impl_name.upper()}")
            print(f"{'='*60}")

            for i in range(iterations):
                print(f"\n  Iteration {i + 1}/{iterations}...")

                result = self.run_single_benchmark(
                    impl_name=impl_name,
                    iteration=i + 1,
                    dbname=dbname,
                    host=host,
                    port=port,
                    user=user,
                    password=password,
                    s3_bucket=s3_bucket,
                    s3_key=s3_key,
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key,
                    region_name=region_name,
                    skip_s3_upload=skip_s3_upload
                )

                self.results.append(result)

                if result.error:
                    print(f"    Error: {result.error}")
                else:
                    print(f"    Total time: {result.total_execution_time_seconds:.4f}s")
                    print(f"    Rows processed: {result.total_rows}")
                    if result.extract_metrics:
                        print(f"    Extract: {result.extract_metrics.execution_time_seconds:.4f}s")
                    if result.transform_metrics:
                        print(f"    Transform: {result.transform_metrics.execution_time_seconds:.4f}s")
                    if result.load_metrics:
                        print(f"    Load: {result.load_metrics.execution_time_seconds:.4f}s")

        return self.results

    def get_results_by_implementation(self) -> Dict[str, List[BenchmarkResult]]:
        """Group results by implementation name."""
        grouped: Dict[str, List[BenchmarkResult]] = {}
        for result in self.results:
            if result.implementation_name not in grouped:
                grouped[result.implementation_name] = []
            grouped[result.implementation_name].append(result)
        return grouped
