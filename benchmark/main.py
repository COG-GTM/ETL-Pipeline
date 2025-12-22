"""
Benchmark orchestrator for ETL pipeline performance comparison.

This module runs all three implementations (Pandas, PyArrow, Polars) against
the same test data and collects performance metrics for comparison.
"""

from typing import Callable

import pandas as pd

from benchmark.data.test_data_generator import generate_test_data_with_duplicates
from benchmark.implementations import pandas_impl, pyarrow_impl, polars_impl
from benchmark.metrics.performance_tracker import (
    BenchmarkResults,
    PerformanceTracker,
)
from benchmark.metrics.results_reporter import print_report


class BenchmarkRunner:
    """
    Runs benchmark tests for all implementations.

    Tests at multiple data sizes: 1000, 10000, and 100000 records.
    Collects performance metrics for each run.
    """

    DEFAULT_DATA_SIZES = [1000, 10000, 100000]

    def __init__(self, data_sizes: list[int] = None):
        """
        Initialize the benchmark runner.

        Parameters:
        - data_sizes: List of data sizes to test (default: [1000, 10000, 100000])
        """
        self.data_sizes = data_sizes or self.DEFAULT_DATA_SIZES
        self.results = BenchmarkResults()
        self.implementations = {
            'pandas': pandas_impl,
            'pyarrow': pyarrow_impl,
            'polars': polars_impl,
        }

    def _run_extract_benchmark(
        self,
        impl_name: str,
        impl_module,
        test_data: pd.DataFrame,
        data_size: int
    ) -> None:
        """Run extract benchmark for a single implementation."""
        tracker = PerformanceTracker(impl_name, 'extract', data_size)

        tracker.start()
        result = impl_module.extract_data(test_data)
        tracker.stop(records_processed=len(result))

        self.results.add_result(tracker.get_metrics())

    def _run_transform_benchmark(
        self,
        impl_name: str,
        impl_module,
        test_data: pd.DataFrame,
        data_size: int
    ) -> None:
        """Run transform benchmark for a single implementation."""
        tracker = PerformanceTracker(impl_name, 'transform', data_size)

        tracker.start()
        result = impl_module.transform_data(test_data)
        tracker.stop(records_processed=len(result))

        self.results.add_result(tracker.get_metrics())

    def run_benchmarks(self, verbose: bool = True) -> BenchmarkResults:
        """
        Run all benchmarks for all implementations and data sizes.

        Parameters:
        - verbose: If True, print progress messages

        Returns:
        - BenchmarkResults object containing all collected metrics
        """
        for data_size in self.data_sizes:
            if verbose:
                print(f"\n{'='*60}")
                print(f"Running benchmarks for data size: {data_size:,} records")
                print(f"{'='*60}")

            test_data = generate_test_data_with_duplicates(
                num_records=data_size,
                duplicate_ratio=0.1,
                seed=42
            )

            if verbose:
                print(f"Generated test data with {len(test_data):,} records")
                print(f"  - Unique records: ~{int(data_size * 0.9):,}")
                print(f"  - Duplicate records: ~{int(data_size * 0.1):,}")

            for impl_name, impl_module in self.implementations.items():
                if verbose:
                    print(f"\n  Testing {impl_name}...")

                try:
                    self._run_extract_benchmark(
                        impl_name, impl_module, test_data.copy(), data_size
                    )
                    if verbose:
                        print(f"    - Extract: completed")
                except Exception as e:
                    if verbose:
                        print(f"    - Extract: FAILED ({e})")

                try:
                    self._run_transform_benchmark(
                        impl_name, impl_module, test_data.copy(), data_size
                    )
                    if verbose:
                        print(f"    - Transform: completed")
                except Exception as e:
                    if verbose:
                        print(f"    - Transform: FAILED ({e})")

        return self.results

    def print_results(self) -> None:
        """Print the benchmark results report."""
        print_report(self.results)


def run_benchmark(
    data_sizes: list[int] = None,
    verbose: bool = True
) -> BenchmarkResults:
    """
    Convenience function to run the full benchmark suite.

    Parameters:
    - data_sizes: List of data sizes to test (default: [1000, 10000, 100000])
    - verbose: If True, print progress messages

    Returns:
    - BenchmarkResults object containing all collected metrics
    """
    runner = BenchmarkRunner(data_sizes=data_sizes)
    results = runner.run_benchmarks(verbose=verbose)

    if verbose:
        print("\n")
        runner.print_results()

    return results


if __name__ == "__main__":
    run_benchmark()
