"""
Performance tracker for benchmark operations.

This module tracks:
- Execution time (using time.time())
- Memory usage (using psutil.Process().memory_info().rss)
- Records processed
- Throughput (records per second)
"""

import time
from dataclasses import dataclass, field
from typing import Optional

import psutil


@dataclass
class PerformanceMetrics:
    """Container for performance metrics of a single operation."""
    implementation: str
    operation: str
    data_size: int
    execution_time: float = 0.0
    memory_before: int = 0
    memory_after: int = 0
    memory_used: int = 0
    records_processed: int = 0
    throughput: float = 0.0

    def to_dict(self) -> dict:
        """Convert metrics to dictionary."""
        return {
            'implementation': self.implementation,
            'operation': self.operation,
            'data_size': self.data_size,
            'execution_time': self.execution_time,
            'memory_before_mb': self.memory_before / (1024 * 1024),
            'memory_after_mb': self.memory_after / (1024 * 1024),
            'memory_used_mb': self.memory_used / (1024 * 1024),
            'records_processed': self.records_processed,
            'throughput': self.throughput
        }


class PerformanceTracker:
    """
    Tracks performance metrics for benchmark operations.

    Usage:
        tracker = PerformanceTracker('pandas', 'extract', 1000)
        tracker.start()
        # ... perform operation ...
        tracker.stop(records_processed=1000)
        metrics = tracker.get_metrics()
    """

    def __init__(self, implementation: str, operation: str, data_size: int):
        """
        Initialize the performance tracker.

        Parameters:
        - implementation: Name of the implementation (pandas, pyarrow, polars)
        - operation: Name of the operation (extract, transform)
        - data_size: Size of the data being processed
        """
        self.implementation = implementation
        self.operation = operation
        self.data_size = data_size
        self._start_time: Optional[float] = None
        self._end_time: Optional[float] = None
        self._memory_before: int = 0
        self._memory_after: int = 0
        self._records_processed: int = 0
        self._process = psutil.Process()

    def start(self) -> None:
        """Start tracking performance metrics."""
        self._memory_before = self._process.memory_info().rss
        self._start_time = time.time()

    def stop(self, records_processed: int = 0) -> None:
        """
        Stop tracking and calculate final metrics.

        Parameters:
        - records_processed: Number of records processed during the operation
        """
        self._end_time = time.time()
        self._memory_after = self._process.memory_info().rss
        self._records_processed = records_processed

    def get_metrics(self) -> PerformanceMetrics:
        """
        Get the collected performance metrics.

        Returns:
        - PerformanceMetrics object with all tracked metrics
        """
        if self._start_time is None or self._end_time is None:
            raise ValueError("Tracker must be started and stopped before getting metrics")

        execution_time = self._end_time - self._start_time
        memory_used = max(0, self._memory_after - self._memory_before)
        throughput = self._records_processed / execution_time if execution_time > 0 else 0

        return PerformanceMetrics(
            implementation=self.implementation,
            operation=self.operation,
            data_size=self.data_size,
            execution_time=execution_time,
            memory_before=self._memory_before,
            memory_after=self._memory_after,
            memory_used=memory_used,
            records_processed=self._records_processed,
            throughput=throughput
        )


class BenchmarkResults:
    """Container for collecting and organizing benchmark results."""

    def __init__(self):
        """Initialize the results container."""
        self.results: list[PerformanceMetrics] = []

    def add_result(self, metrics: PerformanceMetrics) -> None:
        """Add a performance metrics result."""
        self.results.append(metrics)

    def get_results_by_implementation(self, implementation: str) -> list[PerformanceMetrics]:
        """Get all results for a specific implementation."""
        return [r for r in self.results if r.implementation == implementation]

    def get_results_by_operation(self, operation: str) -> list[PerformanceMetrics]:
        """Get all results for a specific operation."""
        return [r for r in self.results if r.operation == operation]

    def get_results_by_data_size(self, data_size: int) -> list[PerformanceMetrics]:
        """Get all results for a specific data size."""
        return [r for r in self.results if r.data_size == data_size]

    def to_list(self) -> list[dict]:
        """Convert all results to a list of dictionaries."""
        return [r.to_dict() for r in self.results]
