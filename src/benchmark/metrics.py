import time
import psutil
import os
from dataclasses import dataclass, field
from typing import Optional
from contextlib import contextmanager


@dataclass
class StageMetrics:
    """Metrics for a single ETL stage (extract, transform, or load)."""
    stage_name: str
    execution_time_seconds: float = 0.0
    memory_start_mb: float = 0.0
    memory_end_mb: float = 0.0
    memory_peak_mb: float = 0.0
    cpu_percent: float = 0.0
    rows_processed: int = 0
    output_size_bytes: int = 0

    @property
    def memory_used_mb(self) -> float:
        """Memory used during the stage."""
        return self.memory_end_mb - self.memory_start_mb

    @property
    def throughput_rows_per_second(self) -> float:
        """Data throughput in rows per second."""
        if self.execution_time_seconds > 0:
            return self.rows_processed / self.execution_time_seconds
        return 0.0


@dataclass
class BenchmarkResult:
    """Complete benchmark result for a single implementation run."""
    implementation_name: str
    iteration: int
    extract_metrics: Optional[StageMetrics] = None
    transform_metrics: Optional[StageMetrics] = None
    load_metrics: Optional[StageMetrics] = None
    total_execution_time_seconds: float = 0.0
    total_rows: int = 0
    error: Optional[str] = None

    @property
    def total_memory_peak_mb(self) -> float:
        """Peak memory usage across all stages."""
        peaks = []
        if self.extract_metrics:
            peaks.append(self.extract_metrics.memory_peak_mb)
        if self.transform_metrics:
            peaks.append(self.transform_metrics.memory_peak_mb)
        if self.load_metrics:
            peaks.append(self.load_metrics.memory_peak_mb)
        return max(peaks) if peaks else 0.0

    @property
    def total_throughput_rows_per_second(self) -> float:
        """Overall throughput in rows per second."""
        if self.total_execution_time_seconds > 0:
            return self.total_rows / self.total_execution_time_seconds
        return 0.0


class MetricsCollector:
    """Collects performance metrics during ETL operations."""

    def __init__(self):
        self.process = psutil.Process(os.getpid())
        self._start_time: float = 0.0
        self._start_memory: float = 0.0
        self._peak_memory: float = 0.0
        self._cpu_times_start = None

    def _get_memory_mb(self) -> float:
        """Get current memory usage in MB."""
        return self.process.memory_info().rss / (1024 * 1024)

    def _get_cpu_percent(self) -> float:
        """Get CPU usage percentage."""
        return self.process.cpu_percent(interval=0.1)

    @contextmanager
    def measure_stage(self, stage_name: str):
        """Context manager to measure metrics for a stage."""
        metrics = StageMetrics(stage_name=stage_name)

        metrics.memory_start_mb = self._get_memory_mb()
        self._peak_memory = metrics.memory_start_mb
        start_time = time.perf_counter()

        cpu_start = self.process.cpu_times()

        try:
            yield metrics
        finally:
            end_time = time.perf_counter()
            cpu_end = self.process.cpu_times()

            metrics.execution_time_seconds = end_time - start_time
            metrics.memory_end_mb = self._get_memory_mb()
            metrics.memory_peak_mb = max(self._peak_memory, metrics.memory_end_mb)

            cpu_time_used = (cpu_end.user - cpu_start.user) + (cpu_end.system - cpu_start.system)
            if metrics.execution_time_seconds > 0:
                metrics.cpu_percent = (cpu_time_used / metrics.execution_time_seconds) * 100

    def start_total_measurement(self):
        """Start measuring total execution time."""
        self._start_time = time.perf_counter()
        self._start_memory = self._get_memory_mb()

    def end_total_measurement(self) -> float:
        """End total measurement and return elapsed time."""
        return time.perf_counter() - self._start_time

    def create_benchmark_result(
        self,
        implementation_name: str,
        iteration: int,
        extract_metrics: Optional[StageMetrics] = None,
        transform_metrics: Optional[StageMetrics] = None,
        load_metrics: Optional[StageMetrics] = None,
        total_time: float = 0.0,
        total_rows: int = 0,
        error: Optional[str] = None
    ) -> BenchmarkResult:
        """Create a complete benchmark result."""
        return BenchmarkResult(
            implementation_name=implementation_name,
            iteration=iteration,
            extract_metrics=extract_metrics,
            transform_metrics=transform_metrics,
            load_metrics=load_metrics,
            total_execution_time_seconds=total_time,
            total_rows=total_rows,
            error=error
        )
