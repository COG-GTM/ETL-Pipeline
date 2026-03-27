import statistics
from typing import Dict, List, Optional
from dataclasses import dataclass

from src.benchmark.metrics import BenchmarkResult


@dataclass
class AggregatedMetrics:
    """Aggregated metrics across multiple benchmark iterations."""
    implementation_name: str
    iterations: int

    extract_time_mean: float = 0.0
    extract_time_std: float = 0.0
    transform_time_mean: float = 0.0
    transform_time_std: float = 0.0
    load_time_mean: float = 0.0
    load_time_std: float = 0.0
    total_time_mean: float = 0.0
    total_time_std: float = 0.0

    memory_peak_mean: float = 0.0
    memory_peak_std: float = 0.0

    throughput_mean: float = 0.0
    throughput_std: float = 0.0

    total_rows: int = 0
    output_size_bytes: int = 0
    error_count: int = 0


class BenchmarkReporter:
    """Generates reports and analysis from benchmark results."""

    def __init__(self, results: List[BenchmarkResult]):
        self.results = results
        self.aggregated: Dict[str, AggregatedMetrics] = {}
        self._aggregate_results()

    def _safe_mean(self, values: List[float]) -> float:
        """Calculate mean, returning 0 if empty."""
        return statistics.mean(values) if values else 0.0

    def _safe_stdev(self, values: List[float]) -> float:
        """Calculate standard deviation, returning 0 if less than 2 values."""
        return statistics.stdev(values) if len(values) >= 2 else 0.0

    def _aggregate_results(self):
        """Aggregate results by implementation."""
        grouped: Dict[str, List[BenchmarkResult]] = {}
        for result in self.results:
            if result.implementation_name not in grouped:
                grouped[result.implementation_name] = []
            grouped[result.implementation_name].append(result)

        for impl_name, impl_results in grouped.items():
            successful_results = [r for r in impl_results if r.error is None]

            extract_times = [r.extract_metrics.execution_time_seconds
                           for r in successful_results if r.extract_metrics]
            transform_times = [r.transform_metrics.execution_time_seconds
                             for r in successful_results if r.transform_metrics]
            load_times = [r.load_metrics.execution_time_seconds
                        for r in successful_results if r.load_metrics]
            total_times = [r.total_execution_time_seconds for r in successful_results]

            memory_peaks = [r.total_memory_peak_mb for r in successful_results]
            throughputs = [r.total_throughput_rows_per_second for r in successful_results]

            last_successful = successful_results[-1] if successful_results else None

            self.aggregated[impl_name] = AggregatedMetrics(
                implementation_name=impl_name,
                iterations=len(impl_results),
                extract_time_mean=self._safe_mean(extract_times),
                extract_time_std=self._safe_stdev(extract_times),
                transform_time_mean=self._safe_mean(transform_times),
                transform_time_std=self._safe_stdev(transform_times),
                load_time_mean=self._safe_mean(load_times),
                load_time_std=self._safe_stdev(load_times),
                total_time_mean=self._safe_mean(total_times),
                total_time_std=self._safe_stdev(total_times),
                memory_peak_mean=self._safe_mean(memory_peaks),
                memory_peak_std=self._safe_stdev(memory_peaks),
                throughput_mean=self._safe_mean(throughputs),
                throughput_std=self._safe_stdev(throughputs),
                total_rows=last_successful.total_rows if last_successful else 0,
                output_size_bytes=(last_successful.load_metrics.output_size_bytes
                                  if last_successful and last_successful.load_metrics else 0),
                error_count=len(impl_results) - len(successful_results)
            )

    def _calculate_confidence_interval(self, mean: float, std: float, n: int, confidence: float = 0.95) -> tuple:
        """Calculate confidence interval using t-distribution approximation."""
        if n < 2 or std == 0:
            return (mean, mean)

        t_values = {0.90: 1.833, 0.95: 2.262, 0.99: 3.250}
        t_value = t_values.get(confidence, 2.262)

        margin = t_value * (std / (n ** 0.5))
        return (mean - margin, mean + margin)

    def generate_summary_table(self) -> str:
        """Generate a summary comparison table."""
        lines = []
        lines.append("\n" + "=" * 100)
        lines.append("BENCHMARK SUMMARY")
        lines.append("=" * 100)

        header = f"{'Implementation':<15} {'Extract (s)':<15} {'Transform (s)':<15} {'Load (s)':<15} {'Total (s)':<15} {'Throughput':<15}"
        lines.append(header)
        lines.append("-" * 100)

        for impl_name in sorted(self.aggregated.keys()):
            metrics = self.aggregated[impl_name]
            row = (
                f"{impl_name:<15} "
                f"{metrics.extract_time_mean:>6.4f} +/- {metrics.extract_time_std:<6.4f} "
                f"{metrics.transform_time_mean:>6.4f} +/- {metrics.transform_time_std:<6.4f} "
                f"{metrics.load_time_mean:>6.4f} +/- {metrics.load_time_std:<6.4f} "
                f"{metrics.total_time_mean:>6.4f} +/- {metrics.total_time_std:<6.4f} "
                f"{metrics.throughput_mean:>8.1f} rows/s"
            )
            lines.append(row)

        lines.append("=" * 100)
        return "\n".join(lines)

    def generate_detailed_report(self) -> str:
        """Generate a detailed report with all metrics."""
        lines = []
        lines.append("\n" + "=" * 80)
        lines.append("DETAILED BENCHMARK REPORT")
        lines.append("=" * 80)

        for impl_name in sorted(self.aggregated.keys()):
            metrics = self.aggregated[impl_name]
            lines.append(f"\n{'-' * 40}")
            lines.append(f"Implementation: {impl_name.upper()}")
            lines.append(f"{'-' * 40}")
            lines.append(f"  Iterations: {metrics.iterations}")
            lines.append(f"  Errors: {metrics.error_count}")
            lines.append(f"  Rows processed: {metrics.total_rows}")
            lines.append(f"  Output size: {metrics.output_size_bytes / 1024:.2f} KB")

            lines.append("\n  Execution Time (seconds):")
            lines.append(f"    Extract:   {metrics.extract_time_mean:.4f} +/- {metrics.extract_time_std:.4f}")
            lines.append(f"    Transform: {metrics.transform_time_mean:.4f} +/- {metrics.transform_time_std:.4f}")
            lines.append(f"    Load:      {metrics.load_time_mean:.4f} +/- {metrics.load_time_std:.4f}")
            lines.append(f"    Total:     {metrics.total_time_mean:.4f} +/- {metrics.total_time_std:.4f}")

            ci_low, ci_high = self._calculate_confidence_interval(
                metrics.total_time_mean, metrics.total_time_std, metrics.iterations
            )
            lines.append(f"    95% CI:    [{ci_low:.4f}, {ci_high:.4f}]")

            lines.append("\n  Memory Usage (MB):")
            lines.append(f"    Peak:      {metrics.memory_peak_mean:.2f} +/- {metrics.memory_peak_std:.2f}")

            lines.append("\n  Throughput:")
            lines.append(f"    Mean:      {metrics.throughput_mean:.1f} rows/second")
            lines.append(f"    Std Dev:   {metrics.throughput_std:.1f} rows/second")

        lines.append("\n" + "=" * 80)
        return "\n".join(lines)

    def generate_comparison_analysis(self) -> str:
        """Generate comparative analysis between implementations."""
        lines = []
        lines.append("\n" + "=" * 80)
        lines.append("COMPARATIVE ANALYSIS")
        lines.append("=" * 80)

        if len(self.aggregated) < 2:
            lines.append("Not enough implementations to compare.")
            return "\n".join(lines)

        baseline_name = 'pandas'
        if baseline_name not in self.aggregated:
            baseline_name = list(self.aggregated.keys())[0]

        baseline = self.aggregated[baseline_name]
        lines.append(f"\nBaseline: {baseline_name}")
        lines.append("-" * 40)

        for impl_name, metrics in sorted(self.aggregated.items()):
            if impl_name == baseline_name:
                continue

            lines.append(f"\n{impl_name} vs {baseline_name}:")

            if baseline.total_time_mean > 0:
                speedup = baseline.total_time_mean / metrics.total_time_mean
                time_diff = ((baseline.total_time_mean - metrics.total_time_mean) / baseline.total_time_mean) * 100
                lines.append(f"  Total Time: {speedup:.2f}x speedup ({time_diff:+.1f}%)")

            if baseline.extract_time_mean > 0:
                extract_speedup = baseline.extract_time_mean / metrics.extract_time_mean
                lines.append(f"  Extract: {extract_speedup:.2f}x")

            if baseline.transform_time_mean > 0:
                transform_speedup = baseline.transform_time_mean / metrics.transform_time_mean
                lines.append(f"  Transform: {transform_speedup:.2f}x")

            if baseline.load_time_mean > 0:
                load_speedup = baseline.load_time_mean / metrics.load_time_mean
                lines.append(f"  Load: {load_speedup:.2f}x")

            if baseline.memory_peak_mean > 0:
                memory_ratio = metrics.memory_peak_mean / baseline.memory_peak_mean
                memory_diff = ((metrics.memory_peak_mean - baseline.memory_peak_mean) / baseline.memory_peak_mean) * 100
                lines.append(f"  Memory: {memory_ratio:.2f}x ({memory_diff:+.1f}%)")

        lines.append("\n" + "=" * 80)
        return "\n".join(lines)

    def generate_ascii_chart(self, metric: str = 'total_time') -> str:
        """Generate a simple ASCII bar chart for visualization."""
        lines = []
        lines.append("\n" + "=" * 60)
        lines.append(f"PERFORMANCE CHART: {metric.upper().replace('_', ' ')}")
        lines.append("=" * 60)

        values = {}
        for impl_name, metrics in self.aggregated.items():
            if metric == 'total_time':
                values[impl_name] = metrics.total_time_mean
            elif metric == 'extract_time':
                values[impl_name] = metrics.extract_time_mean
            elif metric == 'transform_time':
                values[impl_name] = metrics.transform_time_mean
            elif metric == 'load_time':
                values[impl_name] = metrics.load_time_mean
            elif metric == 'memory':
                values[impl_name] = metrics.memory_peak_mean
            elif metric == 'throughput':
                values[impl_name] = metrics.throughput_mean

        if not values:
            return "\n".join(lines)

        max_value = max(values.values()) if values.values() else 1
        max_bar_length = 40

        for impl_name in sorted(values.keys()):
            value = values[impl_name]
            bar_length = int((value / max_value) * max_bar_length) if max_value > 0 else 0
            bar = "#" * bar_length

            if metric in ['total_time', 'extract_time', 'transform_time', 'load_time']:
                value_str = f"{value:.4f}s"
            elif metric == 'memory':
                value_str = f"{value:.1f}MB"
            elif metric == 'throughput':
                value_str = f"{value:.0f} rows/s"
            else:
                value_str = f"{value:.4f}"

            lines.append(f"{impl_name:<12} |{bar:<40}| {value_str}")

        lines.append("=" * 60)
        return "\n".join(lines)

    def generate_full_report(self) -> str:
        """Generate a complete benchmark report."""
        sections = [
            self.generate_summary_table(),
            self.generate_detailed_report(),
            self.generate_comparison_analysis(),
            self.generate_ascii_chart('total_time'),
            self.generate_ascii_chart('memory'),
            self.generate_ascii_chart('throughput')
        ]
        return "\n".join(sections)

    def get_winner(self, metric: str = 'total_time') -> Optional[str]:
        """Determine the best performing implementation for a given metric."""
        if not self.aggregated:
            return None

        best_impl = None
        best_value = None

        for impl_name, metrics in self.aggregated.items():
            if metric == 'total_time':
                value = metrics.total_time_mean
                is_better = best_value is None or value < best_value
            elif metric == 'throughput':
                value = metrics.throughput_mean
                is_better = best_value is None or value > best_value
            elif metric == 'memory':
                value = metrics.memory_peak_mean
                is_better = best_value is None or value < best_value
            else:
                continue

            if is_better:
                best_value = value
                best_impl = impl_name

        return best_impl
