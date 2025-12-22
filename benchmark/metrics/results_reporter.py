"""
Results reporter for benchmark operations.

This module generates comparison reports showing:
- Timing comparison table (execution time for each implementation at each data size)
- Memory comparison table (memory used in MB for each implementation at each data size)
"""

from typing import Optional

from benchmark.metrics.performance_tracker import BenchmarkResults, PerformanceMetrics


def format_data_size(size: int) -> str:
    """Format data size for display (e.g., 1000 -> '1K')."""
    if size >= 1000000:
        return f"{size // 1000000}M"
    elif size >= 1000:
        return f"{size // 1000}K"
    return str(size)


def generate_timing_table(
    results: BenchmarkResults,
    operation: Optional[str] = None
) -> str:
    """
    Generate a timing comparison table.

    Parameters:
    - results: BenchmarkResults object containing all benchmark results
    - operation: Optional filter for specific operation (extract, transform)

    Returns:
    - Formatted string table showing execution times
    """
    implementations = ['pandas', 'pyarrow', 'polars']
    data_sizes = sorted(set(r.data_size for r in results.results))

    header = f"{'Data Size':<12}" + "".join(f"{impl:<15}" for impl in implementations)
    separator = "-" * len(header)

    rows = [
        "Timing Comparison (seconds)",
        separator,
        header,
        separator
    ]

    for size in data_sizes:
        row = f"{format_data_size(size):<12}"
        for impl in implementations:
            matching = [
                r for r in results.results
                if r.implementation == impl
                and r.data_size == size
                and (operation is None or r.operation == operation)
            ]
            if matching:
                total_time = sum(r.execution_time for r in matching)
                row += f"{total_time:<15.4f}"
            else:
                row += f"{'N/A':<15}"
        rows.append(row)

    rows.append(separator)
    return "\n".join(rows)


def generate_memory_table(
    results: BenchmarkResults,
    operation: Optional[str] = None
) -> str:
    """
    Generate a memory comparison table.

    Parameters:
    - results: BenchmarkResults object containing all benchmark results
    - operation: Optional filter for specific operation (extract, transform)

    Returns:
    - Formatted string table showing memory usage in MB
    """
    implementations = ['pandas', 'pyarrow', 'polars']
    data_sizes = sorted(set(r.data_size for r in results.results))

    header = f"{'Data Size':<12}" + "".join(f"{impl:<15}" for impl in implementations)
    separator = "-" * len(header)

    rows = [
        "Memory Comparison (MB)",
        separator,
        header,
        separator
    ]

    for size in data_sizes:
        row = f"{format_data_size(size):<12}"
        for impl in implementations:
            matching = [
                r for r in results.results
                if r.implementation == impl
                and r.data_size == size
                and (operation is None or r.operation == operation)
            ]
            if matching:
                total_memory = sum(r.memory_used for r in matching) / (1024 * 1024)
                row += f"{total_memory:<15.2f}"
            else:
                row += f"{'N/A':<15}"
        rows.append(row)

    rows.append(separator)
    return "\n".join(rows)


def generate_throughput_table(
    results: BenchmarkResults,
    operation: Optional[str] = None
) -> str:
    """
    Generate a throughput comparison table.

    Parameters:
    - results: BenchmarkResults object containing all benchmark results
    - operation: Optional filter for specific operation (extract, transform)

    Returns:
    - Formatted string table showing throughput (records/second)
    """
    implementations = ['pandas', 'pyarrow', 'polars']
    data_sizes = sorted(set(r.data_size for r in results.results))

    header = f"{'Data Size':<12}" + "".join(f"{impl:<15}" for impl in implementations)
    separator = "-" * len(header)

    rows = [
        "Throughput Comparison (records/sec)",
        separator,
        header,
        separator
    ]

    for size in data_sizes:
        row = f"{format_data_size(size):<12}"
        for impl in implementations:
            matching = [
                r for r in results.results
                if r.implementation == impl
                and r.data_size == size
                and (operation is None or r.operation == operation)
            ]
            if matching:
                avg_throughput = sum(r.throughput for r in matching) / len(matching)
                row += f"{avg_throughput:<15.0f}"
            else:
                row += f"{'N/A':<15}"
        rows.append(row)

    rows.append(separator)
    return "\n".join(rows)


def generate_full_report(results: BenchmarkResults) -> str:
    """
    Generate a complete benchmark report with all comparison tables.

    Parameters:
    - results: BenchmarkResults object containing all benchmark results

    Returns:
    - Formatted string with all comparison tables
    """
    report_sections = [
        "=" * 60,
        "BENCHMARK RESULTS REPORT",
        "=" * 60,
        "",
        "OVERALL RESULTS (Extract + Transform)",
        "",
        generate_timing_table(results),
        "",
        generate_memory_table(results),
        "",
        generate_throughput_table(results),
        "",
        "=" * 60,
        "EXTRACT OPERATION ONLY",
        "=" * 60,
        "",
        generate_timing_table(results, operation='extract'),
        "",
        generate_memory_table(results, operation='extract'),
        "",
        "=" * 60,
        "TRANSFORM OPERATION ONLY",
        "=" * 60,
        "",
        generate_timing_table(results, operation='transform'),
        "",
        generate_memory_table(results, operation='transform'),
        "",
        "=" * 60,
    ]

    return "\n".join(report_sections)


def print_report(results: BenchmarkResults) -> None:
    """Print the full benchmark report to stdout."""
    print(generate_full_report(results))
