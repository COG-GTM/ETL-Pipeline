import time
import psutil
import tracemalloc
from contextlib import contextmanager


@contextmanager
def measure_performance(operation_name):
    """Context manager to measure execution time and memory usage"""
    tracemalloc.start()
    start_time = time.time()
    start_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB

    yield

    end_time = time.time()
    end_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    results = {
        'operation': operation_name,
        'execution_time': end_time - start_time,
        'memory_used': end_memory - start_memory,
        'peak_memory': peak / 1024 / 1024  # MB
    }

    print(f"Performance for {operation_name}:")
    print(f"   Time: {results['execution_time']:.4f}s")
    print(f"   Memory: {results['memory_used']:.2f}MB (Peak: {results['peak_memory']:.2f}MB)")

    return results
