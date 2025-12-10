"""
Test different data access patterns to identify potential performance issues.
"""
import pytest
import numpy as np
import threading
from concurrent import futures as concurrent_futures
from mef_tools import MefReader
from bnel_mef3_server.server.file_manager import FileManager
from tests.conftest import mef3_file


def forward_sequential_access(file_manager, file_path, num_chunks=10):
    """Access chunks in forward sequential order (0, 1, 2, ...)"""
    for i in range(num_chunks):
        _ = list(file_manager.get_signal_segment(file_path, i))


def backward_sequential_access(file_manager, file_path, num_chunks=10):
    """Access chunks in backward sequential order (9, 8, 7, ...)"""
    for i in range(num_chunks - 1, -1, -1):
        _ = list(file_manager.get_signal_segment(file_path, i))


def random_access(file_manager, file_path, num_chunks=10):
    """Access chunks in random order"""
    # Use a fixed seed for reproducibility, but vary the pattern
    import random
    indices = list(range(num_chunks))
    random.Random(42).shuffle(indices)
    for i in indices:
        _ = list(file_manager.get_signal_segment(file_path, i))


def back_and_forth_access(file_manager, file_path):
    """Access chunks in back-and-forth pattern (viewer-like)"""
    # Simulate a viewer paging forward then backward
    # Forward: 0, 1, 2, 3, 4
    for i in range(5):
        _ = list(file_manager.get_signal_segment(file_path, i))
    # Backward: 4, 3, 2, 1, 0
    for i in range(4, -1, -1):
        _ = list(file_manager.get_signal_segment(file_path, i))


@pytest.mark.benchmark
def test_forward_sequential_with_prefetch(benchmark, mef3_file):
    """Benchmark forward sequential access WITH prefetching."""
    fm = FileManager(n_prefetch=3, cache_capacity_multiplier=5, max_workers=4)
    fm.open_file(mef3_file)
    fm.set_signal_segment_size(mef3_file, seconds=60)
    benchmark(forward_sequential_access, fm, mef3_file, 10)
    fm.shutdown()


@pytest.mark.benchmark
def test_forward_sequential_no_prefetch(benchmark, mef3_file):
    """Benchmark forward sequential access WITHOUT prefetching."""
    fm = FileManager(n_prefetch=0, cache_capacity_multiplier=0)
    fm.open_file(mef3_file)
    fm.set_signal_segment_size(mef3_file, seconds=60)
    benchmark(forward_sequential_access, fm, mef3_file, 10)
    fm.shutdown()


@pytest.mark.benchmark
def test_backward_sequential_with_prefetch(benchmark, mef3_file):
    """Benchmark backward sequential access WITH prefetching."""
    fm = FileManager(n_prefetch=3, cache_capacity_multiplier=5, max_workers=4)
    fm.open_file(mef3_file)
    fm.set_signal_segment_size(mef3_file, seconds=60)
    benchmark(backward_sequential_access, fm, mef3_file, 10)
    fm.shutdown()


@pytest.mark.benchmark
def test_backward_sequential_no_prefetch(benchmark, mef3_file):
    """Benchmark backward sequential access WITHOUT prefetching."""
    fm = FileManager(n_prefetch=0, cache_capacity_multiplier=0)
    fm.open_file(mef3_file)
    fm.set_signal_segment_size(mef3_file, seconds=60)
    benchmark(backward_sequential_access, fm, mef3_file, 10)
    fm.shutdown()


@pytest.mark.benchmark
def test_random_access_with_prefetch(benchmark, mef3_file):
    """Benchmark random access WITH prefetching."""
    fm = FileManager(n_prefetch=3, cache_capacity_multiplier=5, max_workers=4)
    fm.open_file(mef3_file)
    fm.set_signal_segment_size(mef3_file, seconds=60)
    benchmark(random_access, fm, mef3_file, 10)
    fm.shutdown()


@pytest.mark.benchmark
def test_random_access_no_prefetch(benchmark, mef3_file):
    """Benchmark random access WITHOUT prefetching."""
    fm = FileManager(n_prefetch=0, cache_capacity_multiplier=0)
    fm.open_file(mef3_file)
    fm.set_signal_segment_size(mef3_file, seconds=60)
    benchmark(random_access, fm, mef3_file, 10)
    fm.shutdown()


@pytest.mark.benchmark
def test_back_and_forth_with_prefetch(benchmark, mef3_file):
    """Benchmark back-and-forth access (viewer-like) WITH prefetching."""
    fm = FileManager(n_prefetch=3, cache_capacity_multiplier=5, max_workers=4)
    fm.open_file(mef3_file)
    fm.set_signal_segment_size(mef3_file, seconds=60)
    benchmark(back_and_forth_access, fm, mef3_file)
    fm.shutdown()


@pytest.mark.benchmark
def test_back_and_forth_no_prefetch(benchmark, mef3_file):
    """Benchmark back-and-forth access (viewer-like) WITHOUT prefetching."""
    fm = FileManager(n_prefetch=0, cache_capacity_multiplier=0)
    fm.open_file(mef3_file)
    fm.set_signal_segment_size(mef3_file, seconds=60)
    benchmark(back_and_forth_access, fm, mef3_file)
    fm.shutdown()


# --- Direct MefReader Comparison Benchmarks ---

def direct_mef_reader_access(file_path, num_chunks=5):
    """Read data directly using MefReader (bypassing FileManager and gRPC).
    
    Note: Header reading is done once before benchmarking starts.
    """
    rdr = MefReader(file_path)
    channels = rdr.channels
    start_uutc = min(rdr.get_property('start_time'))
    end_uutc = max(rdr.get_property('end_time'))
    
    # Calculate chunk boundaries (60 second chunks)
    chunk_duration_us = 60 * 1e6
    
    for i in range(num_chunks):
        chunk_start = int(start_uutc + i * chunk_duration_us)
        chunk_end = int(min(chunk_start + chunk_duration_us, end_uutc))
        # Read data for this chunk
        data = rdr.get_data(channels, chunk_start, chunk_end)
        _ = np.array(data)


@pytest.mark.benchmark
def test_direct_mef_reader(benchmark, mef3_file):
    """Benchmark direct MefReader access (no FileManager, no gRPC)."""
    # Pre-read header outside of benchmark
    rdr = MefReader(mef3_file)
    _ = rdr.channels
    _ = rdr.get_property('start_time')
    _ = rdr.get_property('end_time')
    del rdr
    
    benchmark(direct_mef_reader_access, mef3_file, 5)


@pytest.mark.benchmark
def test_file_manager_vs_direct(benchmark, mef3_file):
    """Benchmark FileManager access (with prefetch) for comparison to direct MefReader."""
    fm = FileManager(n_prefetch=3, cache_capacity_multiplier=5, max_workers=4)
    fm.open_file(mef3_file)
    fm.set_signal_segment_size(mef3_file, seconds=60)
    
    # Use same access pattern as direct reader for fair comparison
    benchmark(forward_sequential_access, fm, mef3_file, 5)
    fm.shutdown()


# --- Concurrent Client Benchmarks ---

def concurrent_access_pattern(file_manager, file_path, num_clients, num_chunks=5):
    """Simulate multiple clients accessing data concurrently."""
    def client_work():
        for i in range(num_chunks):
            _ = list(file_manager.get_signal_segment(file_path, i))
    
    threads = []
    for _ in range(num_clients):
        thread = threading.Thread(target=client_work)
        threads.append(thread)
    
    # Start all threads
    for thread in threads:
        thread.start()
    
    # Wait for all to complete
    for thread in threads:
        thread.join()


@pytest.mark.benchmark
def test_concurrent_1_client(benchmark, mef3_file):
    """Benchmark with 1 concurrent client."""
    fm = FileManager(n_prefetch=3, cache_capacity_multiplier=5, max_workers=4)
    fm.open_file(mef3_file)
    fm.set_signal_segment_size(mef3_file, seconds=60)
    benchmark(concurrent_access_pattern, fm, mef3_file, 1, 5)
    fm.shutdown()


@pytest.mark.benchmark
def test_concurrent_2_clients(benchmark, mef3_file):
    """Benchmark with 2 concurrent clients."""
    fm = FileManager(n_prefetch=3, cache_capacity_multiplier=5, max_workers=4)
    fm.open_file(mef3_file)
    fm.set_signal_segment_size(mef3_file, seconds=60)
    benchmark(concurrent_access_pattern, fm, mef3_file, 2, 5)
    fm.shutdown()


@pytest.mark.benchmark
def test_concurrent_3_clients(benchmark, mef3_file):
    """Benchmark with 3 concurrent clients."""
    fm = FileManager(n_prefetch=3, cache_capacity_multiplier=5, max_workers=4)
    fm.open_file(mef3_file)
    fm.set_signal_segment_size(mef3_file, seconds=60)
    benchmark(concurrent_access_pattern, fm, mef3_file, 3, 5)
    fm.shutdown()


@pytest.mark.benchmark
def test_concurrent_5_clients(benchmark, mef3_file):
    """Benchmark with 5 concurrent clients."""
    fm = FileManager(n_prefetch=3, cache_capacity_multiplier=5, max_workers=4)
    fm.open_file(mef3_file)
    fm.set_signal_segment_size(mef3_file, seconds=60)
    benchmark(concurrent_access_pattern, fm, mef3_file, 5, 5)
    fm.shutdown()
