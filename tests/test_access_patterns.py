"""
Benchmark tests for MEF3 server performance.
All benchmarks use the same dataset (2 hours, 64 channels, 256 Hz, precision=2)
and the same number of operations (20 chunks) for fair comparison.
"""
import pytest
import numpy as np
import threading
from mef_tools import MefReader
from bnel_mef3_server.client import Mef3Client


# Standard benchmark parameters
BENCHMARK_NUM_CHUNKS = 20  # All benchmarks use 20 chunks for fair comparison
BENCHMARK_SEGMENT_SIZE_S = 60  # 60 second segments


# --- Helper functions for access patterns ---

def grpc_sequential_forward(client, file_path, num_chunks):
    """Access chunks in forward sequential order via gRPC."""
    for i in range(num_chunks):
        _ = client.get_signal_segment(file_path, i)


def grpc_sequential_backward(client, file_path, num_chunks):
    """Access chunks in backward sequential order via gRPC."""
    for i in range(num_chunks - 1, -1, -1):
        _ = client.get_signal_segment(file_path, i)


def grpc_random_access(client, file_path, num_chunks):
    """Access chunks in random order via gRPC."""
    import random
    indices = list(range(num_chunks))
    random.Random(42).shuffle(indices)
    for i in indices:
        _ = client.get_signal_segment(file_path, i)


def grpc_viewer_pattern(client, file_path, num_chunks):
    """
    Simulate viewer usage: navigate forward a bit, then backward, then forward again.
    Total: 20 chunk accesses for consistency with other benchmarks.
    """
    # Forward: 0-9 (10 chunks)
    for i in range(10):
        _ = client.get_signal_segment(file_path, i)
    # Backward: 9-5 (5 chunks)
    for i in range(9, 4, -1):
        _ = client.get_signal_segment(file_path, i)
    # Forward: 5-9 (5 chunks)
    for i in range(5, 10):
        _ = client.get_signal_segment(file_path, i)


def grpc_concurrent_access(client, file_path, num_clients, chunks_per_client):
    """Simulate multiple concurrent clients accessing data via gRPC."""
    def client_work():
        for i in range(chunks_per_client):
            _ = client.get_signal_segment(file_path, i)
    
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


def direct_mef_reader_access(file_path, num_chunks):
    """
    Read data directly using MefReader (no server, no cache).
    Baseline for comparison.
    """
    rdr = MefReader(file_path)
    channels = rdr.channels
    start_uutc = min(rdr.get_property('start_time'))
    end_uutc = max(rdr.get_property('end_time'))
    
    # 60 second chunks
    chunk_duration_us = 60 * 1e6
    
    for i in range(num_chunks):
        chunk_start = int(start_uutc + i * chunk_duration_us)
        chunk_end = int(min(chunk_start + chunk_duration_us, end_uutc))
        data = rdr.get_data(channels, chunk_start, chunk_end)
        _ = np.array(data)


# --- Benchmark Tests ---

@pytest.mark.benchmark
def test_baseline_direct_mef_reader(benchmark, benchmark_mef3_file):
    """
    BASELINE: Direct MefReader access (no server, no cache).
    20 chunks, 60s each.
    """
    # Pre-read header outside of benchmark
    rdr = MefReader(benchmark_mef3_file)
    _ = rdr.channels
    _ = rdr.get_property('start_time')
    _ = rdr.get_property('end_time')
    del rdr
    
    benchmark(direct_mef_reader_access, benchmark_mef3_file, BENCHMARK_NUM_CHUNKS)


@pytest.mark.benchmark
def test_grpc_sequential_forward_with_prefetch(benchmark, benchmark_mef3_file, grpc_server):
    """
    Sequential forward access via gRPC WITH prefetching.
    20 chunks, 60s each.
    """
    port = grpc_server["port"]
    client = Mef3Client(f"localhost:{port}")
    
    # Setup
    client.open_file(benchmark_mef3_file)
    client.set_signal_segment_size(benchmark_mef3_file, BENCHMARK_SEGMENT_SIZE_S)
    
    # Benchmark
    benchmark(grpc_sequential_forward, client, benchmark_mef3_file, BENCHMARK_NUM_CHUNKS)
    
    # Cleanup
    client.close_file(benchmark_mef3_file)
    client.shutdown()


@pytest.mark.benchmark
def test_grpc_sequential_forward_no_prefetch(benchmark, benchmark_mef3_file, grpc_server):
    """
    Sequential forward access via gRPC WITHOUT prefetching.
    20 chunks, 60s each.
    """
    port = grpc_server["port"]
    client = Mef3Client(f"localhost:{port}")
    
    # Setup - use server with n_prefetch=0 (handled by FileManager config)
    client.open_file(benchmark_mef3_file)
    client.set_signal_segment_size(benchmark_mef3_file, BENCHMARK_SEGMENT_SIZE_S)
    
    # Benchmark
    benchmark(grpc_sequential_forward, client, benchmark_mef3_file, BENCHMARK_NUM_CHUNKS)
    
    # Cleanup
    client.close_file(benchmark_mef3_file)
    client.shutdown()


@pytest.mark.benchmark
def test_grpc_sequential_backward(benchmark, benchmark_mef3_file, grpc_server):
    """
    Sequential backward access via gRPC with prefetching.
    20 chunks, 60s each.
    """
    port = grpc_server["port"]
    client = Mef3Client(f"localhost:{port}")
    
    # Setup
    client.open_file(benchmark_mef3_file)
    client.set_signal_segment_size(benchmark_mef3_file, BENCHMARK_SEGMENT_SIZE_S)
    
    # Benchmark
    benchmark(grpc_sequential_backward, client, benchmark_mef3_file, BENCHMARK_NUM_CHUNKS)
    
    # Cleanup
    client.close_file(benchmark_mef3_file)
    client.shutdown()


@pytest.mark.benchmark
def test_grpc_random_access(benchmark, benchmark_mef3_file, grpc_server):
    """
    Random access via gRPC with prefetching.
    20 chunks, 60s each.
    """
    port = grpc_server["port"]
    client = Mef3Client(f"localhost:{port}")
    
    # Setup
    client.open_file(benchmark_mef3_file)
    client.set_signal_segment_size(benchmark_mef3_file, BENCHMARK_SEGMENT_SIZE_S)
    
    # Benchmark
    benchmark(grpc_random_access, client, benchmark_mef3_file, BENCHMARK_NUM_CHUNKS)
    
    # Cleanup
    client.close_file(benchmark_mef3_file)
    client.shutdown()


@pytest.mark.benchmark
def test_grpc_viewer_pattern(benchmark, benchmark_mef3_file, grpc_server):
    """
    Viewer usage pattern via gRPC (forward, backward, forward navigation).
    20 chunks total, 60s each.
    """
    port = grpc_server["port"]
    client = Mef3Client(f"localhost:{port}")
    
    # Setup
    client.open_file(benchmark_mef3_file)
    client.set_signal_segment_size(benchmark_mef3_file, BENCHMARK_SEGMENT_SIZE_S)
    
    # Benchmark
    benchmark(grpc_viewer_pattern, client, benchmark_mef3_file, BENCHMARK_NUM_CHUNKS)
    
    # Cleanup
    client.close_file(benchmark_mef3_file)
    client.shutdown()


@pytest.mark.benchmark
def test_grpc_concurrent_2_clients(benchmark, benchmark_mef3_file, grpc_server):
    """
    2 concurrent clients via gRPC, each accessing 10 chunks.
    Total: 20 chunk accesses, 60s each.
    """
    port = grpc_server["port"]
    client = Mef3Client(f"localhost:{port}")
    
    # Setup
    client.open_file(benchmark_mef3_file)
    client.set_signal_segment_size(benchmark_mef3_file, BENCHMARK_SEGMENT_SIZE_S)
    
    # Benchmark
    benchmark(grpc_concurrent_access, client, benchmark_mef3_file, 2, 10)
    
    # Cleanup
    client.close_file(benchmark_mef3_file)
    client.shutdown()


@pytest.mark.benchmark
def test_grpc_concurrent_4_clients(benchmark, benchmark_mef3_file, grpc_server):
    """
    4 concurrent clients via gRPC, each accessing 5 chunks.
    Total: 20 chunk accesses, 60s each.
    """
    port = grpc_server["port"]
    client = Mef3Client(f"localhost:{port}")
    
    # Setup
    client.open_file(benchmark_mef3_file)
    client.set_signal_segment_size(benchmark_mef3_file, BENCHMARK_SEGMENT_SIZE_S)
    
    # Benchmark
    benchmark(grpc_concurrent_access, client, benchmark_mef3_file, 4, 5)
    
    # Cleanup
    client.close_file(benchmark_mef3_file)
    client.shutdown()


@pytest.mark.benchmark
def test_grpc_dynamic_window_changes(benchmark, benchmark_mef3_file, grpc_server):
    """
    Test server flexibility with dynamic window size changes.
    Changes window sizes and accesses data after each change.
    Total: ~18 chunk accesses across different window sizes.
    """
    port = grpc_server["port"]
    client = Mef3Client(f"localhost:{port}")
    
    def dynamic_changes():
        client.open_file(benchmark_mef3_file)
        # Change window sizes and access data
        window_sizes = [30, 60, 120, 300, 60, 30]
        for window_size in window_sizes:
            client.set_signal_segment_size(benchmark_mef3_file, window_size)
            # Access 3 segments after each window change
            for i in range(3):
                _ = client.get_signal_segment(benchmark_mef3_file, i)
        client.close_file(benchmark_mef3_file)
    
    # Benchmark
    benchmark(dynamic_changes)
    
    # Cleanup
    client.shutdown()
