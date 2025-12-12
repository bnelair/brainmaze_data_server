"""
Benchmark tests for MEF3 server performance.

Test Data Characteristics:
- Duration: 2 hours of continuous EEG data
- Channels: 64 channels
- Sampling Rate: 256 Hz
- Precision: 2 (MEF compression level)
- Segment Size: 5 minutes (300 seconds)
- Total Segments: 24 segments (2 hours / 5 minutes)

Benchmark Configuration:
- Number of segments tested: 10 (representative sample)
- Processing delay: 0.3 seconds (simulates realistic client-side processing)
- Prefetch: 1 segment ahead
- Cache capacity: 31 segments (1 prefetch + 30 multiplier)
- Worker processes: 2 (for parallel I/O)

All benchmarks use the same test file and configuration for fair comparison.
"""
import pytest
import numpy as np
import threading
from mef_tools import MefReader
from bnel_mef3_server.client import Mef3Client

import time


# Standard benchmark parameters
BENCHMARK_NUM_CHUNKS = 10  # All benchmarks use 10 chunks for fair comparison
BENCHMARK_SEGMENT_SIZE_S = 5*60  # 5 minute segments
ROUNDS = 1
SLEEP_SECONDS = 0.3 # simulating processing delay
N_PREFETCH = 1
CACHE_CAPACITY_MULTIPLIER = 30
N_PROCESS_WORKERS = 2  # Number of worker processes for parallel reading

# --- Helper functions for access patterns ---

def grpc_sequential_forward(client, file_path, num_chunks):
    """Access chunks in forward sequential order via gRPC."""
    for i in range(num_chunks):
        ts = time.time()
        _ = client.get_signal_segment(file_path, i)
        te = time.time()
        # print(f"gRPCReader - Chunk {i} read in {te - ts} seconds")
        time.sleep(SLEEP_SECONDS)  # Simulate slight processing delay



def direct_mef_reader_access(rdr, num_chunks):
    """
    Read data directly using MefReader (no server, no cache).
    Baseline for comparison.
    """
    channels = rdr.channels
    start = min(rdr.get_property('start_time')) / 1e6
    end = max(rdr.get_property('end_time')) / 1e6
    chunk_duration = BENCHMARK_SEGMENT_SIZE_S

    for i in range(num_chunks):
        chunk_start = start + i * chunk_duration
        chunk_end = chunk_start + chunk_duration
        ts = time.time()
        data = rdr.get_data(channels, chunk_start*1e6, chunk_end*1e6)
        te = time.time()
        # print(f"MefReader - Chunk {i} read in {te - ts} seconds")
        time.sleep(SLEEP_SECONDS)  # Simulate slight processing delay


# --- Benchmark Tests ---

@pytest.mark.benchmark
def test_baseline_direct_mef_reader(benchmark, benchmark_mef3_file):
    """
    BASELINE: Direct MefReader access (no server, no cache).
    20 chunks, 60s each.
    """
    # Pre-read header outside of benchmark
    rdr = MefReader(benchmark_mef3_file)

    benchmark.pedantic(direct_mef_reader_access, args=(rdr, BENCHMARK_NUM_CHUNKS), rounds=ROUNDS)


@pytest.mark.benchmark
def test_grpc_sequential_forward_with_prefetch(benchmark, benchmark_mef3_file, grpc_server_factory):
    """
    Sequential forward access via gRPC WITH prefetching.
    10 chunks, 5min each.
    """
    port = grpc_server_factory(n_prefetch=N_PREFETCH, cache_capacity_multiplier=CACHE_CAPACITY_MULTIPLIER, n_process_workers=N_PROCESS_WORKERS)
    client = Mef3Client(f"localhost:{port}")
    
    # Setup
    client.open_file(benchmark_mef3_file)
    fi = client.get_file_info(benchmark_mef3_file)
    channels = fi['channel_names']
    client.set_active_channels(benchmark_mef3_file, channels)
    client.set_signal_segment_size(benchmark_mef3_file, BENCHMARK_SEGMENT_SIZE_S)
    
    # Benchmark
    benchmark.pedantic(grpc_sequential_forward, args=(client, benchmark_mef3_file, BENCHMARK_NUM_CHUNKS), rounds=ROUNDS)

    # Cleanup
    client.close_file(benchmark_mef3_file)
    client.shutdown()


@pytest.mark.benchmark
def test_grpc_sequential_forward_no_prefetch(benchmark, benchmark_mef3_file, grpc_server_factory):
    """
    Sequential forward access via gRPC WITHOUT prefetching.
    10 chunks, 5min each.
    """
    port = grpc_server_factory(n_prefetch=0, cache_capacity_multiplier=0, n_process_workers=0)
    client = Mef3Client(f"localhost:{port}")

    
    # Setup - use server with n_prefetch=0
    client.open_file(benchmark_mef3_file)
    fi = client.get_file_info(benchmark_mef3_file)
    channels = fi['channel_names']
    client.set_active_channels(benchmark_mef3_file, channels)
    client.set_signal_segment_size(benchmark_mef3_file, BENCHMARK_SEGMENT_SIZE_S)
    
    # Benchmark
    benchmark.pedantic(grpc_sequential_forward, args=(client, benchmark_mef3_file, BENCHMARK_NUM_CHUNKS), rounds=ROUNDS)

    # Cleanup
    client.close_file(benchmark_mef3_file)
    client.shutdown()



@pytest.mark.benchmark
def test_grpc_concurrent_access_3_clients(benchmark, benchmark_mef3_file, grpc_server_factory):
    """
    Concurrent access from 3 clients reading different segments simultaneously.
    Tests parallel read performance with multiple concurrent clients.
    Each client reads 3 chunks sequentially with 0.3s processing delay.
    
    Note: All clients share the same file connection to test concurrent segment access.
    """
    import concurrent.futures
    
    port = grpc_server_factory(n_prefetch=N_PREFETCH, cache_capacity_multiplier=CACHE_CAPACITY_MULTIPLIER, n_process_workers=N_PROCESS_WORKERS)
    
    # Setup file once outside the benchmark
    client = Mef3Client(f"localhost:{port}")
    client.open_file(benchmark_mef3_file)
    fi = client.get_file_info(benchmark_mef3_file)
    channels = fi['channel_names']
    client.set_active_channels(benchmark_mef3_file, channels)
    client.set_signal_segment_size(benchmark_mef3_file, BENCHMARK_SEGMENT_SIZE_S)
    
    def read_worker(client_id, num_chunks):
        """Worker function that reads different segments."""
        # Each client reads different segments
        start_idx = client_id * num_chunks
        for i in range(num_chunks):
            chunk_idx = start_idx + i
            if chunk_idx < 15:  # Stay within available segments
                _ = client.get_signal_segment(benchmark_mef3_file, chunk_idx)
                time.sleep(SLEEP_SECONDS)
    
    def concurrent_access():
        """Run 3 concurrent read workers."""
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures_list = [
                executor.submit(read_worker, client_id, 3)
                for client_id in range(3)
            ]
            concurrent.futures.wait(futures_list)
    
    # Benchmark concurrent access
    benchmark.pedantic(concurrent_access, rounds=ROUNDS)
    
    # Cleanup
    client.close_file(benchmark_mef3_file)
    client.shutdown()

