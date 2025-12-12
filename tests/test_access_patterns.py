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

import time


# Standard benchmark parameters
BENCHMARK_NUM_CHUNKS = 10  # All benchmarks use 20 chunks for fair comparison
BENCHMARK_SEGMENT_SIZE_S = 5*60  # 60 second segments
ROUNDS = 1
SLEEP_SECONDS = 0.3 # simulating processing delay
N_PREFETCH = 1
MAX_WORKERS = 20
CACHE_CAPACITY_MULTIPLIER = 30

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
    20 chunks, 60s each.
    """
    port = grpc_server_factory(n_prefetch=N_PREFETCH, cache_capacity_multiplier=CACHE_CAPACITY_MULTIPLIER, max_workers=MAX_WORKERS)
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
    20 chunks, 60s each.
    """
    port = grpc_server_factory(n_prefetch=0, cache_capacity_multiplier=0, max_workers=1)
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


