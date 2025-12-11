from mef_tools import MefWriter

import time

import pytest
import tempfile
import os
import numpy as np
import datetime
import grpc
import threading
from concurrent import futures

import bnel_mef3_server.protobufs.gRPCMef3Server_pb2_grpc as pb2_grpc

from bnel_mef3_server.server.mef3_server import gRPCMef3Server, FileManager


@pytest.fixture(scope="session")
def mef3_file():
    """Legacy fixture for backward compatibility (5 minutes of data)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        file_path = os.path.join(tmpdir, "test_data.mefd")
        channel_names = [f"Ch{i}" for i in range(0, 64)]
        fs = 256
        duration_s = 5*60

        wrt = MefWriter(file_path, overwrite=True)
        wrt.mef_block_len = 1000
        wrt.max_nans_written = 0
        wrt.data_units = 'uV'

        start_uutc = int(np.round(datetime.datetime.now().timestamp() * 1e6))

        for ch in channel_names:
            x = np.random.randn(fs*duration_s)
            wrt.write_data(x, ch, start_uutc=start_uutc, sampling_freq=fs, precision=3, discont_handler=False)

        yield file_path


# MEF3 test data configuration constants
MEF3_TEST_CHANNELS = 64
MEF3_TEST_FS = 256
MEF3_TEST_PRECISION = 2
# Timestamp 100 days in the past (to simulate historical data)
MEF3_TEST_START_OFFSET_DAYS = 100

# For functional tests (1 hour)
MEF3_FUNCTIONAL_TEST_DURATION_S = 60 * 60  

# For benchmarks (2 hours) 
MEF3_BENCHMARK_DURATION_S = 2 * 60 * 60


@pytest.fixture(scope="session")
def benchmark_mef3_file():
    """
    Creates a realistic MEF3 file for benchmarks.
    - 64 channels
    - 256 Hz sampling rate
    - 2 hours of data
    - precision=2 as specified
    - Timestamp set 100 days in past to simulate historical data
    
    Session-scoped for optimal performance across all benchmarks.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        file_path = os.path.join(tmpdir, "benchmark_data.mefd")
        
        wrt = MefWriter(file_path, overwrite=True)
        wrt.mef_block_len = 10000
        wrt.max_nans_written = 0
        
        # Use consistent timestamp in microseconds (MEF3 standard)
        # Set 100 days in the past to simulate historical data
        s = (datetime.datetime.now().timestamp() - 3600*24*MEF3_TEST_START_OFFSET_DAYS) * 1e6
        
        print("\n[Creating benchmark MEF3 file - 2 hours of data]")
        for idx in range(MEF3_TEST_CHANNELS):
            chname = f"chan_{idx+1:03d}"
            x = np.random.randn(MEF3_BENCHMARK_DURATION_S * MEF3_TEST_FS)
            wrt.write_data(x, chname, s, MEF3_TEST_FS, precision=MEF3_TEST_PRECISION)
        print("[Benchmark MEF3 file created successfully]")
        
        yield file_path


@pytest.fixture(scope="module")
def functional_test_mef3_file(tmp_path_factory):
    """
    Creates a realistic MEF3 file for functional tests.
    - 64 channels
    - 256 Hz sampling rate
    - 1 hour of data
    - precision=2 as specified
    - Timestamp set 100 days in past to simulate historical data
    
    Module-scoped so the file is created once for all tests in a module.
    """
    tmpdir = tmp_path_factory.mktemp("functional_test_data")
    pth = str(tmpdir)
    pth_mef = os.path.join(pth, "functional_test_data.mefd")
    
    wrt = MefWriter(pth_mef, overwrite=True)
    wrt.mef_block_len = 10000
    wrt.max_nans_written = 0
    
    # Use consistent timestamp in microseconds (MEF3 standard)
    # Set 100 days in the past to simulate historical data
    s = (datetime.datetime.now().timestamp() - 3600*24*MEF3_TEST_START_OFFSET_DAYS) * 1e6
    
    print("\n[Creating functional test MEF3 file - 1 hour of data]")
    for idx in range(MEF3_TEST_CHANNELS):
        chname = f"chan_{idx+1:03d}"
        x = np.random.randn(MEF3_FUNCTIONAL_TEST_DURATION_S * MEF3_TEST_FS)
        wrt.write_data(x, chname, s, MEF3_TEST_FS, precision=MEF3_TEST_PRECISION)
    print("[Functional test MEF3 file created successfully]")
    
    return pth_mef


@pytest.fixture(scope="function")
def launch_server_process():
    """
    Launches the gRPC server main() in a separate process.
    Used for functional tests that need server on port 50051.
    """
    import multiprocessing
    from bnel_mef3_server.server.__main__ import main as server_entrypoint
    
    # Initialize process targeting the main entrypoint
    proc = multiprocessing.Process(target=server_entrypoint, daemon=True)
    proc.start()

    # Wait for the server to bind the port
    time.sleep(2)

    yield

    # Terminate the process (triggers handle_sigterm in main)
    proc.terminate()
    proc.join(timeout=5)
    # Give a bit more time for the port to be released
    time.sleep(0.5)


# --- Server and Client Fixtures ---------------------------------------
@pytest.fixture(scope="session")
def grpc_server():
    """Starts and stops the gRPC server for the test session."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    file_manager = FileManager(3, 3, 4)
    servicer = gRPCMef3Server(file_manager)
    pb2_grpc.add_gRPCMef3ServerServicer_to_server(servicer, server)

    port = 50052
    server.add_insecure_port(f"localhost:{port}")  # Changed from [::] to localhost

    server_thread = threading.Thread(target=server.start)
    server_thread.daemon = True
    server_thread.start()

    # Give the server a moment to start up in the background thread
    time.sleep(0.1)

    print(f"\nTest gRPC server started on port {port}")

    server_info = {"port": port, "server": server}
    yield server_info

    print("\nStopping test gRPC server...")
    server.stop(0)


@pytest.fixture(scope="session")
def grpc_stub_1(grpc_server):
    """Creates a gRPC client stub for the test session."""
    port = grpc_server["port"]
    channel = grpc.insecure_channel(f"localhost:{port}")
    stub = pb2_grpc.gRPCMef3ServerStub(channel)
    yield stub
    channel.close()

@pytest.fixture(scope="session")
def grpc_stub_2(grpc_server):
    """Creates a gRPC client stub for the test session."""
    port = grpc_server["port"]
    channel = grpc.insecure_channel(f"localhost:{port}")
    stub = pb2_grpc.gRPCMef3ServerStub(channel)
    yield stub
    channel.close()