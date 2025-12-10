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