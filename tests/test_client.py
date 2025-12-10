import pytest
import numpy as np

from bnel_mef3_server.client import Mef3Client

import time
import grpc
from concurrent import futures
import bnel_mef3_server.protobufs.gRPCMef3Server_pb2_grpc as pb2_grpc
from bnel_mef3_server.server.mef3_server import gRPCMef3Server, FileManager

@pytest.fixture(scope="session")
def grpc_server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    file_manager = FileManager(3, 3, 4)
    servicer = gRPCMef3Server(file_manager)
    pb2_grpc.add_gRPCMef3ServerServicer_to_server(servicer, server)

    port = 50052
    server.add_insecure_port(f"localhost:{port}")
    server.start()
    time.sleep(0.1)
    yield
    server.stop(0)

@pytest.fixture(scope="module")
def client(grpc_server):
    c = Mef3Client("localhost:50052")
    yield c
    c.shutdown()

def test_open_and_info(client, mef3_file):
    info = client.open_file(mef3_file)
    assert info["file_opened"]
    assert info["file_path"] == mef3_file
    assert info["number_of_channels"] > 0

    info2 = client.get_file_info(mef3_file)
    assert info2 == info

def test_set_segment_and_get_segment(client, mef3_file):
    client.open_file(mef3_file)
    segment_resp = client.set_signal_segment_size(mef3_file, 60)
    assert segment_resp["number_of_segments"] > 0

    meta = client.get_signal_segment(mef3_file, 0)
    array = meta["array"]
    assert array is not None
    assert isinstance(array, np.ndarray)
    assert meta["channel_names"] is not None
    assert meta["start_uutc"] is not None
    assert meta["end_uutc"] is not None
    assert meta["dtype"] is not None
    assert meta["shape"] == array.shape
    assert meta["error_message"] == ''

def test_list_and_close(client, mef3_file):
    client.open_file(mef3_file)
    files = client.list_open_files()
    assert mef3_file in files

    resp = client.close_file(mef3_file)
    assert not resp["file_opened"]
    files = client.list_open_files()
    assert mef3_file not in files
