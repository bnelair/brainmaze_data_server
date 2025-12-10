import pytest
import numpy as np
from concurrent import futures

from mef_tools import MefReader

# Import your server implementation and generated protobuf files

import bnel_mef3_server.protobufs.gRPCMef3Server_pb2 as pb2

from .conftest import mef3_file, grpc_stub_1, grpc_stub_2


@pytest.mark.order(1)
def test_list_open_files(grpc_stub_1, mef3_file):
    # Should be empty before opening
    resp = grpc_stub_1.ListOpenFiles(pb2.ListOpenFilesRequest())
    assert mef3_file not in resp.file_paths

    # Open file
    grpc_stub_1.OpenFile(pb2.OpenFileRequest(file_path=mef3_file))
    resp = grpc_stub_1.ListOpenFiles(pb2.ListOpenFilesRequest())
    assert mef3_file in resp.file_paths

    # Close file
    grpc_stub_1.CloseFile(pb2.CloseFileRequest(file_path=mef3_file))
    resp = grpc_stub_1.ListOpenFiles(pb2.ListOpenFilesRequest())
    assert mef3_file not in resp.file_paths


def test_basic_functional(grpc_stub_1, mef3_file):
    """Tests the OpenFile RPC."""
    request = pb2.OpenFileRequest(file_path=mef3_file)
    response = grpc_stub_1.OpenFile(request)

    # Use actual file info
    assert response.file_opened is True
    assert response.file_path == mef3_file
    num_channels = response.number_of_channels
    duration_s = response.duration_s

    request_info = pb2.FileInfoRequest(file_path=mef3_file)
    response_info = grpc_stub_1.FileInfo(request)

    assert response == response_info

    # Use a segment size that divides the duration
    segment_seconds = 60
    request = pb2.SetSignalSegmentRequest(file_path=mef3_file, seconds=segment_seconds)
    response = grpc_stub_1.SetSignalSegmentSize(request)

    # Calculate expected number of segments
    expected_segments = int(np.ceil(duration_s / segment_seconds))
    assert response.number_of_segments == expected_segments

    # Request the first segment (index 0)
    request = pb2.SignalChunkRequest(file_path=mef3_file, chunk_idx=0)
    response_stream = grpc_stub_1.GetSignalSegment(request)

    tiles = []
    dtype = None
    for chunk in response_stream:
        arr = np.frombuffer(chunk.array_bytes, dtype=chunk.dtype)
        arr = arr.reshape(chunk.shape)
        tiles.append(arr)
        dtype = chunk.dtype

    # Concatenate all tiles along the sample axis
    reconstructed = np.concatenate(tiles, axis=1)

    rdr = MefReader(mef3_file)
    start = rdr.get_property('start_time')[0]
    end = start + segment_seconds * 1e6

    data_reference = rdr.get_data(rdr.channels, start, end)

    np.testing.assert_array_equal(reconstructed, data_reference)

import concurrent.futures

def _get_signal_segment(grpc_stub_1, file_path, chunk_idx):
    request = pb2.SignalChunkRequest(file_path=file_path, chunk_idx=chunk_idx)
    response_stream = grpc_stub_1.GetSignalSegment(request)
    tiles = []
    dtype = None
    for chunk in response_stream:
        arr = np.frombuffer(chunk.array_bytes, dtype=chunk.dtype)
        arr = arr.reshape(chunk.shape)
        tiles.append(arr)
        dtype = chunk.dtype
    reconstructed = np.concatenate(tiles, axis=1)
    return reconstructed

def test_concurrent_same_data(grpc_stub_1, grpc_stub_2, mef3_file):
    """Test two concurrent stubs requesting the same chunk and compare results."""
    # Prepare: open file, set chunk size
    grpc_stub_1.OpenFile(pb2.OpenFileRequest(file_path=mef3_file))
    grpc_stub_1.SetSignalSegmentSize(pb2.SetSignalSegmentRequest(file_path=mef3_file, seconds=1*60))

    # Use ThreadPoolExecutor to simulate concurrency
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(_get_signal_segment, grpc_stub_1, mef3_file, 0),
            executor.submit(_get_signal_segment, grpc_stub_2, mef3_file, 0)
        ]
        results = [f.result() for f in futures]

    # Both results should be identical and match the reference
    np.testing.assert_array_equal(results[0], results[1])

    rdr = MefReader(mef3_file)
    start = rdr.get_property('start_time')[0]
    end = start + 1*60*1e6
    data_reference = rdr.get_data(rdr.channels, start, end)
    data_reference = np.array(data_reference)
    assert data_reference.shape == results[0].shape == results[1].shape
    np.testing.assert_array_equal(results[0], data_reference)



def test_concurrent_different_data(grpc_stub_1, grpc_stub_2, mef3_file):
    """Test two concurrent stubs requesting different chunks and compare to reference."""
    grpc_stub_1.OpenFile(pb2.OpenFileRequest(file_path=mef3_file))
    grpc_stub_1.SetSignalSegmentSize(pb2.SetSignalSegmentRequest(file_path=mef3_file, seconds=1*60))

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(_get_signal_segment, grpc_stub_1, mef3_file, 0),
            executor.submit(_get_signal_segment, grpc_stub_2, mef3_file, 1)
        ]
        result0, result1 = [f.result() for f in futures]

    rdr = MefReader(mef3_file)
    start0 = rdr.get_property('start_time')[0]
    end0 = start0 + 1*60*1e6
    start1 = end0
    end1 = start1 + 1*60*1e6

    data_ref0 = rdr.get_data(rdr.channels, start0, end0)
    data_ref1 = rdr.get_data(rdr.channels, start1, end1)

    np.testing.assert_array_equal(result0, data_ref0)
    np.testing.assert_array_equal(result1, data_ref1)
