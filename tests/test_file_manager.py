import pytest
import numpy as np
from unittest.mock import patch
import concurrent.futures
import os
import time

from bnel_mef3_server.server.file_manager import FileManager
from mef_tools import MefReader
import bnel_mef3_server.protobufs.gRPCMef3Server_pb2 as pb2

from .conftest import mef3_file


def test_open_and_close_file(mef3_file):
    fm = FileManager()
    resp = fm.open_file(mef3_file)
    assert resp.file_opened
    assert resp.file_path == mef3_file
    assert mef3_file in fm.list_open_files()

    # Open again, should still be in open files and return error message
    resp_dup = fm.open_file(mef3_file)
    assert resp_dup.file_opened
    assert resp_dup.file_path == mef3_file
    assert mef3_file in fm.list_open_files()
    assert "already open" in resp_dup.error_message

    resp2 = fm.close_file(mef3_file)
    assert not resp2.file_opened
    assert mef3_file not in fm.list_open_files()


def test_set_signal_segment_size_and_get_info(mef3_file):
    fm = FileManager()
    fm.open_file(mef3_file)
    resp = fm.set_signal_segment_size(mef3_file, 0.1)
    assert resp.number_of_segments > 0
    assert resp.file_path == mef3_file
    assert resp.error_message == ""

    info = fm.get_file_info(mef3_file)
    assert info.file_opened
    assert info.file_path == mef3_file
    assert info.number_of_channels > 0


def test_get_signal_segment_and_cache(mef3_file):
    fm = FileManager(n_prefetch=1)
    fm.open_file(mef3_file)
    fm.set_signal_segment_size(mef3_file, 0.1)
    state = fm._files[mef3_file]

    # First access: should be a cache miss
    chunks = list(fm.get_signal_segment(mef3_file, 0))
    assert len(chunks) > 0
    # Second access: should be a cache hit
    chunks2 = list(fm.get_signal_segment(mef3_file, 0))
    assert len(chunks2) > 0

def test_set_and_get_active_channels_and_signal(mef3_file):
    fm = FileManager()
    fm.open_file(mef3_file)
    all_channels = fm._files[mef3_file]['reader'].channels
    # Select a subset and reorder
    selected = [all_channels[3], all_channels[1], all_channels[5]]
    # Set active channels
    resp_set = fm.set_active_channels(mef3_file, selected)
    assert resp_set.active_channels == selected
    # Get active channels
    resp_get = fm.get_active_channels(mef3_file)
    assert resp_get.active_channels == selected
    # Set segment size and get a chunk
    fm.set_signal_segment_size(mef3_file, 0.1)
    chunks = list(fm.get_signal_segment(mef3_file, 0))
    assert len(chunks) > 0
    # Check the returned signal shape matches the selected channels
    arr = np.frombuffer(chunks[0].array_bytes, dtype=chunks[0].dtype)
    arr = arr.reshape(chunks[0].shape)
    assert arr.shape[0] == len(selected)
    # Check the data matches the reference for those channels and order
    rdr = fm._files[mef3_file]['reader']
    chunk_info = fm._files[mef3_file]['chunks'][0]
    ref = rdr.get_data(selected, chunk_info['start'], chunk_info['end'])
    np.testing.assert_array_equal(arr, ref)


def test_prefetching_neighbors(mef3_file):
    fm = FileManager(n_prefetch=1)
    fm.open_file(mef3_file)
    fm.set_signal_segment_size(mef3_file, 0.1)
    state = fm._files[mef3_file]
    state['chunks'] = [{'start': 0, 'end': 100}, {'start': 100, 'end': 200}]
    # Access chunk 1, should prefetch chunk 0
    list(fm.get_signal_segment(mef3_file, 1))
    # Give some time for prefetch thread to run
    import time; time.sleep(0.1)
    # Now chunk 0 should be in cache
    assert 0 in state['cache']


def test_error_handling_on_open():
    fm = FileManager()
    resp = fm.open_file("badfile.mef")
    assert not resp.file_opened


def test_get_signal_segment_invalid(mef3_file):
    fm = FileManager()
    fm.open_file(mef3_file)
    # No chunks set yet
    result = list(fm.get_signal_segment(mef3_file, 0))
    assert len(result) == 1
    assert result[0].error_message != ""

    # Set chunks, but invalid index
    state = fm._files[mef3_file]
    state['chunks'] = [{'start': 0, 'end': 100}]
    result2 = list(fm.get_signal_segment(mef3_file, 2))
    assert len(result2) == 1
    assert result2[0].error_message != ""


def test_shutdown_thread_pool():
    for k in range(5):
        fm = FileManager()
        time.sleep(.1)
        fm.shutdown()
        time.sleep(.1)




def access_pattern(file_manager, file_path):
    """
    Defines a sequence of data access to be measured.
    Accessing the first 5 chunks sequentially is a good test, as the
    benefit of prefetching will appear on chunks 1 through 4.
    """
    for i in range(5):
        # Consume the generator by converting to a list to ensure
        # the data is fully processed.
        _ = list(file_manager.get_signal_segment(file_path, i))
        # time.sleep(0.1)


def test_integrity_multithreaded_read_real(mef3_file):
    """Test that reading the whole file by 5 independent clients yields correct and identical data using the real MEF3 reader."""
    fm = FileManager()
    file_path = mef3_file
    fm.open_file(file_path)
    chunk_seconds = 60
    fm.set_signal_segment_size(file_path, chunk_seconds)
    state = fm._files[file_path]
    num_chunks = len(state['chunks'])

    def read_all_chunks():
        all_data = []
        for i in range(num_chunks):
            chunks = list(fm.get_signal_segment(file_path, i))
            for chunk in chunks:
                arr = np.frombuffer(chunk.array_bytes, dtype=chunk.dtype)
                arr = arr.reshape(chunk.shape)
                all_data.append(arr)
        return np.concatenate(all_data, axis=1)

    # Run 5 clients in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        results = list(executor.map(lambda _: read_all_chunks(), range(5)))

    # All results should be identical
    for arr in results[1:]:
        np.testing.assert_array_equal(arr, results[0])

    # The data should match the reference (all data concatenated)
    rdr = MefReader(file_path)
    start = rdr.get_property('start_time')[0]
    end = rdr.get_property('end_time')[0]
    data_reference = rdr.get_data(rdr.channels, start, end)
    np.testing.assert_array_equal(results[0], data_reference)

def test_open_nonexistent_file_returns_not_opened(tmp_path):
    fm = FileManager()
    non_existent_path = os.path.join(tmp_path, "does_not_exist.mef")
    resp = fm.open_file(non_existent_path)
    assert isinstance(resp, pb2.FileInfoResponse)
    assert resp.file_path == non_existent_path
    assert resp.file_opened is False
