
from bnel_mef3_server.client import Mef3Client
from tests.conftest import MEF3_TEST_FS, MEF3_TEST_CHANNELS, MEF3_FUNCTIONAL_TEST_DURATION_S

import numpy as np
import pytest
from tqdm import tqdm


@pytest.mark.slow
def test_real_life_data(functional_test_mef3_file, launch_server_process):
    """
    Test real-world usage patterns with dynamic parameter changes.
    Tests server flexibility with window size and active channel changes.
    """
    pth_mef = functional_test_mef3_file
    fs = MEF3_TEST_FS
    n_channels = MEF3_TEST_CHANNELS
    data_len_s = MEF3_FUNCTIONAL_TEST_DURATION_S

    cl = Mef3Client("localhost:50051")
    cl.open_file(pth_mef)
    fi = cl.get_file_info(pth_mef)

    channels = fi["channel_names"]
    assert len(channels) == n_channels, f"Expected {n_channels} channels, got {len(channels)}"
    
    # Verify timestamps are in microseconds
    duration_us = fi["end_uutc"] - fi["start_uutc"]
    duration_s = duration_us / 1e6
    assert abs(duration_s - data_len_s) < 1, f"Duration mismatch: expected ~{data_len_s}s, got {duration_s}s"
    
    cl.set_active_channels(pth_mef, channels)

    # Test 1: Change active channels dynamically
    cl.set_active_channels(pth_mef, channels[:32])

    # Test 2: Change segment sizes dynamically (server must handle this flexibly)
    cl.set_signal_segment_size(pth_mef, 1*60)
    cl.set_signal_segment_size(pth_mef, 5*60)

    # Verify data integrity after segment size change
    x = cl.get_signal_segment(pth_mef, 0)
    assert x['array'] is not None, "No data returned after segment size change"
    assert x['array'].shape[0] == 32, f"Expected 32 active channels, got {x['array'].shape[0]}"
    # Verify sample count matches expected (5 minutes * 60 seconds * fs)
    expected_samples = 5 * 60 * fs
    assert x['array'][0].shape[0] == expected_samples, \
        f"Expected {expected_samples} samples, got {x['array'][0].shape[0]}"

    # Test 3: Change back to smaller segment size
    cl.set_signal_segment_size(pth_mef, 1*60)
    n_segments = (data_len_s) // (1*60)

    # Test 4: Verify sample count for smaller segments
    x = cl.get_signal_segment(pth_mef, 0)
    expected_samples = 1 * 60 * fs
    assert x['array'][0].shape[0] == expected_samples, \
        f"Expected {expected_samples} samples per 1-minute segment, got {x['array'][0].shape[0]}"

    # Test 5: Read all segments and verify consistency
    for seg_idx in tqdm(range(n_segments), desc="Verifying all segments"):
        x = cl.get_signal_segment(pth_mef, seg_idx)
        arr = x['array']
        # Only 32 channels are active
        assert arr.shape[0] == 32, f"Segment {seg_idx}: expected 32 channels, got {arr.shape[0]}"
        for ch_idx in range(32):
            # Verify sample count
            assert arr[ch_idx].shape[0] == expected_samples, \
                f"Segment {seg_idx}, channel {ch_idx}: expected {expected_samples} samples"


@pytest.mark.slow
def test_dynamic_parameter_changes(functional_test_mef3_file, launch_server_process):
    """
    Test that the server handles frequent parameter changes gracefully.
    This simulates real-world usage where users frequently adjust window sizes and channels.
    """
    pth_mef = functional_test_mef3_file
    fs = MEF3_TEST_FS
    
    cl = Mef3Client("localhost:50051")
    cl.open_file(pth_mef)
    fi = cl.get_file_info(pth_mef)
    channels = fi["channel_names"]
    
    # Test rapid window size changes
    window_sizes = [30, 60, 120, 300, 60, 30]  # seconds
    for window_size in window_sizes:
        resp = cl.set_signal_segment_size(pth_mef, window_size)
        assert resp["number_of_segments"] > 0, \
            f"Failed to set segment size to {window_size}s"
        
        # Verify we can retrieve data after each change
        x = cl.get_signal_segment(pth_mef, 0)
        assert x['array'] is not None, f"No data returned for window size {window_size}s"
        expected_samples = window_size * fs
        assert x['array'][0].shape[0] == expected_samples, \
            f"Window size {window_size}s: expected {expected_samples} samples, got {x['array'][0].shape[0]}"
    
    # Test rapid channel changes
    channel_configs = [
        channels[:16],   # First 16
        channels[16:32], # Next 16
        channels[:32],   # First 32
        channels[32:],   # Last 32
        channels,        # All channels
    ]
    
    cl.set_signal_segment_size(pth_mef, 60)  # Fixed window for channel tests
    
    for i, channel_list in enumerate(channel_configs):
        resp = cl.set_active_channels(pth_mef, channel_list)
        assert len(resp["active_channels"]) == len(channel_list), \
            f"Config {i}: expected {len(channel_list)} active channels"
        
        # Verify data retrieval with new channel config
        x = cl.get_signal_segment(pth_mef, 0)
        assert x['array'].shape[0] == len(channel_list), \
            f"Config {i}: expected {len(channel_list)} channels in data"


@pytest.mark.slow
def test_error_handling(functional_test_mef3_file, launch_server_process):
    """
    Test that server errors are properly caught and returned to the client.
    No server crashes should occur.
    """
    pth_mef = functional_test_mef3_file
    
    cl = Mef3Client("localhost:50051")
    
    # Test 1: Request data from unopened file
    x = cl.get_signal_segment(pth_mef, 0)
    assert 'error_message' in x, "Expected error_message in response"
    assert x['error_message'] != '', "Expected non-empty error message for unopened file"
    
    # Now open the file
    cl.open_file(pth_mef)
    
    # Test 2: Request invalid segment index (negative)
    x = cl.get_signal_segment(pth_mef, -1)
    assert 'error_message' in x
    assert x['error_message'] != '', "Expected error for negative segment index"
    
    # Test 3: Request segment without setting segment size
    # (This might return an error or handle gracefully)
    x = cl.get_signal_segment(pth_mef, 0)
    # Should return error or empty data, but not crash
    assert 'error_message' in x
    
    # Test 4: Set segment size properly
    cl.set_signal_segment_size(pth_mef, 60)
    
    # Test 5: Request segment index beyond available data
    x = cl.get_signal_segment(pth_mef, 99999)
    assert 'error_message' in x
    assert x['error_message'] != '', "Expected error for out-of-bounds segment index"
    
    # Test 6: Set invalid active channels
    resp = cl.set_active_channels(pth_mef, ["invalid_channel_name"])
    assert 'error_message' in resp
    # Should handle gracefully, not crash
    
    # Test 7: Verify server is still responsive after errors
    fi = cl.get_file_info(pth_mef)
    assert fi["file_opened"] == True, "Server should still be responsive after errors"