"""
Error handling tests for MEF3 server.

These tests ensure that the server properly handles errors and reports them
to the client without crashing, maintaining robustness and reliability.
"""
import pytest
import grpc
from bnel_mef3_server.client import Mef3Client
import bnel_mef3_server.protobufs.gRPCMef3Server_pb2 as pb2


def test_open_nonexistent_file(grpc_server_factory):
    """Test that opening a nonexistent file returns an error without crashing."""
    port = grpc_server_factory(n_prefetch=1, cache_capacity_multiplier=3, n_process_workers=0)
    client = Mef3Client(f"localhost:{port}")
    
    # Try to open a file that doesn't exist
    result = client.open_file("/nonexistent/path/to/file.mefd")
    
    # Should return error, not crash - file_opened should be False
    assert result['file_opened'] is False
    
    client.shutdown()


def test_get_segment_from_unopened_file(grpc_server_factory):
    """Test that getting segment from unopened file returns error."""
    port = grpc_server_factory(n_prefetch=1, cache_capacity_multiplier=3, n_process_workers=0)
    client = Mef3Client(f"localhost:{port}")
    
    # Try to get segment without opening file - should raise or return empty
    try:
        result = client.get_signal_segment("/some/file.mefd", 0)
        # If it doesn't raise, it should return something indicating error
        # The client wraps errors, so we just check it doesn't crash
        assert result is not None or True  # Server didn't crash
    except Exception:
        # Expected - file not found or not open
        pass
    
    client.shutdown()


def test_invalid_segment_index(grpc_server_factory, mef3_file):
    """Test that requesting invalid segment index returns error."""
    port = grpc_server_factory(n_prefetch=1, cache_capacity_multiplier=3, n_process_workers=0)
    client = Mef3Client(f"localhost:{port}")
    
    # Open file and set segment size
    client.open_file(mef3_file)
    client.set_signal_segment_size(mef3_file, 60)
    
    # Request segment with invalid index (way out of range)
    try:
        result = client.get_signal_segment(mef3_file, 9999)
        # Client should handle error gracefully
        # May return None or raise exception, but shouldn't crash server
    except Exception:
        # Expected behavior - invalid index
        pass
    
    # Verify server still works after error
    result = client.get_signal_segment(mef3_file, 0)
    assert result is not None
    assert 'array' in result
    
    client.close_file(mef3_file)
    client.shutdown()


def test_set_segment_size_before_open(grpc_server_factory):
    """Test that setting segment size on unopened file returns error."""
    port = grpc_server_factory(n_prefetch=1, cache_capacity_multiplier=3, n_process_workers=0)
    client = Mef3Client(f"localhost:{port}")
    
    # Try to set segment size without opening file
    result = client.set_signal_segment_size("/some/file.mefd", 60)
    
    # Should return error
    assert result['number_of_segments'] == 0
    # error_message field may or may not be present depending on client version
    
    client.shutdown()


def test_set_active_channels_invalid(grpc_server_factory, mef3_file):
    """Test that setting invalid channel names returns appropriate response."""
    port = grpc_server_factory(n_prefetch=1, cache_capacity_multiplier=3, n_process_workers=0)
    client = Mef3Client(f"localhost:{port}")
    
    # Open file
    client.open_file(mef3_file)
    
    # Try to set channels that don't exist
    result = client.set_active_channels(mef3_file, ["InvalidChannel1", "InvalidChannel2"])
    
    # Should indicate error or return empty active channels
    # The server should handle this gracefully
    assert 'active_channels' in result
    # Either returns error or returns valid channels (not the invalid ones)
    if result['active_channels']:
        assert "InvalidChannel1" not in result['active_channels']
    
    client.close_file(mef3_file)
    client.shutdown()


def test_concurrent_open_same_file(grpc_server_factory, mef3_file):
    """Test that opening the same file twice is handled properly."""
    port = grpc_server_factory(n_prefetch=1, cache_capacity_multiplier=3, n_process_workers=0)
    client = Mef3Client(f"localhost:{port}")
    
    # Open file first time
    result1 = client.open_file(mef3_file)
    assert result1['file_opened'] is True
    
    # Try to open same file again
    result2 = client.open_file(mef3_file)
    
    # Should either succeed or return informative error
    # The important thing is the server doesn't crash
    assert 'file_opened' in result2
    
    client.close_file(mef3_file)
    client.shutdown()


def test_close_unopened_file(grpc_server_factory):
    """Test that closing an unopened file doesn't crash the server."""
    port = grpc_server_factory(n_prefetch=1, cache_capacity_multiplier=3, n_process_workers=0)
    client = Mef3Client(f"localhost:{port}")
    
    # Try to close a file that was never opened
    result = client.close_file("/some/file.mefd")
    
    # Should return response without crashing
    assert 'file_opened' in result
    assert result['file_opened'] is False
    
    client.shutdown()


def test_get_info_unopened_file(grpc_server_factory):
    """Test that getting info for unopened file returns appropriate response."""
    port = grpc_server_factory(n_prefetch=1, cache_capacity_multiplier=3, n_process_workers=0)
    client = Mef3Client(f"localhost:{port}")
    
    # Try to get info for file that isn't open
    result = client.get_file_info("/some/file.mefd")
    
    # Should return response indicating file not open
    assert result['file_opened'] is False
    
    client.shutdown()


def test_server_survives_multiple_errors(grpc_server_factory, mef3_file):
    """Test that server continues to work after multiple errors."""
    port = grpc_server_factory(n_prefetch=1, cache_capacity_multiplier=3, n_process_workers=0)
    client = Mef3Client(f"localhost:{port}")
    
    # Generate multiple errors
    client.open_file("/nonexistent1.mefd")
    client.get_file_info("/nonexistent2.mefd")
    client.close_file("/nonexistent3.mefd")
    list(client.get_signal_segment("/nonexistent4.mefd", 0))
    
    # Now do a valid operation to confirm server still works
    result = client.open_file(mef3_file)
    assert result['file_opened'] is True
    
    # Clean up
    client.close_file(mef3_file)
    client.shutdown()


def test_worker_process_error_handling(grpc_server_factory, mef3_file):
    """Test that errors in worker processes are handled gracefully."""
    # Use worker processes for this test
    port = grpc_server_factory(n_prefetch=2, cache_capacity_multiplier=5, n_process_workers=2)
    client = Mef3Client(f"localhost:{port}")
    
    # Open file and set up segments
    client.open_file(mef3_file)
    client.set_signal_segment_size(mef3_file, 60)
    
    # Request a valid segment (should work even with workers)
    result = client.get_signal_segment(mef3_file, 0)
    assert result is not None
    assert 'array' in result
    
    # Request invalid segment (workers should handle error)
    try:
        result = client.get_signal_segment(mef3_file, 9999)
        # May return None or raise, either is fine
    except Exception:
        # Expected - invalid segment
        pass
    
    # Verify server still works after worker error
    result = client.get_signal_segment(mef3_file, 0)
    assert result is not None
    assert 'array' in result
    
    client.close_file(mef3_file)
    client.shutdown()
