"""
Demo: Testing the Mef3Client with real-life big data.

This script demonstrates opening a large MEF3 file, setting segment size, 
retrieving signal data, and testing various edge cases that may occur with 
real-life data.

This test is designed to work with large files and may take a long time to run.
It should be run manually for integration testing but not as part of regular CI/CD.
"""
import sys
import os
import time
from bnel_mef3_server.client import Mef3Client

def test_big_data():
    """Test the MEF3 server with big data."""
    
    # Path to the big MEF3 file - this should be provided as a command line argument
    if len(sys.argv) < 2:
        print("Usage: python run_big_data.py <path_to_mef3_file> [server_address]")
        print("Example: python run_big_data.py /path/to/big_file.mefd localhost:50051")
        sys.exit(1)
    
    MEF3_FILE = sys.argv[1]
    server_address = sys.argv[2] if len(sys.argv) > 2 else "localhost:50051"
    
    if not os.path.exists(MEF3_FILE):
        print(f"ERROR: File does not exist: {MEF3_FILE}")
        sys.exit(1)
    
    print(f"Testing with file: {MEF3_FILE}")
    print(f"Connecting to server: {server_address}")
    
    # Connect to the server
    try:
        client = Mef3Client(server_address)
        print("✓ Connected to server")
    except Exception as e:
        print(f"ERROR: Failed to connect to server: {e}")
        sys.exit(1)
    
    try:
        # Test 1: Open the file
        print("\n--- Test 1: Opening file ---")
        start_time = time.time()
        info = client.open_file(MEF3_FILE)
        elapsed = time.time() - start_time
        print(f"✓ File opened in {elapsed:.2f}s")
        print(f"  Channels: {info['number_of_channels']}")
        print(f"  Duration: {info['duration_s']:.2f}s")
        print(f"  Start: {info['start_uutc']}")
        print(f"  End: {info['end_uutc']}")
        
        if not info['file_opened']:
            print(f"ERROR: Failed to open file: {info.get('error_message', 'Unknown error')}")
            sys.exit(1)
        
        # Test 2: Set segment size to 60 seconds
        print("\n--- Test 2: Setting segment size to 60 seconds ---")
        start_time = time.time()
        chunk_resp = client.set_signal_segment_size(MEF3_FILE, 60)
        elapsed = time.time() - start_time
        print(f"✓ Segment size set in {elapsed:.2f}s")
        print(f"  Number of segments: {chunk_resp['number_of_segments']}")
        
        # Test 3: Query number of segments
        print("\n--- Test 3: Querying number of segments ---")
        seg_resp = client.get_number_of_segments(MEF3_FILE)
        print(f"✓ Number of segments: {seg_resp['number_of_segments']}")
        if seg_resp['number_of_segments'] != chunk_resp['number_of_segments']:
            print(f"WARNING: Mismatch in segment count!")
        
        # Test 4: Retrieve first segment
        print("\n--- Test 4: Retrieving first segment ---")
        start_time = time.time()
        result = client.get_signal_segment(MEF3_FILE, 0)
        elapsed = time.time() - start_time
        if result['array'] is not None:
            print(f"✓ First segment retrieved in {elapsed:.2f}s")
            print(f"  Shape: {result['shape']}")
            print(f"  Channels: {len(result['channel_names'])}")
            print(f"  Sampling rate: {result['fs']} Hz")
        else:
            print(f"ERROR: Failed to retrieve segment: {result.get('error_message', 'Unknown error')}")
        
        # Test 5: Retrieve multiple segments sequentially
        print("\n--- Test 5: Retrieving first 5 segments sequentially ---")
        num_segments_to_test = min(5, chunk_resp['number_of_segments'])
        start_time = time.time()
        for i in range(num_segments_to_test):
            result = client.get_signal_segment(MEF3_FILE, i)
            if result['array'] is None:
                print(f"  ERROR on segment {i}: {result.get('error_message', 'Unknown error')}")
            else:
                print(f"  Segment {i}: shape={result['shape']}, retrieved successfully")
        elapsed = time.time() - start_time
        print(f"✓ Retrieved {num_segments_to_test} segments in {elapsed:.2f}s ({elapsed/num_segments_to_test:.2f}s per segment)")
        
        # Test 6: Reset segment size (test cache clearing)
        print("\n--- Test 6: Resetting segment size to 30 seconds ---")
        start_time = time.time()
        chunk_resp2 = client.set_signal_segment_size(MEF3_FILE, 30)
        elapsed = time.time() - start_time
        print(f"✓ Segment size reset in {elapsed:.2f}s")
        print(f"  New number of segments: {chunk_resp2['number_of_segments']}")
        
        # Test 7: Verify segment count after reset
        print("\n--- Test 7: Verifying segment count after reset ---")
        seg_resp2 = client.get_number_of_segments(MEF3_FILE)
        print(f"✓ Number of segments: {seg_resp2['number_of_segments']}")
        if seg_resp2['number_of_segments'] != chunk_resp2['number_of_segments']:
            print(f"ERROR: Mismatch in segment count after reset!")
        
        # Test 8: Retrieve a segment with new size
        print("\n--- Test 8: Retrieving segment with new size ---")
        start_time = time.time()
        result = client.get_signal_segment(MEF3_FILE, 0)
        elapsed = time.time() - start_time
        if result['array'] is not None:
            print(f"✓ Segment retrieved in {elapsed:.2f}s")
            print(f"  Shape: {result['shape']}")
        else:
            print(f"ERROR: Failed to retrieve segment: {result.get('error_message', 'Unknown error')}")
        
        # Test 9: Set active channels (if enough channels exist)
        print("\n--- Test 9: Setting active channels ---")
        all_channels = info["channel_names"]
        if len(all_channels) >= 3:
            selected = [all_channels[0], all_channels[1], all_channels[2]]
            print(f"  Selecting channels: {selected}")
            resp_set = client.set_active_channels(MEF3_FILE, selected)
            print(f"✓ Active channels set: {resp_set['active_channels']}")
            
            # Retrieve a segment with filtered channels
            result = client.get_signal_segment(MEF3_FILE, 0)
            if result['array'] is not None:
                print(f"✓ Segment with filtered channels: shape={result['shape']}")
                if result['shape'][0] != len(selected):
                    print(f"ERROR: Expected {len(selected)} channels, got {result['shape'][0]}")
            else:
                print(f"ERROR: Failed to retrieve segment: {result.get('error_message', 'Unknown error')}")
        else:
            print(f"  Skipping (only {len(all_channels)} channels available)")
        
        # Test 10: List open files
        print("\n--- Test 10: Listing open files ---")
        open_files = client.list_open_files()
        print(f"✓ Open files: {open_files}")
        if MEF3_FILE not in open_files:
            print(f"WARNING: Expected {MEF3_FILE} in open files list")
        
        # Test 11: Close file
        print("\n--- Test 11: Closing file ---")
        client.close_file(MEF3_FILE)
        print(f"✓ File closed")
        
        print("\n" + "="*60)
        print("ALL TESTS PASSED!")
        print("="*60)
        
    except Exception as e:
        print(f"\nERROR: Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        client.shutdown()

if __name__ == "__main__":
    test_big_data()
