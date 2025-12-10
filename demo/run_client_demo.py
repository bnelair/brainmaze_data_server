"""
Demo: Using the Mef3Client to interact with a running MEF3 gRPC server (in Docker).

This script demonstrates opening a file, setting chunk size, selecting active channels,
retrieving signal data, and closing the file using the high-level Python client.

Assumes the server is running in Docker and demo/test_file.mefd exists.
"""
from bnel_mef3_server.client import Mef3Client
import numpy as np
import os

# Path to the demo MEF3 file (should exist in the container or be mounted)
MEF3_FILE = os.path.abspath(os.path.join(os.path.dirname(__file__), "test_file.mefd"))
MEF3_FILE = '/mnt/Beryllium/filip/bnel-mef3-server/demo/test_file.mefd'
print(MEF3_FILE)

# Connect to the server (default Docker port)
client = Mef3Client("10.144.10.74:50051")

print("Opening file:", MEF3_FILE)
info = client.open_file(MEF3_FILE)
print("File info:", info)

print("Setting chunk size to 60 seconds...")
chunk_resp = client.set_signal_chunk_size(MEF3_FILE, 60)
print("Chunk response:", chunk_resp)

print("Getting all channel names...")
all_channels = info["channel_names"]
print("All channels:", all_channels)

# Select a subset of channels (if at least 3 exist)
if len(all_channels) >= 3:
    selected = [all_channels[2], all_channels[0], all_channels[1]]
    print(f"Setting active channels to: {selected}")
    resp_set = client.set_active_channels(MEF3_FILE, selected)
    print("Set active channels response:", resp_set)
    print("Getting active channels...")
    resp_get = client.get_active_channels(MEF3_FILE)
    print("Active channels:", resp_get["active_channels"])
else:
    print("Not enough channels to demonstrate active channel selection.")

print("Retrieving first chunk of signal data...")
for i, arr in enumerate(client.get_signal_segment(MEF3_FILE, 0)):
    print(f"Tile {i}: shape={arr.shape}, dtype={arr.dtype}")
    if i == 0:
        print("First tile data (truncated):", arr.flatten()[:10])

print("Listing open files:")
print(client.list_open_files())

print("Closing file...")
client.close_file(MEF3_FILE)

client.shutdown()
print("Demo complete.")
