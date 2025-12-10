#!/bin/bash
set -e

# Optional: print environment info for debugging
echo "Starting bnel-mef3-server..."
echo "Python version: $(python --version)"
echo "Working directory: $(pwd)"

# Launch the gRPC server
exec python -u -m bnel_mef3_server.server

