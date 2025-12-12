"""Entrypoint for running the gRPC MEF3 server as a Python module.

This script reads configuration from environment variables and launches the gRPC server.
"""
import os
import signal
import sys
import time
from bnel_mef3_server.server.mef3_server import gRPCMef3ServerHandler

print("[MEF3 SERVER] Starting server from __main__.py...")


def main():
    """Main entrypoint for the gRPC MEF3 server.

    Reads configuration from environment variables and starts the server.
    Handles graceful shutdown on SIGTERM/SIGINT.
    """
    # Read configuration from environment variables (with defaults)
    port = int(os.environ.get("PORT", 50051))
    n_prefetch = int(os.environ.get("N_PREFETCH", 3))
    cache_capacity_multiplier = int(os.environ.get("CACHE_CAPACITY_MULTIPLIER", 3))
    n_process_workers = int(os.environ.get("N_PROCESS_WORKERS", 2))

    print(f"Starting gRPC MEF3 server on port {port} with FileManager config: n_prefetch={n_prefetch}, cache_capacity_multiplier={cache_capacity_multiplier}, n_process_workers={n_process_workers}")

    handler = gRPCMef3ServerHandler(
        port=port,
        n_prefetch=n_prefetch,
        cache_capacity_multiplier=cache_capacity_multiplier,
        n_process_workers=n_process_workers
    )

    def handle_sigterm(signum, frame):
        """Handles SIGTERM/SIGINT for graceful shutdown."""
        print("Received termination signal, shutting down...")
        handler.stop()
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        handle_sigterm(None, None)


if __name__ == "__main__":
    main()
