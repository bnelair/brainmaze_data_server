# bnel-mef3-server

A gRPC server for efficient, concurrent access to MEF3 (Multiscale Electrophysiology Format) files, with LRU caching and background prefetching. Designed for scalable neurophysiology data streaming and analysis.

## Features
- gRPC API for remote MEF3 file access
- Thread-safe LRU cache for signal chunks
- Asynchronous prefetching for low-latency streaming
- Configurable via environment variables or Docker
- Ready for deployment in Docker and CI/CD pipelines

## Installation

### Requirements
- Python 3.8+
- `pip install -r requirements.txt`
- (Optional) Docker for containerized deployment

### Local Setup
Clone the repository and install dependencies:
```sh
pip install -r requirements.txt
```

### Docker
Build and run the server in a container:
```sh
docker build -t bnel-mef3-server .
docker run -e PORT=50051 -p 50051:50051 bnel-mef3-server
```

## Usage

### As a Python Module
Run the server with configurable options:
```sh
python -m bnel_mef3_server
```

#### Configuration via Environment Variables
- `PORT`: gRPC server port (default: 50051)
- `N_PREFETCH`: Number of chunks to prefetch (default: 3)
- `CACHE_CAPACITY_MULTIPLIER`: Extra cache slots (default: 3)
- `MAX_WORKERS`: Prefetch thread pool size (default: 4)

Example:
```sh
PORT=50052 N_PREFETCH=2 python -m bnel_mef3_server
```

### As a Docker Container
```sh
docker run -e PORT=50051 -e N_PREFETCH=2 -p 50051:50051 bnel-mef3-server
```

## Python Usage Examples

### Launching the Server from Python
You can launch the gRPC server directly from Python by importing and running the server class:

```python
from bnel_mef3_server.server.mef3_server import gRPCMef3Server
from bnel_mef3_server.server.file_manager import FileManager
import grpc
from concurrent import futures

# Configure file manager (optional arguments: n_prefetch, cache_capacity_multiplier, max_workers)
file_manager = FileManager(n_prefetch=3, cache_capacity_multiplier=3, max_workers=4)

# Create the gRPC server and add the MEF3 service
server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
servicer = gRPCMef3Server(file_manager)
from bnel_mef3_server.protobufs.gRPCMef3Server_pb2_grpc import add_gRPCMef3ServerServicer_to_server
add_gRPCMef3ServerServicer_to_server(servicer, server)

# Start the server
port = 50052
server.add_insecure_port(f"[::]:{port}")
server.start()
print(f"Server started on port {port}")
server.wait_for_termination()
```

### Using the Python Client
The package provides a high-level client for interacting with the server:

```python
from bnel_mef3_server.client import Mef3Client

client = Mef3Client("localhost:50052")

# Open a file
info = client.open_file("/path/to/file.mefd")
print("Opened file:", info)

# Set chunk size (in seconds)
client.set_signal_chunk_size("/path/to/file.mefd", 60)

# Set active channels (optional)
client.set_active_channels("/path/to/file.mefd", ["Ch1", "Ch2"])  # Use your channel names

# Get a chunk of signal data (as numpy arrays)
for arr in client.get_signal_segment("/path/to/file.mefd", chunk_idx=0):
    print(arr.shape)

# List open files
print(client.list_open_files())

# Close the file
client.close_file("/path/to/file.mefd")

client.shutdown()
```

See the [API section](#api) and the Python docstrings for more details on each method.

## API
The server exposes a gRPC API. See `bnel_mef3_server/protobufs/gRPCMef3Server.proto` for service and message definitions.

## Development

### Running Tests
All tests are in the `tests/` directory and use `pytest`:
```sh
pytest
```

### Linting
You can use any linter compatible with Google-style docstrings (e.g., `pylint`, `flake8`).

### Building Documentation
This project uses Sphinx with Google-style docstrings for API documentation.

To build the documentation locally:
```sh
# Install documentation dependencies
pip install -e ".[docs]"

# Build HTML documentation
cd docs
make html

# Or use sphinx-build directly
sphinx-build -b html docs/ docs/_build/html
```

The built documentation will be available at `docs/_build/html/index.html`.

#### Online Documentation
Documentation is automatically built and deployed to GitHub Pages on every push to the `main` branch. The documentation is available at: https://bnelair.github.io/brainmaze_data_server/

The deployment uses GitHub Actions (see `.github/workflows/docs.yml`) and publishes to the `gh-pages` branch.

## CI/CD
Documentation is automatically built and deployed to GitHub Pages on every push to the `main` branch via GitHub Actions (see `.github/workflows/docs.yml`).

## License
Specify your license here.

## Authors
See Git history for contributors.
