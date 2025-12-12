# bnel-mef3-server

A gRPC server for efficient, concurrent access to MEF3 (Multiscale Electrophysiology Format) files, with LRU caching, multi-process parallel reading, and background prefetching. Designed for scalable neurophysiology data streaming and analysis.

## Features
- gRPC API for remote MEF3 file access
- Thread-safe LRU cache for signal chunks
- **Multi-process parallel reading** to work around pymef global variable limitations
- Asynchronous prefetching for low-latency streaming
- Configurable via environment variables or Docker
- Ready for deployment in Docker and CI/CD pipelines

## Architecture

The server uses a sophisticated multi-process architecture to achieve parallel MEF reading:

- **Main Process**: Contains a MefReader instance in a separate thread for metadata operations and cache misses
- **Worker Processes** (configurable, default: 2): Each has its own MefReader instance for parallel prefetching
- **Coordinator Thread**: Manages prefetch tasks and collects results from worker processes
- **Thread Pool**: Fallback for non-worker operations and metadata queries

This architecture works around pymef's global variable limitations, enabling true parallel I/O on SSD storage.

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
- `N_PREFETCH`: Number of chunks to prefetch ahead (default: 3)
- `CACHE_CAPACITY_MULTIPLIER`: Extra cache slots beyond prefetch window (default: 3)
- `N_PROCESS_WORKERS`: Number of worker processes for parallel MEF reading (default: 2)

Example:
```sh
PORT=50052 N_PREFETCH=5 N_PROCESS_WORKERS=4 python -m bnel_mef3_server
```

**Performance Tuning Guidelines:**
- **For SSD storage**: Use `N_PROCESS_WORKERS=2-4` to enable parallel I/O
- **For sequential access patterns**: Increase `N_PREFETCH` (e.g., 5-10) for smoother streaming
- **For random access**: Keep `N_PREFETCH` lower (1-3) and increase `CACHE_CAPACITY_MULTIPLIER`
- **Single-process mode**: Set `N_PROCESS_WORKERS=0` to disable parallel reading (useful for debugging)

### As a Docker Container
```sh
docker run -e PORT=50051 -e N_PREFETCH=2 -p 50051:50051 bnel-mef3-server
```

## Python Usage Examples

### Launching the Server from Python
You can launch the gRPC server directly from Python by importing and running the server class:

```python
from bnel_mef3_server.server.mef3_server import gRPCMef3ServerHandler

# Configure and start server with parallel reading enabled
handler = gRPCMef3ServerHandler(
    port=50052,
    n_prefetch=5,                    # Prefetch 5 segments ahead
    cache_capacity_multiplier=10,     # Keep 10 extra segments in cache
    n_process_workers=2               # Use 2 worker processes for parallel reading
)

print(f"Server started with parallel reading support")
# Server runs in background, call handler.stop() to stop it
```

Alternatively, using FileManager directly:

```python
from bnel_mef3_server.server.mef3_server import gRPCMef3Server
from bnel_mef3_server.server.file_manager import FileManager
import grpc
from concurrent import futures

# Configure file manager with parallel workers
file_manager = FileManager(
    n_prefetch=3, 
    cache_capacity_multiplier=3, 
    n_process_workers=2  # Enable parallel reading
)

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

## Caching and Prefetching

The server implements a sophisticated LRU (Least Recently Used) caching system with intelligent prefetching to minimize latency and maximize throughput.

### How Caching Works

**Cache Capacity:**
```
Total Cache Size = n_prefetch + cache_capacity_multiplier
```

- `n_prefetch`: Forward-looking cache (upcoming segments)
- `cache_capacity_multiplier`: Backward-looking cache (previously accessed segments)

**Cache Behavior:**

1. **First Access** (Cold Start):
   - Segment loaded from disk (main thread)
   - Next `n_prefetch` segments queued for prefetch
   - Worker processes load segments in parallel
   
2. **Subsequent Access** (Cache Hit):
   - Segment returned immediately from cache (<1ms)
   - Next `n_prefetch` segments prefetched if not already cached

3. **Cache Eviction**:
   - Follows LRU policy when cache is full
   - Least recently used segments removed first

### Prefetching Strategy

The server uses a **sequential access optimization** strategy:

```
Current Request: Segment N
         ↓
    Cache Check
         ↓
   [Hit] → Return immediately
         ↓
   [Miss] → Load from disk
         ↓
    Trigger Prefetch: N+1, N+2, ..., N+n_prefetch
         ↓
    Worker Processes read in parallel
         ↓
    Future requests hit cache
```

### Configuration Examples

#### Scenario 1: Sequential Video Viewer
**Use Case**: User paging forward through EEG data  
**Optimization**: Aggressive prefetching, moderate cache

```python
fm = FileManager(
    n_prefetch=10,              # Prefetch 10 segments ahead
    cache_capacity_multiplier=15,  # Keep 15 past segments
    n_process_workers=4         # Use 4 workers for faster prefetch
)
# Total cache: 25 segments
# Perfect for smooth forward/backward navigation
```

#### Scenario 2: Random Access Analysis
**Use Case**: Algorithm jumping between different time points  
**Optimization**: Large cache, minimal prefetch

```python
fm = FileManager(
    n_prefetch=2,               # Only prefetch 2 ahead
    cache_capacity_multiplier=20,  # Keep 20 recent segments
    n_process_workers=2         # Standard parallelism
)
# Total cache: 22 segments
# Optimized for revisiting recent segments
```

#### Scenario 3: Single-Pass Processing
**Use Case**: Export or batch processing (forward-only)  
**Optimization**: Maximum prefetch, minimal backward cache

```python
fm = FileManager(
    n_prefetch=15,              # Aggressive prefetch
    cache_capacity_multiplier=5,   # Minimal backward cache
    n_process_workers=4         # Maximum parallel I/O
)
# Total cache: 20 segments
# Optimized for maximum throughput
```

#### Scenario 4: Debug Mode
**Use Case**: Development and debugging  
**Optimization**: Disable parallelism for simpler debugging

```python
fm = FileManager(
    n_prefetch=3,               # Moderate prefetch
    cache_capacity_multiplier=5,   # Small cache
    n_process_workers=0         # Single process (no workers)
)
# Easier to debug without multi-process complexity
```

### Performance Characteristics

**Benchmark Results** (from `tests/test_access_patterns.py`):

Test data specifications:
- **Dataset**: 2 hours of continuous EEG data
- **Channels**: 64 channels  
- **Sampling Rate**: 256 Hz
- **MEF Compression**: Precision level 2
- **Segment Size**: 5 minutes (24 total segments)
- **Test Configuration**: 10 segments with 0.3s processing delay between reads

Performance comparison (total time for 10 segments):
| Access Pattern | Time | vs Baseline | Description |
|----------------|------|-------------|-------------|
| **Concurrent (3 clients)** | 3.6s | **+113%** | 3 clients reading different segments simultaneously |
| **With Prefetch** | 6.7s | **+15%** | Sequential access with n_prefetch=1, n_process_workers=2 |
| **Baseline (Direct MefReader)** | 7.7s | — | Direct mef_tools access, no server overhead |
| **Without Prefetch** | 9.2s | **-19%** | Sequential access with no prefetching |

Key findings:
- Concurrent access provides **2.5x speedup** over no-prefetch mode
- Prefetching provides **37% speedup** over no-prefetch mode  
- Server with prefetching is **15% faster** than direct MefReader baseline
- The multi-process architecture enables true parallel I/O despite pymef limitations

**Cache Hit Performance:**
- In-memory access: <1ms per segment
- No I/O or decompression overhead

**Cache Miss Performance (Cold Start):**
- Disk read + decompression: ~200-500ms (depends on segment size and channels)
- Worker prefetch overlaps with client processing
- Subsequent accesses hit cache

**Prefetch Effectiveness:**
With `n_prefetch=5` and 0.3s client processing:
- Read time: ~300ms per segment
- Client processing: ~300ms
- Result: Prefetch completes before next request → 100% cache hit rate

### Memory Considerations

**Memory per Cached Segment:**
```
Memory ≈ segment_duration × num_channels × sampling_rate × 8 bytes
```

Example: 60s segment, 64 channels, 256 Hz:
```
60 × 64 × 256 × 8 = 78.6 MB per segment
```

With cache capacity of 20 segments:
```
20 × 78.6 MB = 1.57 GB total cache memory
```

**Recommendations:**
- For systems with <4GB RAM: `cache_capacity ≤ 10`
- For systems with 8-16GB RAM: `cache_capacity = 15-25`
- For systems with >16GB RAM: `cache_capacity = 30-50`

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
