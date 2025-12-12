# Multi-Process Parallel Reading Architecture

## Overview

The MEF3 server now implements a multi-process architecture to enable true parallel reading of MEF files. This addresses a critical limitation in pymef (the underlying MEF library) which uses global variables that prevent concurrent reads within the same process.

## Problem Statement

The original issue was that pymef uses global variables internally, which means:
- Multiple threads in the same process cannot read MEF files in parallel
- Even with threading, reads are effectively serialized
- This becomes a bottleneck when serving data from fast storage (SSD)
- Prefetching couldn't achieve its full potential due to this limitation

## Solution: Multi-Process Architecture

### Architecture Components

1. **Main Process**
   - Runs the gRPC server and FileManager
   - Contains a MefReader instance for metadata queries and synchronous operations
   - Handles cache management and coordination

2. **Worker Processes (N configurable, default: 2)**
   - Each runs in a separate Python process
   - Each has its own isolated MefReader instance
   - Processes prefetch tasks in parallel
   - Communicate via multiprocessing queues

3. **Coordinator Thread**
   - Runs in the main process
   - Collects results from worker processes
   - Updates the cache with prefetched data
   - Manages task completion tracking

4. **Thread Pool Executor**
   - Fallback for tasks when workers are busy
   - Used for metadata operations
   - Provides backward compatibility

### Data Flow

```
Client Request
    ↓
Main Process (FileManager)
    ↓
[Cache Hit?] → Yes → Return data immediately
    ↓ No
[Prefetch in progress?] → Yes → Wait for completion
    ↓ No
Read from disk (main thread)
    ↓
Trigger prefetch for next N segments
    ↓
Submit tasks to Worker Pool
    ↓
Worker Processes read data in parallel
    ↓
Coordinator Thread collects results
    ↓
Cache updated for future requests
```

## Implementation Details

### Worker Process Management (`mef_worker.py`)

- **MefWorkerProcess**: Individual worker process
  - Maintains its own MefReader instances (cached per file)
  - Processes READ and CLOSE_FILE commands
  - Sends results back via result queue

- **MefWorkerPool**: Manages all worker processes
  - Distributes tasks using a shared task queue
  - Collects results via a shared result queue
  - Handles graceful startup and shutdown

### FileManager Integration

The FileManager has been enhanced with:
- `_coordinate_worker_results()`: Coordinator thread that processes worker results
- `_load_and_cache_chunk()`: Updated to delegate to workers when available
- Chunk-to-file mapping for result correlation
- Graceful fallback to main thread if workers fail

### Configuration

Environment variables:
- `N_PROCESS_WORKERS`: Number of worker processes (default: 2)
  - Set to 0 to disable parallel reading (single-process mode)
  - Recommended: 2-4 for SSD storage
  - Higher values provide more parallelism but use more memory

Python API:
```python
from bnel_mef3_server.server.file_manager import FileManager

fm = FileManager(
    n_prefetch=5,              # Prefetch 5 segments ahead
    cache_capacity_multiplier=10,  # Extra cache capacity
    max_workers=4,             # Thread pool size
    n_process_workers=2        # Worker processes for parallel reading
)
```

## Performance Characteristics

### Benefits
- **True Parallel I/O**: Multiple MEF files can be read simultaneously
- **Better SSD Utilization**: Takes advantage of SSD's parallel read capabilities
- **Reduced Latency**: Prefetching happens faster with parallel workers
- **Scalable**: Can adjust worker count based on workload

### Trade-offs
- **Memory Usage**: Each worker maintains its own MefReader instance
- **Process Overhead**: Process creation and IPC have some overhead
- **Complexity**: More complex than pure threading

### Benchmark Results

Comprehensive benchmarks from `tests/test_access_patterns.py`:

**Test Data Specifications:**
- Duration: 2 hours continuous EEG
- Channels: 64
- Sampling Rate: 256 Hz
- MEF Compression: Precision level 2
- Segment Size: 5 minutes (24 total segments)
- Test Parameters: 10 segments, 0.3s processing delay

**Performance Results:**
- **Concurrent access (3 clients)**: 3.6s - Best performance (2.54x faster than no-prefetch)
- **Sequential with prefetch**: 6.7s - 15% faster than baseline
- **Baseline (Direct MefReader)**: 7.7s - Reference point
- **Sequential without prefetch**: 9.2s - 20% slower than baseline

**Key Findings:**
- Concurrent access provides **113% improvement** over baseline
- Prefetching provides **37% improvement** over no-prefetch mode
- Multi-process architecture successfully enables parallel I/O
- Server overhead is minimal - actually faster than direct access with prefetching

## Design Decisions

### Why multiprocessing.Queue over Redis?
- **Simplicity**: No external dependencies
- **Performance**: In-process communication is faster for our use case
- **Maintainability**: Easier to deploy and debug
- **No Network**: Avoids network serialization overhead

### Why separate processes instead of threads?
- **GIL Limitation**: Python's GIL prevents true parallel CPU work in threads
- **Library Constraints**: pymef's global variables make threading ineffective
- **True Parallelism**: Processes provide real parallel execution

### Why coordinator thread instead of async?
- **Blocking Operations**: MefReader operations are synchronous
- **Integration**: Easier to integrate with existing threaded architecture
- **Reliability**: Simpler error handling and task tracking

## Future Enhancements

Possible improvements for even better performance:
1. **Predictive Prefetching**: Analyze access patterns to prefetch smarter
2. **Dynamic Worker Scaling**: Adjust worker count based on load
3. **Shared Memory**: Use shared memory for larger data transfers
4. **Worker Specialization**: Dedicate workers to specific files
5. **Metrics and Monitoring**: Add detailed performance metrics

## Testing

The implementation includes:
- Unit tests for worker processes
- Integration tests for FileManager with workers
- Benchmark tests comparing performance
- Graceful degradation tests (worker failure scenarios)

All existing tests pass with the new architecture, ensuring backward compatibility.

## Migration Guide

For existing deployments:

1. **No code changes required** for existing clients
2. **Optional configuration**: Set `N_PROCESS_WORKERS` environment variable
3. **Default behavior**: 2 workers enabled by default (can disable with `N_PROCESS_WORKERS=0`)
4. **Backward compatible**: Works exactly like before if workers disabled

## Troubleshooting

### Fork Warnings
You may see warnings about forking in multi-threaded processes. These are expected and can be safely ignored in most cases. The architecture is designed to handle this scenario.

### Worker Crashes
If a worker crashes, the system falls back to the main thread reader. Check logs for worker error messages.

### Memory Usage
If memory usage is high, consider:
- Reducing `N_PROCESS_WORKERS`
- Reducing `CACHE_CAPACITY_MULTIPLIER`
- Reducing `N_PREFETCH`

## Conclusion

The multi-process parallel reading architecture successfully addresses pymef's global variable limitations, enabling true parallel MEF reading. This results in improved performance, especially for sequential access patterns on fast storage, while maintaining backward compatibility and graceful degradation.
