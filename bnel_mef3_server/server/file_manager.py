from concurrent import futures
import threading
import numpy as np
import bnel_mef3_server.protobufs.gRPCMef3Server_pb2 as gRPCMef3Server_pb2

from bnel_mef3_server.server.cache import LRUCache
from mef_tools import MefReader
from bnel_mef3_server.server.log_manager import get_logger
from bnel_mef3_server.server.mef_worker import MefWorkerPool
import os

logger = get_logger("bnel_mef3_server.file_manager")


def is_running_in_docker():
    """Detect if running inside a Docker container."""
    return os.path.exists('/.dockerenv')


def get_actual_file_path(file_path):
    """Map the user-supplied absolute file path to the correct on-disk path.
    If running in Docker, prepend /host_root to absolute paths.
    """
    if is_running_in_docker() and os.path.isabs(file_path):
        return '/host_root' + file_path
    return file_path


class FileManager:
    """
    Manages the state and operations for multiple MEF files in a thread-safe manner.

    This class provides efficient, concurrent access to MEF3 files, including:
      - File open/close and info management
      - LRU caching and asynchronous prefetching of signal segments
      - Multi-process parallel reading to work around pymef global variable limitations
      - Chunking of signal data for streaming
      - Active channel selection and order preservation
      - Error handling for invalid requests and file states

    Thread safety is ensured via a lock for all state-changing operations. Each file is managed independently.
    The cache and prefetching system is designed for high-throughput, low-latency access to large files.
    
    Architecture:
      - Main thread: Has a MefReader for metadata and synchronous reads (cache misses)
      - Worker processes (configurable): Each has its own MefReader for parallel prefetching
      - Coordinator thread: Manages prefetch tasks and collects results from workers
    """

    def __init__(self, n_prefetch=2, cache_capacity_multiplier=5, n_process_workers=2):
        """
        Initialize the FileManager with caching and parallel reading support.

        The FileManager coordinates data access across multiple MEF files using a sophisticated
        caching and prefetching system. It employs a multi-process architecture to achieve
        true parallel I/O, working around pymef's global variable limitations.

        **Architecture Overview:**
        
        1. **Main Thread Reader**: 
           - Handles synchronous operations (metadata queries, cache misses)
           - Contains a MefReader instance for immediate data access
           
        2. **Worker Processes** (n_process_workers):
           - Each runs in a separate Python process with isolated MefReader
           - Performs parallel prefetching of future segments
           - Communicates via multiprocessing queues
           
        3. **Coordinator Thread**:
           - Collects results from worker processes
           - Updates the LRU cache with prefetched data
           - Ensures thread-safe cache operations
           
        4. **Fallback Thread Pool**:
           - Handles prefetch task submission when workers unavailable
           - Provides graceful degradation if workers crash
        
        **Caching Strategy:**
        
        The LRU cache stores recently accessed segments with capacity:
        `cache_capacity = n_prefetch + cache_capacity_multiplier`
        
        - `n_prefetch`: Forward-looking segments for sequential access
        - `cache_capacity_multiplier`: Additional slots for backward navigation
        
        When a segment is requested:
        1. Check cache → return immediately if hit
        2. Check if prefetch in progress → wait for completion
        3. Read from disk (main thread) → cache the result
        4. Trigger prefetch for next n_prefetch segments via worker processes
        
        **Prefetch Workflow:**
        
        ```
        Client Request (segment N)
              ↓
        [Cache Hit?] → Yes → Return immediately
              ↓ No
        [Prefetch pending?] → Yes → Wait for worker
              ↓ No
        Load from disk (main thread)
              ↓
        Submit prefetch tasks for segments N+1, N+2, ..., N+n_prefetch
              ↓
        Worker processes read in parallel
              ↓
        Coordinator thread caches results
              ↓
        Future requests hit cache
        ```

        Args:
            n_prefetch (int): Number of segments to prefetch ahead during sequential access.
                Higher values improve streaming performance but use more memory.
                Recommended: 3-5 for sequential access, 1-2 for random access.
                Default: 2
                
            cache_capacity_multiplier (int): Extra cache slots beyond the prefetch window.
                Allows caching of previously accessed segments for backward navigation.
                Total cache capacity = n_prefetch + cache_capacity_multiplier
                Recommended: 5-10 for viewer apps, 3-5 for analysis workflows.
                Default: 5
                
            n_process_workers (int): Number of worker processes for parallel MEF reading.
                Each worker has an independent MefReader, enabling true parallel I/O.
                Set to 0 to disable parallel reading (useful for debugging).
                Recommended: 2-4 for SSD storage, 0-1 for network storage.
                Default: 2

        Example:
            >>> # Sequential streaming with aggressive prefetch
            >>> fm = FileManager(n_prefetch=10, cache_capacity_multiplier=15, n_process_workers=4)
            >>> 
            >>> # Random access with large cache
            >>> fm = FileManager(n_prefetch=2, cache_capacity_multiplier=20, n_process_workers=2)
            >>> 
            >>> # Debug mode (single process)
            >>> fm = FileManager(n_prefetch=3, cache_capacity_multiplier=5, n_process_workers=0)
        """
        self._files = {}
        self._lock = threading.Lock()

        # --- Configuration for caching ---
        self.n_prefetch = n_prefetch  # Number of chunks to prefetch ahead
        # Capacity: n_prefetch ahead + extra for keeping past chunks (backward navigation)
        self.cache_capacity = n_prefetch + cache_capacity_multiplier

        # --- Multi-process worker pool for parallel MEF reading ---
        self.n_process_workers = n_process_workers
        self._worker_pool = None
        if n_process_workers > 0:
            self._worker_pool = MefWorkerPool(n_workers=n_process_workers)
            self._worker_pool.start()
            logger.info(f"Started {n_process_workers} worker processes for parallel MEF reading")
        else:
            logger.info("Running in single-process mode (n_process_workers=0)")

        # --- Coordinator thread for collecting worker results ---
        self._coordinator_thread = None
        self._coordinator_running = False
        if self._worker_pool:
            self._coordinator_running = True
            self._coordinator_thread = threading.Thread(
                target=self._coordinate_worker_results,
                name='WorkerCoordinator',
                daemon=True
            )
            self._coordinator_thread.start()

        # --- Fallback thread pool for prefetch task submission ---
        # Used to submit prefetch tasks to worker pool without blocking main thread
        # Fixed size is sufficient since it only submits tasks, doesn't do I/O
        self._prefetch_executor = futures.ThreadPoolExecutor(
            max_workers=4, thread_name_prefix='prefetch_submit'
        )
        
        # Track in-progress prefetches with Events: {file_path: {chunk_idx: threading.Event}}
        # Events allow waiting for chunk completion
        self._in_progress = {}
        # Track last requested chunk per file for sequential access detection
        self._last_chunk = {}
        # Track task-to-file mapping for worker results
        self._task_file_map = {}  # task_id -> (file_path, chunk_idx)

    def _coordinate_worker_results(self):
        """
        Coordinator thread that collects results from worker processes.
        Runs continuously while the FileManager is active.
        """
        logger.info("Worker coordinator thread started")
        
        while self._coordinator_running:
            if not self._worker_pool:
                break
                
            # Get result with timeout to allow checking coordinator_running flag
            result = self._worker_pool.get_result(timeout=0.1)
            
            if result is None:
                continue
                
            # Process the result
            if result['success']:
                task_id = result['task_id']
                chunk_idx = result['chunk_idx']
                data = result['data']
                worker_id = result['worker_id']
                
                # Find which file this belongs to using the task-file map
                with self._lock:
                    mapping = self._task_file_map.get(task_id)
                    if mapping:
                        file_path, expected_chunk_idx = mapping
                        if file_path in self._files and chunk_idx == expected_chunk_idx:
                            state = self._files[file_path]
                            cache = state['cache']
                            chunks = state['chunks']
                            in_progress = self._in_progress.get(file_path, {})
                            
                            # Verify chunk is still valid
                            if chunks and 0 <= chunk_idx < len(chunks):
                                cache.put(chunk_idx, data)
                                logger.debug(f"Worker {worker_id} completed prefetch of chunk {chunk_idx} for {file_path}")
                            
                            # Mark as complete
                            event = in_progress.get(chunk_idx)
                            if event:
                                event.set()
                                in_progress.pop(chunk_idx, None)
                        
                        # Clean up mapping
                        self._task_file_map.pop(task_id, None)
            else:
                # Handle error
                logger.error(f"Worker {result.get('worker_id')} failed: {result.get('error')}")
                # Mark the task as complete even on error
                task_id = result.get('task_id')
                with self._lock:
                    mapping = self._task_file_map.get(task_id)
                    if mapping:
                        file_path, chunk_idx = mapping
                        in_progress = self._in_progress.get(file_path, {})
                        event = in_progress.get(chunk_idx)
                        if event:
                            event.set()
                            in_progress.pop(chunk_idx, None)
                        self._task_file_map.pop(task_id, None)
                            
        logger.info("Worker coordinator thread exiting")

    # --- NEW: Helper method for background loading ---
    def _load_and_cache_chunk(self, file_path, chunk_idx, use_worker=True):
        """Worker function to load a single chunk and put it in the cache.

        Args:
            file_path (str): Path to the MEF file.
            chunk_idx (int): Index of the chunk to load and cache.
            use_worker (bool): If True, delegate to worker process. If False, use main thread reader.
        """
        # Minimize lock duration - only check and mark as in-progress
        with self._lock:
            # Check if file is still open and chunk isn't already cached
            if file_path not in self._files:
                return
            
            state = self._files[file_path]
            cache = state['cache']
            
            # Quick check: already cached or invalid index
            if chunk_idx in cache:
                return
            
            chunks = state['chunks']
            if not (chunks and 0 <= chunk_idx < len(chunks)):
                return
            
            # --- In-progress event tracking ---
            in_progress = self._in_progress.setdefault(file_path, {})
            if chunk_idx in in_progress:
                # Already being prefetched
                return
            
            # Mark as in progress
            event = threading.Event()
            in_progress[chunk_idx] = event
            
            # Get references we need (outside lock, these are safe to use)
            rdr = state['reader']
            chunk_info = chunks[chunk_idx]
            actual_path = get_actual_file_path(file_path)

        # --- Try to use worker process for parallel reading ---
        if use_worker and self._worker_pool:
            try:
                # Submit to worker pool (reads all channels for flexibility)
                task_id = self._worker_pool.submit_read_task(
                    actual_path,
                    chunk_idx,
                    chunk_info['start'],
                    chunk_info['end'],
                    channels=None  # Load all channels in worker
                )
                
                # Register the task-file mapping for the coordinator (after successful submission)
                with self._lock:
                    self._task_file_map[task_id] = (file_path, chunk_idx)
                
                # The coordinator thread will handle the result
                return
            except Exception as e:
                logger.warning(f"Failed to submit to worker pool: {e}, falling back to main thread")
                # Fall through to main thread reading
                with self._lock:
                    # Clear the in-progress marker since we're going to retry
                    if file_path in self._in_progress:
                        self._in_progress[file_path].pop(chunk_idx, None)
                    event.set()
                # Recursive call with use_worker=False to use main thread
                return self._load_and_cache_chunk(file_path, chunk_idx, use_worker=False)

        # --- Fallback: Data reading in main thread (outside the main lock) ---
        try:
            # Load all channels in prefetch (active channel filtering happens in get_signal_segment)
            # We load all channels here because prefetch happens asynchronously and we don't know which
            # channels will be active when the chunk is requested. This trades memory for flexibility.
            channels = rdr.channels
            data = rdr.get_data(channels, chunk_info['start'], chunk_info['end'])
            data = np.array(data)

            # --- Put loaded data into the cache only if chunk info is still valid ---
            # This prevents stale data from being cached if segment size changed during prefetch
            with self._lock:
                if file_path in self._files:
                    current_chunks = self._files[file_path]['chunks']
                    # Verify the chunk_info we used is still valid
                    if (current_chunks and 0 <= chunk_idx < len(current_chunks) and 
                        current_chunks[chunk_idx] == chunk_info):
                        cache.put(chunk_idx, data)
                        logger.debug(f"Cache PREFETCHED (main thread): chunk {chunk_idx} for {file_path}")
                    else:
                        logger.debug(f"Skipping cache put for chunk {chunk_idx} - segment config changed during prefetch")
        except Exception as e:
            logger.error(f"Error prefetching chunk {chunk_idx} for {file_path}: {e}")
        finally:
            # Signal completion and cleanup
            with self._lock:
                event.set()
                if file_path in self._in_progress:
                    self._in_progress[file_path].pop(chunk_idx, None)

    def open_file(self, file_path):
        """Opens a MEF file and initializes its state.

        Args:
            file_path (str): Path to the MEF file.

        Returns:
            FileInfoResponse: Protobuf response with file info and open status.
        """
        actual_path = get_actual_file_path(file_path)
        if not os.path.exists(actual_path):
            logger.warning(f"Attempted to open non-existent file: {file_path}")
            return gRPCMef3Server_pb2.FileInfoResponse(
                file_path=file_path,
                file_opened=False,
                error_message=f"File does not exist: {file_path}"
            )
        with self._lock:
            if file_path in self._files:
                # File is already open, return info with error message
                info = self._get_file_info_unsafe(file_path)
                info.error_message = f"File already open: {file_path}"
                return info
            try:
                rdr = MefReader(actual_path)
                self._files[file_path] = {
                    'reader': rdr,
                    'chunks': [],
                    'chunk_duration_s': 0,
                    # --- NEW: Initialize a dedicated LRUCache for this file ---
                    'cache': LRUCache(capacity=self.cache_capacity)
                }
                logger.info(f"Opened file: {file_path}")
            except Exception as e:
                logger.error(f"Error opening file {file_path}: {e}")
                return gRPCMef3Server_pb2.FileInfoResponse(
                    file_path=str(file_path),
                    file_opened=False,
                    error_message=str(e)
                )

            # Prefetch initial chunks if chunks are already set (e.g., re-opening)
            state = self._files[file_path]
            if state['chunks']:
                num_to_prefetch = min(self.n_prefetch + 1, len(state['chunks']))
                for idx in range(num_to_prefetch):
                    self._prefetch_executor.submit(self._load_and_cache_chunk, file_path, idx)

            return self._get_file_info_unsafe(file_path)

    def get_signal_segment(self, file_path, chunk_idx):
        """
        Yields signal data for a given segment, streaming in chunks of ~2.5MB.
        Args:
            file_path (str): Path to the MEF file.
            chunk_idx (int): Index of the segment to retrieve.
        Yields:
            SignalChunk: Protobuf message containing a chunk of signal data.
        """
        try:
            with self._lock:
                if file_path not in self._files:
                    yield gRPCMef3Server_pb2.SignalChunk(
                        file_path=file_path,
                        error_message=f"File not open: {file_path}"
                    )
                    return
                state = self._files[file_path]
                rdr = state['reader']
                chunks = state['chunks']
                cache = state['cache']
                in_progress = self._in_progress.setdefault(file_path, {})
                if not chunks:
                    yield gRPCMef3Server_pb2.SignalChunk(
                        file_path=file_path,
                        error_message=f"No chunks available for file: {file_path}. Set a segment size to init chunks."
                    )
                    return
                active_channels = state.get('active_channels')
                if active_channels is None:
                    active_channels = list(rdr.channels)
            if not (chunks and 0 <= chunk_idx < len(chunks)):
                yield gRPCMef3Server_pb2.SignalChunk(
                    file_path=file_path,
                    error_message=f"Invalid chunk request: {chunk_idx} for {file_path}"
                )
                return
            
            # Track last chunk for sequential detection
            last_chunk_idx = self._last_chunk.get(file_path, -1)
            self._last_chunk[file_path] = chunk_idx
            
            # --- Try to get from cache first ---
            data = cache.get(chunk_idx)
            if data is not None:
                # --- CACHE HIT ---
                logger.debug(f"Cache HIT: chunk {chunk_idx} for {file_path}")
                # Filter data to only active channels if needed
                if 'active_channels' in state and state['active_channels'] is not None:
                    all_channels = list(rdr.channels)
                    channel_indices = [all_channels.index(ch) for ch in active_channels]
                    data = data[channel_indices, :]
            else:
                # --- Check if prefetch is in progress ---
                wait_event = None
                with self._lock:
                    if chunk_idx in in_progress:
                        wait_event = in_progress[chunk_idx]
                if wait_event is not None:
                    # Wait for the prefetch to complete
                    logger.debug(f"Waiting for prefetch of chunk {chunk_idx} for {file_path}")
                    wait_event.wait()
                    data = cache.get(chunk_idx)
                    if data is not None:
                        logger.debug(f"Cache HIT after wait: chunk {chunk_idx} for {file_path}")
                        # Filter data to only active channels if needed
                        if 'active_channels' in state and state['active_channels'] is not None:
                            all_channels = list(rdr.channels)
                            channel_indices = [all_channels.index(ch) for ch in active_channels]
                            data = data[channel_indices, :]
                    else:
                        logger.warning(f"Prefetch failed for chunk {chunk_idx} for {file_path}, loading from disk.")
                if data is None:
                    logger.info(f"Cache MISS: chunk {chunk_idx} for {file_path}. Loading from disk.")
                    # --- CACHE MISS (not in progress or prefetch failed) ---
                    try:
                        chunk_info = chunks[chunk_idx]
                        # Load data (allows parallel decompression across multiple threads)
                        data = rdr.get_data(active_channels, chunk_info['start'], chunk_info['end'])
                        data = np.array(data)
                        cache.put(chunk_idx, data)
                    except Exception as e:
                        logger.error(f"Error loading chunk {chunk_idx} for {file_path}: {e}")
                        yield gRPCMef3Server_pb2.SignalChunk(
                            file_path=file_path,
                            error_message=str(e)
                        )
                        return

            # --- Dynamic chunking for ~2.5MB ---
            shape = list(data.shape)
            num_channels = shape[0]
            dtype_size = np.dtype('float64').itemsize
            max_bytes = int(2.5 * 1024 * 1024)  # 2.5MB
            samples_per_chunk = max(int(max_bytes / (num_channels * dtype_size)), 1)
            total_samples = shape[1]
            chunk_info = chunks[chunk_idx]
            chunk_start = int(chunk_info['start'])
            chunk_end = int(chunk_info['end'])

            if active_channels:
                fs = rdr.get_property('fsamp', active_channels[0])
            else:
                fs = rdr.get_property('fsamp')[0]

            for start in range(0, total_samples, samples_per_chunk):
                end = min(start + samples_per_chunk, total_samples)
                tile = data[:, start:end]
                # Calculate tile start/end timestamps
                tile_start = chunk_start + int((start / total_samples) * (chunk_end - chunk_start))
                tile_end = chunk_start + int((end / total_samples) * (chunk_end - chunk_start))
                yield gRPCMef3Server_pb2.SignalChunk(
                    file_path=file_path,
                    array_bytes=tile.tobytes(),
                    dtype='float64',
                    shape=list(tile.shape),
                    start_uutc=tile_start,
                    end_uutc=tile_end,
                    fs=fs,
                    channel_names=active_channels,
                    error_message=""
                )
            
            # --- PREFETCHING: Trigger after serving current chunk ---
            # This happens while client is processing the data
            # Prefetch next n_prefetch chunks for sequential forward access
            with self._lock:
                for i in range(1, self.n_prefetch + 1):
                    future_chunk = chunk_idx + i
                    if future_chunk < len(chunks) and future_chunk not in cache and future_chunk not in in_progress:
                        self._prefetch_executor.submit(self._load_and_cache_chunk, file_path, future_chunk)
                        
        except Exception as e:
            logger.error(f"Unexpected error in get_signal_segment: {e}")
            yield gRPCMef3Server_pb2.SignalChunk(
                file_path=file_path,
                error_message=str(e)
            )

    # --- NEW: Method to gracefully shut down the thread pool ---
    def shutdown(self):
        """Shuts down the background prefetch thread pool executor and worker processes."""
        logger.info("Shutting down FileManager...")
        
        # Stop coordinator thread
        if self._coordinator_thread:
            self._coordinator_running = False
            self._coordinator_thread.join(timeout=2)
        
        # Stop worker pool
        if self._worker_pool:
            self._worker_pool.stop()
            
        # Shutdown thread pool
        self._prefetch_executor.shutdown(wait=False)
        logger.info("FileManager shutdown complete")

    # ... (rest of the FileManager methods: _get_file_info_unsafe, close_file, etc. remain the same) ...
    # Make sure to also add a shutdown method.
    def _get_file_info_unsafe(self, file_path):
        """Internal helper to get file info. Assumes lock is already held.

        Args:
            file_path (str): Path to the MEF file.

        Returns:
            FileInfoResponse: Protobuf response with file info and open status.
        """
        if file_path not in self._files:
            return gRPCMef3Server_pb2.FileInfoResponse(
                file_path=file_path,
                file_opened=False
            )

        state = self._files[file_path]
        rdr = state['reader']
        fs = rdr.get_property('fsamp')
        ch_names = rdr.channels
        nch = len(ch_names)
        start_uutc = min(rdr.get_property('start_time'))
        end_uutc = max(rdr.get_property('end_time'))
        duration_s = (end_uutc - start_uutc) / 1e6

        return gRPCMef3Server_pb2.FileInfoResponse(
            file_path=file_path,
            file_opened=True,
            number_of_channels=nch,
            channel_names=ch_names,
            channel_sampling_rates=fs,
            start_uutc=start_uutc,
            end_uutc=end_uutc,
            duration_s=duration_s,
        )

    def close_file(self, file_path):
        """Closes an open MEF file and cleans up its resources.

        Args:
            file_path (str): Path to the MEF file.

        Returns:
            FileInfoResponse: Protobuf response indicating the file is closed.
        """
        with self._lock:
            try:
                if file_path in self._files:
                    # Clean up resources if necessary (e.g., rdr.close())
                    del self._files[file_path]
                    # Clean up in-progress events for this file
                    self._in_progress.pop(file_path, None)
                    logger.info(f"Closed and removed file: {file_path}")
                
                # Notify worker processes to close their readers for this file
                if self._worker_pool:
                    self._worker_pool.close_file(file_path)
                    
                return gRPCMef3Server_pb2.FileInfoResponse(
                    file_path=file_path,
                    file_opened=False,
                    error_message=""
                )
            except Exception as e:
                logger.error(f"Error closing file {file_path}: {e}")
                return gRPCMef3Server_pb2.FileInfoResponse(
                    file_path=file_path,
                    file_opened=False,
                    error_message=str(e)
                )

    def get_file_info(self, file_path):
        """Gets information about an open MEF file.

        Args:
            file_path (str): Path to the MEF file.

        Returns:
            FileInfoResponse: Protobuf response with file info and open status.
        """
        with self._lock:
            try:
                return self._get_file_info_unsafe(file_path)
            except Exception as e:
                logger.error(f"Error getting file info for {file_path}: {e}")
                return gRPCMef3Server_pb2.FileInfoResponse(
                    file_path=file_path,
                    file_opened=False,
                    error_message=str(e)
                )

    def set_signal_segment_size(self, file_path, seconds):
        """Sets the segment size for signal data and updates segment metadata.

        Args:
            file_path (str): Path to the MEF file.
            seconds (float): Duration of each segment in seconds.

        Returns:
            SetSignalSegmentResponse: Protobuf response with the number of segments.
        """
        with self._lock:
            if file_path not in self._files:
                logger.warning(f"set_signal_segment_size: file not open: {file_path}")
                return gRPCMef3Server_pb2.SetSignalSegmentResponse(
                    file_path=file_path,
                    number_of_segments=0,
                    error_message=f"File not open: {file_path}"
                )

            try:
                state = self._files[file_path]
                rdr = state['reader']
                start_uutc = min(rdr.get_property('start_time'))
                end_uutc = max(rdr.get_property('end_time'))
                segment_starts = np.arange(start_uutc, end_uutc, seconds * 1e6)
                segments = []
                for s in segment_starts:
                    seg_end = min(s + seconds * 1e6, end_uutc)
                    segments.append({'start': s, 'end': seg_end})
                # Ensure last segment is included even if shorter
                if not segments or segments[-1]['end'] < end_uutc:
                    if segments:
                        last_start = segments[-1]['end']
                    else:
                        last_start = start_uutc
                    segments.append({'start': last_start, 'end': end_uutc})
                
                # Clear the cache and in-progress tracking when segment size changes to avoid stale data
                cache = state['cache']
                cache.clear()
                # Clear in-progress prefetches for this file
                if file_path in self._in_progress:
                    self._in_progress[file_path].clear()
                logger.debug(f"Cleared cache and in-progress prefetches for {file_path} due to segment size change")
                
                state['chunk_duration_s'] = seconds
                state['chunks'] = segments
                if segments:
                    logger.info(f"Set segment size to {seconds}s for {file_path}, total segments: {len(segments)}")
                    # Eagerly prefetch the first n_prefetch chunks
                    num_to_prefetch = min(self.n_prefetch + 1, len(segments))
                    for idx in range(num_to_prefetch):
                        self._prefetch_executor.submit(self._load_and_cache_chunk, file_path, idx)

                return gRPCMef3Server_pb2.SetSignalSegmentResponse(
                    file_path=file_path,
                    number_of_segments=len(segments),
                    error_message=""
                )
            except Exception as e:
                logger.error(f"Error in set_signal_segment_size for {file_path}: {e}")
                return gRPCMef3Server_pb2.SetSignalSegmentResponse(
                    file_path=file_path,
                    number_of_segments=0,
                    error_message=str(e)
                )

    def list_open_files(self):
        """Lists all currently open MEF files.

        Returns:
            list: List of file paths for open files.
        """
        with self._lock:
            return list(self._files.keys())

    def set_active_channels(self, file_path, channel_names):
        with self._lock:
            if file_path not in self._files:
                return gRPCMef3Server_pb2.SetActiveChannelsResponse(
                    file_path=file_path,
                    active_channels=[],
                    error_message=f"File not open: {file_path}"
                )
            state = self._files[file_path]
            rdr = state['reader']
            all_channels = set(rdr.channels)
            requested = list(channel_names)
            valid = [ch for ch in requested if ch in all_channels]
            invalid = [ch for ch in requested if ch not in all_channels]
            if not requested:
                # Default to all channels
                state['active_channels'] = list(rdr.channels)
                return gRPCMef3Server_pb2.SetActiveChannelsResponse(
                    file_path=file_path,
                    active_channels=state['active_channels'],
                    error_message=""
                )
            if not valid:
                # None of the requested channels are valid, keep previous setup
                prev = state.get('active_channels', list(rdr.channels))
                return gRPCMef3Server_pb2.SetActiveChannelsResponse(
                    file_path=file_path,
                    active_channels=prev,
                    error_message=f"No valid channels in request. Invalid: {invalid}"
                )
            # Set only valid channels
            state['active_channels'] = valid
            err_msg = f"Some channels do not exist: {invalid}" if invalid else ""
            return gRPCMef3Server_pb2.SetActiveChannelsResponse(
                file_path=file_path,
                active_channels=valid,
                error_message=err_msg
            )

    def get_active_channels(self, file_path):
        with self._lock:
            if file_path not in self._files:
                return gRPCMef3Server_pb2.GetActiveChannelsResponse(
                    file_path=file_path,
                    active_channels=[],
                    error_message=f"File not open: {file_path}"
                )
            state = self._files[file_path]
            active = state.get('active_channels')
            if active is None:
                # Default to all channels
                active = list(state['reader'].channels)
                state['active_channels'] = active
            return gRPCMef3Server_pb2.GetActiveChannelsResponse(
                file_path=file_path,
                active_channels=active,
                error_message=""
            )
