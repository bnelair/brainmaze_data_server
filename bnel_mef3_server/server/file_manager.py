from concurrent import futures
import threading
import numpy as np
import bnel_mef3_server.protobufs.gRPCMef3Server_pb2 as gRPCMef3Server_pb2

from bnel_mef3_server.server.cache import LRUCache
from mef_tools import MefReader
from bnel_mef3_server.server.log_manager import get_logger
import os

logger = get_logger("bnel_mef3_server.file_manager")


def is_running_in_docker():
    """Detect if running inside a Docker container."""
    in_docker = os.path.exists('/.dockerenv')
    logger.debug(f"Docker detection: {'Running in Docker' if in_docker else 'Not running in Docker'}")
    return in_docker


def get_actual_file_path(file_path):
    """Map the user-supplied absolute file path to the correct on-disk path.
    If running in Docker, prepend /host_root to absolute paths.
    """
    if is_running_in_docker() and os.path.isabs(file_path):
        mapped_path = '/host_root' + file_path
        logger.debug(f"Docker path mapping: {file_path} -> {mapped_path}")
        return mapped_path
    return file_path


class FileManager:
    """
    Manages the state and operations for multiple MEF files in a thread-safe manner.

    This class provides efficient, concurrent access to MEF3 files, including:
      - File open/close and info management
      - LRU caching and asynchronous prefetching of signal segments
      - Chunking of signal data for streaming
      - Active channel selection and order preservation
      - Error handling for invalid requests and file states

    Thread safety is ensured via a lock for all state-changing operations. Each file is managed independently.
    The cache and prefetching system is designed for high-throughput, low-latency access to large files.
    """

    def __init__(self, n_prefetch=2, cache_capacity_multiplier=5, max_workers=4):
        """
        Initialize the FileManager.

        Args:
            n_prefetch (int): Number of chunks to prefetch before and after each request.
            cache_capacity_multiplier (int): Additional cache capacity beyond the prefetch window.
            max_workers (int): Maximum number of background threads for prefetching.
        """
        self._files = {}
        self._lock = threading.Lock()

        # --- NEW: Configuration for caching ---
        self.n_prefetch = n_prefetch  # Number of chunks to prefetch before and after
        self.cache_capacity = (n_prefetch * 2) + cache_capacity_multiplier

        # --- NEW: Dedicated thread pool for background data loading ---
        self._prefetch_executor = futures.ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix='cache_prefetch'
        )
        # Track in-progress prefetches: {file_path: {chunk_idx: threading.Event}}
        self._in_progress = {}

    # --- NEW: Helper method for background loading ---
    def _load_and_cache_chunk(self, file_path, chunk_idx):
        """Worker function to load a single chunk and put it in the cache.

        Args:
            file_path (str): Path to the MEF file.
            chunk_idx (int): Index of the chunk to load and cache.
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

        # --- Data reading happens outside the main lock ---
        try:
            channels = rdr.channels
            start_time = chunk_info['start']
            end_time = chunk_info['end']
            
            # Validate time range against file bounds
            file_start = min(rdr.get_property('start_time'))
            file_end = max(rdr.get_property('end_time'))
            
            if start_time < file_start or end_time > file_end:
                logger.warning(f"Chunk {chunk_idx} has invalid time range [{start_time}, {end_time}] for file bounds [{file_start}, {file_end}] in {file_path}")
                # Clamp to valid range
                start_time = max(start_time, file_start)
                end_time = min(end_time, file_end)
                
                # Check if clamping resulted in invalid range
                if start_time >= end_time:
                    logger.error(f"Chunk {chunk_idx} has completely invalid time range after clamping: [{start_time}, {end_time}] in {file_path}")
                    return
            
            data = rdr.get_data(channels, start_time, end_time)
            data = np.array(data)

            # --- Put loaded data into the cache ---
            cache.put(chunk_idx, data)
            logger.debug(f"Cache PREFETCHED: chunk {chunk_idx} for {file_path}")
        except Exception as e:
            # Use exc_info only for DEBUG level to avoid excessive logging
            logger.error(f"Error prefetching chunk {chunk_idx} for {file_path}: {e}")
            logger.debug(f"Prefetch error details:", exc_info=True)
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
        logger.info(f"Opening file: {file_path}")
        actual_path = get_actual_file_path(file_path)
        logger.debug(f"Actual file path (after Docker mapping): {actual_path}")
        
        if not os.path.exists(actual_path):
            logger.warning(f"Attempted to open non-existent file: {file_path} (actual: {actual_path})")
            return gRPCMef3Server_pb2.FileInfoResponse(
                file_path=file_path,
                file_opened=False,
                error_message=f"File does not exist: {file_path}"
            )
        with self._lock:
            if file_path in self._files:
                # File is already open, return info with error message
                logger.warning(f"File already open: {file_path}")
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
                logger.info(f"Successfully opened file: {file_path} ({len(rdr.channels)} channels)")
            except Exception as e:
                logger.error(f"Error opening file {file_path}: {e}", exc_info=True)
                return gRPCMef3Server_pb2.FileInfoResponse(
                    file_path=str(file_path),
                    file_opened=False,
                    error_message=str(e)
                )

            # Prefetch initial chunks if chunks are already set (e.g., re-opening)
            state = self._files[file_path]
            if state['chunks']:
                num_to_prefetch = min(self.n_prefetch + 1, len(state['chunks']))
                logger.debug(f"Prefetching {num_to_prefetch} initial chunks for {file_path}")
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
            # --- PREFETCHING: Submit background tasks to load neighbors FIRST (before waiting) ---
            # This ensures prefetching happens eagerly, even before we need the current chunk
            # Batch check all neighbors in one lock acquisition to reduce contention
            with self._lock:
                for i in range(1, self.n_prefetch + 1):
                    neighbor_before = chunk_idx - i
                    neighbor_after = chunk_idx + i
                    if neighbor_before >= 0 and neighbor_before not in cache and neighbor_before not in in_progress:
                        self._prefetch_executor.submit(self._load_and_cache_chunk, file_path, neighbor_before)
                    if neighbor_after < len(chunks) and neighbor_after not in cache and neighbor_after not in in_progress:
                        self._prefetch_executor.submit(self._load_and_cache_chunk, file_path, neighbor_after)
            
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
                        start_time = chunk_info['start']
                        end_time = chunk_info['end']
                        
                        # Validate time range against file bounds
                        file_start = min(rdr.get_property('start_time'))
                        file_end = max(rdr.get_property('end_time'))
                        
                        if start_time < file_start or end_time > file_end:
                            logger.warning(f"Chunk {chunk_idx} has invalid time range [{start_time}, {end_time}] for file bounds [{file_start}, {file_end}] in {file_path}")
                            # Clamp to valid range
                            start_time = max(start_time, file_start)
                            end_time = min(end_time, file_end)
                            
                            # Check if clamping resulted in invalid range
                            if start_time >= end_time:
                                logger.error(f"Chunk {chunk_idx} has completely invalid time range after clamping: [{start_time}, {end_time}] in {file_path}")
                                yield gRPCMef3Server_pb2.SignalChunk(
                                    file_path=file_path,
                                    error_message=f"Invalid time range for chunk {chunk_idx}"
                                )
                                return
                        
                        data = rdr.get_data(active_channels, start_time, end_time)
                        data = np.array(data)
                        cache.put(chunk_idx, data)
                    except Exception as e:
                        logger.error(f"Error loading chunk {chunk_idx} for {file_path}: {e}", exc_info=True)
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
        except Exception as e:
            logger.error(f"Unexpected error in get_signal_segment: {e}")
            yield gRPCMef3Server_pb2.SignalChunk(
                file_path=file_path,
                error_message=str(e)
            )

    # --- NEW: Method to gracefully shut down the thread pool ---
    def shutdown(self):
        """Shuts down the background prefetch thread pool executor."""
        logger.info("Shutting down prefetch executor...")
        self._prefetch_executor.shutdown(wait=False)

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
        logger.info(f"Closing file: {file_path}")
        with self._lock:
            try:
                if file_path in self._files:
                    # Get stats before cleanup
                    state = self._files[file_path]
                    cache_size = len(state['cache']) if 'cache' in state else 0
                    num_chunks = len(state.get('chunks', []))
                    
                    # Clean up resources if necessary (e.g., rdr.close())
                    del self._files[file_path]
                    # Clean up in-progress events for this file
                    in_progress_count = len(self._in_progress.get(file_path, {}))
                    self._in_progress.pop(file_path, None)
                    
                    logger.info(f"Closed and removed file: {file_path} (had {num_chunks} chunks, {cache_size} cached, {in_progress_count} in-progress)")
                else:
                    logger.warning(f"Attempted to close file that was not open: {file_path}")
                    
                return gRPCMef3Server_pb2.FileInfoResponse(
                    file_path=file_path,
                    file_opened=False,
                    error_message=""
                )
            except Exception as e:
                logger.error(f"Error closing file {file_path}: {e}", exc_info=True)
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
                
                logger.info(f"Setting segment size for {file_path}: {seconds}s, file time range: [{start_uutc}, {end_uutc}]")
                
                # Clear cache and in-progress when resetting segment size
                if 'chunks' in state and state['chunks']:
                    logger.debug(f"Clearing cache for {file_path} before resetting segment size")
                    state['cache'].clear()
                    # Clear in-progress tracking (background threads will complete naturally)
                    # The events will still be set in the finally blocks of those threads
                    if file_path in self._in_progress:
                        # Wait briefly for any in-progress operations to notice the cleared cache
                        in_progress_events = list(self._in_progress[file_path].values())
                        self._in_progress[file_path].clear()
                        # Give existing operations a moment to complete (non-blocking)
                        for event in in_progress_events[:5]:  # Wait for up to 5 recent operations
                            event.wait(timeout=0.1)
                
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
                state['chunk_duration_s'] = seconds
                state['chunks'] = segments
                if segments:
                    logger.info(f"Set segment size to {seconds}s for {file_path}, total segments: {len(segments)}")
                    # Eagerly prefetch the first n_prefetch chunks
                    num_to_prefetch = min(self.n_prefetch + 1, len(segments))
                    logger.debug(f"Prefetching first {num_to_prefetch} chunks for {file_path}")
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
    
    def get_number_of_segments(self, file_path):
        """Gets the number of segments for a file.
        
        Args:
            file_path (str): Path to the MEF file.
        
        Returns:
            int: Number of segments, or 0 if file not open or segments not set.
        """
        with self._lock:
            if file_path not in self._files:
                logger.warning(f"get_number_of_segments: file not open: {file_path}")
                return 0
            state = self._files[file_path]
            chunks = state.get('chunks', [])
            return len(chunks)

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
