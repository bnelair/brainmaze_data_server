"""
MEF Worker Process Module

This module implements worker processes for parallel MEF data reading.
Each worker process maintains its own MefReader instance to work around
pymef's global variable limitations.
"""

import multiprocessing as mp
import queue
import numpy as np
from mef_tools import MefReader
from bnel_mef3_server.server.log_manager import get_logger
import traceback

logger = get_logger("bnel_mef3_server.mef_worker")


class MefWorkerProcess:
    """
    Worker process for parallel MEF data reading.
    
    Each worker runs in a separate process with its own MefReader instance,
    allowing true parallel reading despite pymef's global variable usage.
    """
    
    def __init__(self, worker_id, task_queue, result_queue):
        """
        Initialize the worker process.
        
        Args:
            worker_id (int): Unique identifier for this worker
            task_queue (mp.Queue): Queue to receive read tasks
            result_queue (mp.Queue): Queue to send results back
        """
        self.worker_id = worker_id
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.process = None
        self._readers = {}  # Cache of open readers per file path
        
    def start(self):
        """Start the worker process."""
        self.process = mp.Process(
            target=self._run,
            name=f"MefWorker-{self.worker_id}",
            daemon=True
        )
        self.process.start()
        logger.info(f"Started MEF worker process {self.worker_id} (PID: {self.process.pid})")
        
    def stop(self, timeout=5):
        """Stop the worker process gracefully."""
        if self.process and self.process.is_alive():
            # Send stop signal
            self.task_queue.put(('STOP', None))
            self.process.join(timeout=timeout)
            if self.process.is_alive():
                logger.warning(f"Worker {self.worker_id} did not stop gracefully, terminating")
                self.process.terminate()
                self.process.join(timeout=1)
            logger.info(f"Stopped MEF worker process {self.worker_id}")
            
    def _run(self):
        """Main loop for the worker process."""
        logger.info(f"MEF Worker {self.worker_id} starting main loop")
        
        try:
            while True:
                # Wait for a task
                task = self.task_queue.get()
                
                if task is None or (isinstance(task, tuple) and task[0] == 'STOP'):
                    logger.info(f"Worker {self.worker_id} received stop signal")
                    break
                    
                # Process the task
                command, params = task
                
                if command == 'READ':
                    self._handle_read_task(params)
                elif command == 'CLOSE_FILE':
                    self._handle_close_file(params)
                else:
                    logger.warning(f"Worker {self.worker_id} received unknown command: {command}")
                    
        except Exception as e:
            logger.error(f"Worker {self.worker_id} crashed: {e}\n{traceback.format_exc()}")
        finally:
            # Cleanup: close all readers
            for file_path in list(self._readers.keys()):
                try:
                    del self._readers[file_path]
                    logger.debug(f"Worker {self.worker_id} closed reader for {file_path}")
                except Exception as e:
                    logger.error(f"Worker {self.worker_id} error closing reader: {e}")
            logger.info(f"Worker {self.worker_id} exiting")
            
    def _get_or_create_reader(self, file_path):
        """
        Get or create a MefReader for the given file path.
        
        Args:
            file_path (str): Path to the MEF file
            
        Returns:
            MefReader: Reader instance for the file
        """
        if file_path not in self._readers:
            self._readers[file_path] = MefReader(file_path)
            logger.debug(f"Worker {self.worker_id} opened reader for {file_path}")
        return self._readers[file_path]
        
    def _handle_read_task(self, params):
        """
        Handle a read task.
        
        Args:
            params (dict): Task parameters with keys:
                - task_id: Unique task identifier
                - file_path: Path to MEF file
                - chunk_idx: Chunk index to read
                - start: Start time in microseconds
                - end: End time in microseconds
                - channels: List of channel names (or None for all)
        """
        task_id = params['task_id']
        file_path = params['file_path']
        chunk_idx = params['chunk_idx']
        start = params['start']
        end = params['end']
        channels = params.get('channels')
        
        try:
            # Get reader for this file
            reader = self._get_or_create_reader(file_path)
            
            # If no channels specified, read all
            if channels is None:
                channels = reader.channels
                
            # Read the data
            data = reader.get_data(channels, start, end)
            data = np.array(data)
            
            # Send result back
            result = {
                'task_id': task_id,
                'success': True,
                'chunk_idx': chunk_idx,
                'data': data,
                'worker_id': self.worker_id
            }
            self.result_queue.put(result)
            logger.debug(f"Worker {self.worker_id} completed task {task_id} for chunk {chunk_idx}")
            
        except Exception as e:
            logger.error(f"Worker {self.worker_id} error reading chunk {chunk_idx}: {e}")
            # Send error result
            result = {
                'task_id': task_id,
                'success': False,
                'chunk_idx': chunk_idx,
                'error': str(e),
                'worker_id': self.worker_id
            }
            self.result_queue.put(result)
            
    def _handle_close_file(self, params):
        """
        Handle a close file request.
        
        Args:
            params (dict): Parameters with 'file_path' key
        """
        file_path = params['file_path']
        if file_path in self._readers:
            try:
                del self._readers[file_path]
                logger.debug(f"Worker {self.worker_id} closed reader for {file_path}")
            except Exception as e:
                logger.error(f"Worker {self.worker_id} error closing {file_path}: {e}")


class MefWorkerPool:
    """
    Pool of worker processes for parallel MEF reading.
    """
    
    def __init__(self, n_workers=2):
        """
        Initialize the worker pool.
        
        Args:
            n_workers (int): Number of worker processes to create
        """
        self.n_workers = n_workers
        self.task_queue = mp.Queue()
        self.result_queue = mp.Queue()
        self.workers = []
        self._next_task_id = 0
        self._pending_tasks = {}  # task_id -> task info
        self._started = False
        
        logger.info(f"Initializing MEF worker pool with {n_workers} workers")
        
    def start(self):
        """Start all worker processes."""
        if self._started:
            logger.warning("Worker pool already started")
            return
            
        for i in range(self.n_workers):
            worker = MefWorkerProcess(i, self.task_queue, self.result_queue)
            worker.start()
            self.workers.append(worker)
            
        self._started = True
        logger.info(f"Started {self.n_workers} MEF worker processes")
        
    def stop(self):
        """Stop all worker processes."""
        if not self._started:
            return
            
        logger.info("Stopping MEF worker pool")
        for worker in self.workers:
            worker.stop()
        self.workers.clear()
        self._started = False
        
    def submit_read_task(self, file_path, chunk_idx, start, end, channels=None):
        """
        Submit a read task to the worker pool.
        
        Args:
            file_path (str): Path to MEF file
            chunk_idx (int): Chunk index
            start (int): Start time in microseconds
            end (int): End time in microseconds
            channels (list): List of channel names (None for all)
            
        Returns:
            int: Task ID for tracking
        """
        task_id = self._next_task_id
        self._next_task_id += 1
        
        task = ('READ', {
            'task_id': task_id,
            'file_path': file_path,
            'chunk_idx': chunk_idx,
            'start': start,
            'end': end,
            'channels': channels
        })
        
        self._pending_tasks[task_id] = {
            'chunk_idx': chunk_idx,
            'file_path': file_path
        }
        
        self.task_queue.put(task)
        logger.debug(f"Submitted read task {task_id} for chunk {chunk_idx}")
        return task_id
        
    def get_result(self, timeout=None):
        """
        Get a result from the result queue.
        
        Args:
            timeout (float): Timeout in seconds (None for blocking)
            
        Returns:
            dict: Result dictionary or None if timeout
        """
        try:
            result = self.result_queue.get(timeout=timeout)
            task_id = result['task_id']
            if task_id in self._pending_tasks:
                del self._pending_tasks[task_id]
            return result
        except queue.Empty:
            return None
            
    def close_file(self, file_path):
        """
        Notify workers to close readers for a file.
        
        Args:
            file_path (str): Path to MEF file
        """
        task = ('CLOSE_FILE', {'file_path': file_path})
        # Send to all workers
        for _ in range(self.n_workers):
            self.task_queue.put(task)
            
    def has_pending_tasks(self):
        """Check if there are pending tasks."""
        return len(self._pending_tasks) > 0
        
    def get_pending_task_count(self):
        """Get the number of pending tasks."""
        return len(self._pending_tasks)
