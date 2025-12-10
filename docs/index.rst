.. bnel-mef3-server documentation master file

Welcome to bnel-mef3-server's documentation!
=============================================

A gRPC server for efficient, concurrent access to MEF3 (Multiscale Electrophysiology Format) files, with LRU caching and background prefetching. Designed for scalable neurophysiology data streaming and analysis.

Features
--------

- gRPC API for remote MEF3 file access
- Thread-safe LRU cache for signal chunks
- Asynchronous prefetching for low-latency streaming
- Configurable via environment variables or Docker
- Ready for deployment in Docker and CI/CD pipelines

Installation
------------

Requirements
~~~~~~~~~~~~

- Python 3.8+
- ``pip install -r requirements.txt``
- (Optional) Docker for containerized deployment

Local Setup
~~~~~~~~~~~

Clone the repository and install dependencies:

.. code-block:: bash

   pip install -r requirements.txt

Docker
~~~~~~

Build and run the server in a container:

.. code-block:: bash

   docker build -t bnel-mef3-server .
   docker run -e PORT=50051 -p 50051:50051 bnel-mef3-server

Quick Start
-----------

As a Python Module
~~~~~~~~~~~~~~~~~~

Run the server with configurable options:

.. code-block:: bash

   python -m bnel_mef3_server

Configuration via Environment Variables:

- ``PORT``: gRPC server port (default: 50051)
- ``N_PREFETCH``: Number of chunks to prefetch (default: 3)
- ``CACHE_CAPACITY_MULTIPLIER``: Extra cache slots (default: 3)
- ``MAX_WORKERS``: Prefetch thread pool size (default: 4)

Using the Python Client
~~~~~~~~~~~~~~~~~~~~~~~~

The package provides a high-level client for interacting with the server:

.. code-block:: python

   from bnel_mef3_server.client import Mef3Client

   client = Mef3Client("localhost:50052")

   # Open a file
   info = client.open_file("/path/to/file.mefd")
   print("Opened file:", info)

   # Set chunk size (in seconds)
   client.set_signal_chunk_size("/path/to/file.mefd", 60)

   # Get a chunk of signal data (as numpy arrays)
   for arr in client.get_signal_segment("/path/to/file.mefd", chunk_idx=0):
       print(arr.shape)

   # Close the file
   client.close_file("/path/to/file.mefd")
   client.shutdown()

Contents
--------

.. toctree::
   :maxdepth: 2
   :caption: API Reference

   api/client
   api/server
   api/cache
   api/file_manager
   api/config_manager
   api/log_manager

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
