import grpc
import numpy as np

import bnel_mef3_server.protobufs.gRPCMef3Server_pb2 as pb2
import bnel_mef3_server.protobufs.gRPCMef3Server_pb2_grpc as pb2_grpc

class Mef3Client:
    """
    Client for interacting with the MEF3 gRPC server.

    This class provides a high-level, Pythonic interface to the MEF3 gRPC server for accessing and manipulating
    MEF3 files. It abstracts away the gRPC/protobuf details and exposes methods for file management, chunking,
    channel selection, and signal data retrieval.

    Usage:
        client = Mef3Client("localhost:50052")
        client.open_file("/path/to/file.mefd")
        ...
        client.shutdown()
    """

    def __init__(self, address="localhost:50052"):
        """
        Initialize the Mef3Client and connect to the gRPC server.

        Args:
            address (str): Address of the gRPC server (default: "localhost:50052").
        """
        self.channel = grpc.insecure_channel(address)
        self.stub = pb2_grpc.gRPCMef3ServerStub(self.channel)

    def open_file(self, file_path):
        """
        Open a MEF3 file on the server.

        Args:
            file_path (str): Path to the MEF3 file.
        Returns:
            dict: File info including file_path, file_opened, number_of_channels, channel_names, etc.
        """
        resp = self.stub.OpenFile(pb2.OpenFileRequest(file_path=file_path))
        return {
            "file_path": resp.file_path,
            "file_opened": resp.file_opened,
            "number_of_channels": getattr(resp, "number_of_channels", None),
            "channel_names": list(getattr(resp, "channel_names", [])),
            "channel_sampling_rates": list(getattr(resp, "channel_sampling_rates", [])),
            "start_uutc": getattr(resp, "start_uutc", None),
            "end_uutc": getattr(resp, "end_uutc", None),
            "duration_s": getattr(resp, "duration_s", None),
        }

    def close_file(self, file_path):
        """
        Close a MEF3 file on the server.

        Args:
            file_path (str): Path to the MEF3 file.
        Returns:
            dict: Contains file_path and file_opened status.
        """
        resp = self.stub.CloseFile(pb2.FileInfoRequest(file_path=file_path))
        return {
            "file_path": resp.file_path,
            "file_opened": resp.file_opened,
        }

    def get_file_info(self, file_path):
        """
        Get information about an open MEF3 file.

        Args:
            file_path (str): Path to the MEF3 file.
        Returns:
            dict: File info including file_path, file_opened, number_of_channels, channel_names, etc.
        """
        resp = self.stub.FileInfo(pb2.FileInfoRequest(file_path=file_path))
        return {
            "file_path": resp.file_path,
            "file_opened": resp.file_opened,
            "number_of_channels": getattr(resp, "number_of_channels", None),
            "channel_names": list(getattr(resp, "channel_names", [])),
            "channel_sampling_rates": list(getattr(resp, "channel_sampling_rates", [])),
            "start_uutc": getattr(resp, "start_uutc", None),
            "end_uutc": getattr(resp, "end_uutc", None),
            "duration_s": getattr(resp, "duration_s", None),
        }

    def set_signal_segment_size(self, file_path, seconds):
        """
        Set the segment size (in seconds) for signal data for a given file.

        Args:
            file_path (str): Path to the MEF3 file.
            seconds (int): Duration of each segment in seconds.
        Returns:
            dict: Contains file_path and number_of_segments.
        """
        resp = self.stub.SetSignalSegmentSize(pb2.SetSignalSegmentRequest(file_path=file_path, seconds=seconds))
        return {
            "file_path": resp.file_path,
            "number_of_segments": resp.number_of_segments,
        }

    def get_signal_segment(self, file_path, chunk_idx):
        """
        Retrieve the full signal segment (as a single numpy array) and its metadata for the requested segment.

        Args:
            file_path (str): Path to the MEF3 file.
            chunk_idx (int): Index of the segment to retrieve.
        Returns:
            dict: {
                'array': np.ndarray,
                'channel_names': list,
                'fs': float,
                'start_uutc': int,
                'end_uutc': int,
                'dtype': str,
                'shape': tuple,
                'error_message': str (if any)
            }
        """
        req = pb2.SignalChunkRequest(file_path=file_path, chunk_idx=chunk_idx)
        arrays = []
        channel_names = None
        fs = None
        start_uutc = None
        end_uutc = None
        dtype = None
        shape = None
        error_message = None
        for chunk in self.stub.GetSignalSegment(req):
            if chunk.error_message:
                error_message = chunk.error_message
                break
            arr = np.frombuffer(chunk.array_bytes, dtype=chunk.dtype)
            arr = arr.reshape(chunk.shape)
            arrays.append(arr)
            channel_names = list(chunk.channel_names)
            dtype = chunk.dtype
            fs = chunk.fs
            shape = tuple(chunk.shape)
            start_uutc = chunk.start_uutc if start_uutc is None else min(start_uutc, chunk.start_uutc)
            end_uutc = chunk.end_uutc if end_uutc is None else max(end_uutc, chunk.end_uutc)
        if not arrays:
            return {
                'array': None,
                'channel_names': channel_names,
                'fs': fs,
                'start_uutc': start_uutc,
                'end_uutc': end_uutc,
                'dtype': dtype,
                'shape': shape,
                'error_message': error_message or 'No data returned.'
            }
        array = np.concatenate(arrays, axis=1) if len(arrays) > 1 else arrays[0]
        return {
            'array': array,
            'channel_names': channel_names,
            'fs': fs,
            'start_uutc': start_uutc,
            'end_uutc': end_uutc,
            'dtype': dtype,
            'shape': array.shape,
            'error_message': error_message or ''
        }

    def list_open_files(self):
        """
        List all currently open MEF3 files on the server.

        Returns:
            list: List of file paths for open files.
        """
        resp = self.stub.ListOpenFiles(pb2.ListOpenFilesRequest())
        return list(resp.file_paths)

    def set_active_channels(self, file_path, channel_names):
        """
        Set the active channels for a given file. Only these channels will be included in subsequent data requests.

        Args:
            file_path (str): Path to the MEF3 file.
            channel_names (list): List of channel names to activate.
        Returns:
            dict: Contains file_path, active_channels, and error_message.
        """
        resp = self.stub.SetActiveChannels(pb2.SetActiveChannelsRequest(file_path=file_path, channel_names=channel_names))
        return {
            "file_path": resp.file_path,
            "active_channels": list(resp.active_channels),
            "error_message": getattr(resp, "error_message", "")
        }

    def get_active_channels(self, file_path):
        """
        Get the currently active channels for a given file.

        Args:
            file_path (str): Path to the MEF3 file.
        Returns:
            dict: Contains file_path, active_channels, and error_message.
        """
        resp = self.stub.GetActiveChannels(pb2.GetActiveChannelsRequest(file_path=file_path))
        return {
            "file_path": resp.file_path,
            "active_channels": list(resp.active_channels),
            "error_message": getattr(resp, "error_message", "")
        }

    def shutdown(self):
        """
        Close the gRPC channel and clean up resources.
        """
        self.channel.close()
