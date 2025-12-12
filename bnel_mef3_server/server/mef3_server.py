# mef
import grpc
from concurrent import futures

from bnel_mef3_server.server.log_manager import get_logger, setup_logging
from bnel_mef3_server.server.config_manager import read_app_config, get_log_level_from_config
import os

import bnel_mef3_server.protobufs.gRPCMef3Server_pb2 as gRPCMef3Server_pb2
import bnel_mef3_server.protobufs.gRPCMef3Server_pb2_grpc as gRPCMef3Server_pb2_grpc

from bnel_mef3_server.server.file_manager import FileManager

# Setup logging before anything else
def _init_logging():
    config = read_app_config()
    log_level = get_log_level_from_config(config)
    log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../logs')
    log_file = setup_logging(log_dir, log_level)
    print(f"Logging to: {log_file}")
    return log_file

_init_logging()
logger = get_logger("bnel_mef3_server")


class gRPCMef3Server(gRPCMef3Server_pb2_grpc.gRPCMef3ServerServicer):
    """gRPC servicer for MEF3 files. Stateless, delegates all requests to FileManager."""

    def __init__(self, file_manager):
        """Initializes the gRPC servicer.

        Args:
            file_manager (FileManager): The FileManager instance to delegate requests to.
        """
        self.manager = file_manager

    def OpenFile(self, request, context):
        logger.info(f"Received OpenFile request for: {request.file_path}")
        try:
            return self.manager.open_file(request.file_path)
        except Exception as e:
            logger.error(f"Exception in OpenFile: {e}")
            return gRPCMef3Server_pb2.FileInfoResponse(
                file_path=request.file_path,
                file_opened=False,
                error_message=str(e)
            )

    def CloseFile(self, request, context):
        logger.info(f"Received CloseFile request for: {request.file_path}")
        try:
            return self.manager.close_file(request.file_path)
        except Exception as e:
            logger.error(f"Exception in CloseFile: {e}")
            return gRPCMef3Server_pb2.FileInfoResponse(
                file_path=request.file_path,
                file_opened=False,
                error_message=str(e)
            )

    def FileInfo(self, request, context):
        logger.info(f"Received FileInfo request for: {request.file_path}")
        try:
            return self.manager.get_file_info(request.file_path)
        except Exception as e:
            logger.error(f"Exception in FileInfo: {e}")
            return gRPCMef3Server_pb2.FileInfoResponse(
                file_path=request.file_path,
                file_opened=False,
                error_message=str(e)
            )

    def SetSignalSegmentSize(self, request, context):
        logger.info(f"Received SetSignalSegmentSize for: {request.file_path}")
        try:
            return self.manager.set_signal_segment_size(request.file_path, request.seconds)
        except Exception as e:
            logger.error(f"Exception in SetSignalSegmentSize: {e}")
            return gRPCMef3Server_pb2.SetSignalSegmentResponse(
                file_path=request.file_path,
                number_of_segments=0,
                error_message=str(e)
            )

    def GetSignalSegment(self, request, context):
        logger.info(f"Received GetSignalSegment for chunk {request.chunk_idx} from: {request.file_path}")
        try:
            yield from self.manager.get_signal_segment(request.file_path, request.chunk_idx)
        except Exception as e:
            logger.error(f"Exception in GetSignalSegment: {e}")
            yield gRPCMef3Server_pb2.SignalChunk(
                file_path=request.file_path,
                error_message=str(e)
            )

    def ListOpenFiles(self, request, context):
        logger.info("Received ListOpenFiles request")
        try:
            file_paths = self.manager.list_open_files()
            return gRPCMef3Server_pb2.ListOpenFilesResponse(file_paths=file_paths, error_message="")
        except Exception as e:
            logger.error(f"Exception in ListOpenFiles: {e}")
            return gRPCMef3Server_pb2.ListOpenFilesResponse(file_paths=[], error_message=str(e))

    def SetActiveChannels(self, request, context):
        logger.info(f"Received SetActiveChannels for: {request.file_path}")
        try:
            return self.manager.set_active_channels(request.file_path, list(request.channel_names))
        except Exception as e:
            logger.error(f"Exception in SetActiveChannels: {e}")
            return gRPCMef3Server_pb2.SetActiveChannelsResponse(
                file_path=request.file_path,
                active_channels=[],
                error_message=str(e)
            )

    def GetActiveChannels(self, request, context):
        logger.info(f"Received GetActiveChannels for: {request.file_path}")
        try:
            return self.manager.get_active_channels(request.file_path)
        except Exception as e:
            logger.error(f"Exception in GetActiveChannels: {e}")
            return gRPCMef3Server_pb2.GetActiveChannelsResponse(
                file_path=request.file_path,
                active_channels=[],
                error_message=str(e)
            )


class gRPCMef3ServerHandler:
    """Handler to launch and manage the gRPC MEF3 server lifecycle."""

    def __init__(self, port, n_prefetch=3, cache_capacity_multiplier=3, n_process_workers=2):
        """Initializes the gRPC server and FileManager.

        Args:
            port (int): Port to listen on.
            n_prefetch (int): Number of chunks to prefetch.
            cache_capacity_multiplier (int): Cache capacity multiplier.
            n_process_workers (int): Number of worker processes for parallel MEF reading.
        """
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

        # Pass parameters to FileManager
        self.file_manager = FileManager(
            n_prefetch=n_prefetch,
            cache_capacity_multiplier=cache_capacity_multiplier,
            n_process_workers=n_process_workers
        )

        gRPCMef3Server_pb2_grpc.add_gRPCMef3ServerServicer_to_server(
            gRPCMef3Server(self.file_manager), self.server
        )
        self.server.add_insecure_port(f"0.0.0.0:{port}")
        self.server.start()
        logger.info(f"gRPC server started on port {port}.")

    def stop(self, grace=0.1):
        logger.info("Stopping gRPC server...")
        # --- NEW: Shut down the file manager's resources ---
        self.file_manager.shutdown()
        self.server.stop(grace)
