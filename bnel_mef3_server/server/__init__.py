"""
Server subpackage for the BNEL MEF3 Server.

This subpackage contains the gRPC server implementation and supporting modules for serving MEF3 files.
Import the gRPCMef3Server class from here to start a server instance.
"""
from bnel_mef3_server.server.mef3_server import gRPCMef3Server


__all__ = ['gRPCMef3Server']
