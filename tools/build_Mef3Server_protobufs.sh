python -m grpc.tools.protoc \
      -I. --python_out=. \
      --pyi_out=. \
      --grpc_python_out=. \
      bnel_mef3_server/protobufs/gRPCMef3Server.proto

