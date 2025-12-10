from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class OpenFileRequest(_message.Message):
    __slots__ = ("file_path",)
    FILE_PATH_FIELD_NUMBER: _ClassVar[int]
    file_path: str
    def __init__(self, file_path: _Optional[str] = ...) -> None: ...

class CloseFileRequest(_message.Message):
    __slots__ = ("file_path",)
    FILE_PATH_FIELD_NUMBER: _ClassVar[int]
    file_path: str
    def __init__(self, file_path: _Optional[str] = ...) -> None: ...

class CloseFileResponse(_message.Message):
    __slots__ = ("file_path", "error_message")
    FILE_PATH_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    file_path: str
    error_message: str
    def __init__(self, file_path: _Optional[str] = ..., error_message: _Optional[str] = ...) -> None: ...

class FileInfoRequest(_message.Message):
    __slots__ = ("file_path",)
    FILE_PATH_FIELD_NUMBER: _ClassVar[int]
    file_path: str
    def __init__(self, file_path: _Optional[str] = ...) -> None: ...

class FileInfoResponse(_message.Message):
    __slots__ = ("file_path", "file_opened", "number_of_channels", "channel_names", "channel_sampling_rates", "start_uutc", "end_uutc", "duration_s", "error_message")
    FILE_PATH_FIELD_NUMBER: _ClassVar[int]
    FILE_OPENED_FIELD_NUMBER: _ClassVar[int]
    NUMBER_OF_CHANNELS_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_NAMES_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_SAMPLING_RATES_FIELD_NUMBER: _ClassVar[int]
    START_UUTC_FIELD_NUMBER: _ClassVar[int]
    END_UUTC_FIELD_NUMBER: _ClassVar[int]
    DURATION_S_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    file_path: str
    file_opened: bool
    number_of_channels: int
    channel_names: _containers.RepeatedScalarFieldContainer[str]
    channel_sampling_rates: _containers.RepeatedScalarFieldContainer[float]
    start_uutc: int
    end_uutc: int
    duration_s: float
    error_message: str
    def __init__(self, file_path: _Optional[str] = ..., file_opened: bool = ..., number_of_channels: _Optional[int] = ..., channel_names: _Optional[_Iterable[str]] = ..., channel_sampling_rates: _Optional[_Iterable[float]] = ..., start_uutc: _Optional[int] = ..., end_uutc: _Optional[int] = ..., duration_s: _Optional[float] = ..., error_message: _Optional[str] = ...) -> None: ...

class SetSignalSegmentRequest(_message.Message):
    __slots__ = ("file_path", "seconds")
    FILE_PATH_FIELD_NUMBER: _ClassVar[int]
    SECONDS_FIELD_NUMBER: _ClassVar[int]
    file_path: str
    seconds: int
    def __init__(self, file_path: _Optional[str] = ..., seconds: _Optional[int] = ...) -> None: ...

class SetSignalSegmentResponse(_message.Message):
    __slots__ = ("file_path", "number_of_segments", "error_message")
    FILE_PATH_FIELD_NUMBER: _ClassVar[int]
    NUMBER_OF_SEGMENTS_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    file_path: str
    number_of_segments: int
    error_message: str
    def __init__(self, file_path: _Optional[str] = ..., number_of_segments: _Optional[int] = ..., error_message: _Optional[str] = ...) -> None: ...

class SignalChunkRequest(_message.Message):
    __slots__ = ("file_path", "chunk_idx")
    FILE_PATH_FIELD_NUMBER: _ClassVar[int]
    CHUNK_IDX_FIELD_NUMBER: _ClassVar[int]
    file_path: str
    chunk_idx: int
    def __init__(self, file_path: _Optional[str] = ..., chunk_idx: _Optional[int] = ...) -> None: ...

class SignalChunk(_message.Message):
    __slots__ = ("file_path", "array_bytes", "dtype", "shape", "start_uutc", "end_uutc", "fs", "channel_names", "error_message")
    FILE_PATH_FIELD_NUMBER: _ClassVar[int]
    ARRAY_BYTES_FIELD_NUMBER: _ClassVar[int]
    DTYPE_FIELD_NUMBER: _ClassVar[int]
    SHAPE_FIELD_NUMBER: _ClassVar[int]
    START_UUTC_FIELD_NUMBER: _ClassVar[int]
    END_UUTC_FIELD_NUMBER: _ClassVar[int]
    FS_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_NAMES_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    file_path: str
    array_bytes: bytes
    dtype: str
    shape: _containers.RepeatedScalarFieldContainer[int]
    start_uutc: int
    end_uutc: int
    fs: float
    channel_names: _containers.RepeatedScalarFieldContainer[str]
    error_message: str
    def __init__(self, file_path: _Optional[str] = ..., array_bytes: _Optional[bytes] = ..., dtype: _Optional[str] = ..., shape: _Optional[_Iterable[int]] = ..., start_uutc: _Optional[int] = ..., end_uutc: _Optional[int] = ..., fs: _Optional[float] = ..., channel_names: _Optional[_Iterable[str]] = ..., error_message: _Optional[str] = ...) -> None: ...

class ListOpenFilesRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ListOpenFilesResponse(_message.Message):
    __slots__ = ("file_paths", "error_message")
    FILE_PATHS_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    file_paths: _containers.RepeatedScalarFieldContainer[str]
    error_message: str
    def __init__(self, file_paths: _Optional[_Iterable[str]] = ..., error_message: _Optional[str] = ...) -> None: ...

class SetActiveChannelsRequest(_message.Message):
    __slots__ = ("file_path", "channel_names")
    FILE_PATH_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_NAMES_FIELD_NUMBER: _ClassVar[int]
    file_path: str
    channel_names: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, file_path: _Optional[str] = ..., channel_names: _Optional[_Iterable[str]] = ...) -> None: ...

class SetActiveChannelsResponse(_message.Message):
    __slots__ = ("file_path", "active_channels", "error_message")
    FILE_PATH_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_CHANNELS_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    file_path: str
    active_channels: _containers.RepeatedScalarFieldContainer[str]
    error_message: str
    def __init__(self, file_path: _Optional[str] = ..., active_channels: _Optional[_Iterable[str]] = ..., error_message: _Optional[str] = ...) -> None: ...

class GetActiveChannelsRequest(_message.Message):
    __slots__ = ("file_path",)
    FILE_PATH_FIELD_NUMBER: _ClassVar[int]
    file_path: str
    def __init__(self, file_path: _Optional[str] = ...) -> None: ...

class GetActiveChannelsResponse(_message.Message):
    __slots__ = ("file_path", "active_channels", "error_message")
    FILE_PATH_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_CHANNELS_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    file_path: str
    active_channels: _containers.RepeatedScalarFieldContainer[str]
    error_message: str
    def __init__(self, file_path: _Optional[str] = ..., active_channels: _Optional[_Iterable[str]] = ..., error_message: _Optional[str] = ...) -> None: ...
