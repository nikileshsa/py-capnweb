"""Cap'n Web Protocol - Python Implementation

This module provides a Python implementation of the Cap'n Web protocol,
a capability-based RPC system with promise pipelining.
"""

from capnweb.config import (
    ClientConfig,
    RpcSessionConfig,
    WebSocketServerConfig,
    BatchRpcConfig,
)
from capnweb.error import ErrorCode, RpcError
from capnweb.value_codec import (
    ValueCodec,
    ValueCodecOptions,
    DateValue,
    UndefinedValue,
    BigIntValue,
    BytesValue,
    NaNValue,
    InfValue,
    NegInfValue,
    ExportRef,
    ImportRef,
    PromiseRef,
    encode_value,
    decode_value,
)
from capnweb.capability_codec import (
    CapabilityCodec,
    CapabilityCodecOptions,
    create_capability_codec,
)
from capnweb.ids import ExportId, IdAllocator, ImportId
from capnweb.types import RpcTarget
from capnweb.stubs import RpcStub, RpcPromise, create_stub
from capnweb.rpc_session import BidirectionalSession, RpcSessionOptions
from capnweb.ws_session import (
    WebSocketRpcClient,
    WebSocketRpcServer,
    handle_websocket_rpc,
)
from capnweb.unified_client import UnifiedClient, UnifiedClientConfig
from capnweb.map_builder import MapBuilder, build_map
from capnweb.batch import (
    new_http_batch_rpc_session,
    new_http_batch_rpc_response,
    aiohttp_batch_rpc_handler,
    fastapi_batch_rpc_handler,
    BatchClientTransport,
    BatchServerTransport,
)

__version__ = "0.1.0"

__all__ = [
    # Core types
    "RpcTarget",
    "RpcStub",
    "RpcPromise",
    "create_stub",
    "ImportId",
    "ExportId",
    "IdAllocator",
    # Errors
    "RpcError",
    "ErrorCode",
    # Configuration (Pydantic models)
    "ClientConfig",
    "RpcSessionConfig",
    "WebSocketServerConfig",
    "BatchRpcConfig",
    # ValueCodec - explicit array-escape encoding
    "ValueCodec",
    "ValueCodecOptions",
    "DateValue",
    "UndefinedValue",
    "BigIntValue",
    "BytesValue",
    "NaNValue",
    "InfValue",
    "NegInfValue",
    "ExportRef",
    "ImportRef",
    "PromiseRef",
    "encode_value",
    "decode_value",
    # CapabilityCodec - value encoding with capability table integration
    "CapabilityCodec",
    "CapabilityCodecOptions",
    "create_capability_codec",
    # Bidirectional Session (production-grade)
    "BidirectionalSession",
    "RpcSessionOptions",  # Backwards compat alias for RpcSessionConfig
    # WebSocket Session (production-grade)
    "WebSocketRpcClient",
    "WebSocketRpcServer",
    "handle_websocket_rpc",
    # Unified Client (production-grade, recommended)
    "UnifiedClient",
    "UnifiedClientConfig",  # Backwards compat alias for ClientConfig
    # Map operations
    "MapBuilder",
    "build_map",
    # HTTP Batch RPC
    "new_http_batch_rpc_session",
    "new_http_batch_rpc_response",
    "aiohttp_batch_rpc_handler",
    "fastapi_batch_rpc_handler",
    "BatchClientTransport",
    "BatchServerTransport",
]
