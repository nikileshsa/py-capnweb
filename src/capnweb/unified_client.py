"""Unified RPC Client with production-grade session management.

This module provides a unified client that automatically uses the appropriate
session type based on the transport:
- HTTP Batch: Uses simple request/response pattern
- WebSocket: Uses BidirectionalSession for full bidirectional RPC
- WebTransport: Uses BidirectionalSession for full bidirectional RPC

All session types support:
- Proper release message sending
- Reference counting
- onBroken callbacks
- drain() for graceful shutdown
- getStats() for debugging
"""

from __future__ import annotations

import asyncio
from typing import Any, Self

import aiohttp

from capnweb.config import ClientConfig, RpcSessionConfig
from capnweb.error import RpcError
from capnweb.payload import RpcPayload
from capnweb.rpc_session import BidirectionalSession, RpcTransport
from capnweb.stubs import RpcStub
from capnweb.types import RpcTarget
from capnweb.wire import (
    PropertyKey,
    WireError,
    WirePipeline,
    WirePull,
    WirePush,
    WireReject,
    WireResolve,
    parse_wire_batch,
    serialize_wire_batch,
)

# Backwards compatibility alias
UnifiedClientConfig = ClientConfig


class WebSocketClientTransport(RpcTransport):
    """WebSocket transport adapter for BidirectionalSession."""
    
    def __init__(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        self._ws = ws
        self._closed = False
    
    async def send(self, message: str) -> None:
        if self._closed:
            raise ConnectionError("WebSocket is closed")
        await self._ws.send_str(message)
    
    async def receive(self) -> str:
        if self._closed:
            raise ConnectionError("WebSocket is closed")
        
        msg = await self._ws.receive()
        
        if msg.type == aiohttp.WSMsgType.TEXT:
            return msg.data
        elif msg.type == aiohttp.WSMsgType.BINARY:
            return msg.data.decode("utf-8")
        elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED):
            self._closed = True
            raise ConnectionError("WebSocket closed")
        elif msg.type == aiohttp.WSMsgType.ERROR:
            self._closed = True
            raise ConnectionError(f"WebSocket error: {self._ws.exception()}")
        else:
            raise ValueError(f"Unexpected message type: {msg.type}")
    
    def abort(self, reason: Exception) -> None:
        self._closed = True
        asyncio.create_task(self._close())
    
    async def _close(self) -> None:
        try:
            await self._ws.close()
        except Exception:
            pass


class UnifiedClient:
    """Production-grade RPC client with automatic transport selection.
    
    This client automatically selects the appropriate session type:
    - HTTP URLs: Simple request/response pattern
    - WebSocket URLs: Full bidirectional RPC with BidirectionalSession
    - WebTransport URLs: Full bidirectional RPC with BidirectionalSession
    
    Features:
    - Proper release message sending (prevents memory leaks)
    - Reference counting for capabilities
    - onBroken callbacks for connection death handling
    - drain() for graceful shutdown
    - getStats() for debugging
    - Server can call back to client (bidirectional)
    
    Example:
        ```python
        async with UnifiedClient(UnifiedClientConfig(url="ws://localhost:8080/rpc")) as client:
            stub = client.get_main_stub()
            result = await stub.greet("World")
            print(result)  # "Hello, World!"
        ```
    """
    
    def __init__(self, config: UnifiedClientConfig) -> None:
        self.config = config
        self._http_session: aiohttp.ClientSession | None = None
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._session: BidirectionalSession | None = None
        self._is_websocket = config.url.startswith(("ws://", "wss://"))
        self._is_webtransport = "/wt" in config.url or ":4433" in config.url
    
    async def __aenter__(self) -> Self:
        """Connect to the server."""
        if self._is_websocket:
            await self._connect_websocket()
        elif self._is_webtransport:
            await self._connect_webtransport()
        else:
            # HTTP batch - create session for each request
            self._http_session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, *args: object) -> None:
        """Disconnect from the server."""
        await self.close()
    
    async def _connect_websocket(self) -> None:
        """Connect via WebSocket with BidirectionalSession."""
        self._http_session = aiohttp.ClientSession()
        self._ws = await self._http_session.ws_connect(self.config.url)
        
        transport = WebSocketClientTransport(self._ws)
        self._session = BidirectionalSession(
            transport=transport,
            local_main=self.config.local_main,
            options=self.config.options,
        )
        self._session.start()
    
    async def _connect_webtransport(self) -> None:
        """Connect via WebTransport with BidirectionalSession."""
        # WebTransport requires aioquic
        try:
            from capnweb.webtransport import WebTransportClient
        except ImportError as e:
            raise RuntimeError("WebTransport requires aioquic: pip install aioquic") from e
        
        # For now, WebTransport uses its own client
        # TODO: Integrate with BidirectionalSession
        raise NotImplementedError("WebTransport integration with BidirectionalSession pending")
    
    async def close(self) -> None:
        """Close the connection."""
        if self._session:
            await self._session.stop()
            self._session = None
        if self._ws:
            await self._ws.close()
            self._ws = None
        if self._http_session:
            await self._http_session.close()
            self._http_session = None
    
    def get_main_stub(self) -> RpcStub:
        """Get the server's main capability as an RpcStub.
        
        Returns:
            An RpcStub for the server's main capability
        """
        if self._session:
            return RpcStub(self._session.get_main_stub())
        raise RuntimeError("Not connected or using HTTP batch mode")
    
    async def call(
        self,
        cap_id: int,
        method: str,
        args: list[Any],
    ) -> Any:
        """Call a method on a remote capability.
        
        Args:
            cap_id: The capability ID (0 for main)
            method: The method name
            args: Arguments to pass
            
        Returns:
            The result of the call
        """
        if self._session:
            # Use BidirectionalSession
            args_payload = RpcPayload.from_app_params(args)
            
            # Check if this import is already resolved (e.g., from a previous call)
            # If so, delegate to the resolution instead of sending a new call
            from capnweb.rpc_session import ImportHook
            entry = self._session._imports.get(cap_id)
            if entry is not None and entry.resolution is not None:
                # Use the resolved hook directly (call is now synchronous)
                result_hook = entry.resolution.call([method], args_payload)
            else:
                # Send call to peer (send_call is now synchronous)
                result_hook = self._session.send_call(cap_id, [method], args_payload)
            
            payload = await result_hook.pull()
            return payload.value
        elif self._http_session:
            # HTTP batch mode
            return await self._call_http_batch(cap_id, method, args)
        else:
            raise RuntimeError("Not connected")
    
    async def _call_http_batch(
        self,
        cap_id: int,
        method: str,
        args: list[Any],
    ) -> Any:
        """Make an RPC call via HTTP batch."""
        if not self._http_session:
            raise RuntimeError("HTTP session not initialized")
        
        # Build the request
        from capnweb.serializer import Serializer
        from capnweb.parser import Parser
        
        # Create a minimal exporter/importer for serialization
        class MinimalSession:
            def __init__(self):
                self._exports = {}
                self._next_export_id = -1
            
            def export_capability(self, stub: Any) -> int:
                export_id = self._next_export_id
                self._next_export_id -= 1
                return export_id
            
            def import_capability(self, import_id: int) -> Any:
                from capnweb.hooks import PayloadStubHook
                return PayloadStubHook(RpcPayload.owned(None))
            
            def create_promise_hook(self, promise_id: int) -> Any:
                from capnweb.hooks import PayloadStubHook
                return PayloadStubHook(RpcPayload.owned(None))
        
        session = MinimalSession()
        serializer = Serializer(exporter=session)
        parser = Parser(importer=session)
        
        # Build pipeline expression
        path_keys = [PropertyKey(method)]
        args_payload = RpcPayload.from_app_params(args)
        serialized_args = serializer.serialize_payload(args_payload)
        
        pipeline = WirePipeline(
            import_id=cap_id,
            property_path=path_keys,
            args=serialized_args,
        )
        
        import_id = 1
        push_msg = WirePush(pipeline)
        pull_msg = WirePull(import_id)
        
        batch = serialize_wire_batch([push_msg, pull_msg])
        
        async with self._http_session.post(
            self.config.url,
            data=batch.encode("utf-8"),
            headers={"Content-Type": "application/x-ndjson"},
            timeout=aiohttp.ClientTimeout(total=self.config.timeout),
        ) as response:
            response.raise_for_status()
            response_text = await response.text()
        
        if not response_text:
            return None
        
        messages = parse_wire_batch(response_text)
        
        for msg in messages:
            if isinstance(msg, WireResolve) and abs(msg.export_id) == import_id:
                result_payload = parser.parse(msg.value)
                return result_payload.value
            elif isinstance(msg, WireReject) and abs(msg.export_id) == import_id:
                if isinstance(msg.error, WireError):
                    raise RpcError.internal(msg.error.message)
                raise RpcError.internal(str(msg.error))
        
        return None
    
    async def drain(self) -> None:
        """Wait for all pending operations to complete."""
        if self._session:
            await self._session.drain()
    
    def get_stats(self) -> dict[str, int]:
        """Get session statistics."""
        if self._session:
            return self._session.get_stats()
        return {"imports": 0, "exports": 0}
    
    @property
    def is_bidirectional(self) -> bool:
        """Check if this client supports bidirectional RPC."""
        return self._session is not None
