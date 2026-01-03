"""WebSocket transport adapter for BidirectionalSession.

This module provides a WebSocket transport that implements the RpcTransport
protocol for use with BidirectionalSession.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import aiohttp

if TYPE_CHECKING:
    from aiohttp import ClientWebSocketResponse


class WebSocketClientTransport:
    """WebSocket transport for client-side bidirectional RPC.
    
    Implements the RpcTransport protocol expected by BidirectionalSession.
    """
    
    def __init__(self, url: str) -> None:
        """Initialize the transport.
        
        Args:
            url: WebSocket URL (e.g., "ws://localhost:8080/rpc/ws")
        """
        self.url = url
        self._session: aiohttp.ClientSession | None = None
        self._ws: ClientWebSocketResponse | None = None
        self._closed = False
    
    async def connect(self) -> None:
        """Connect to the WebSocket server."""
        self._session = aiohttp.ClientSession()
        self._ws = await self._session.ws_connect(self.url)
    
    async def send(self, message: str) -> None:
        """Send a message to the server."""
        if self._ws is None or self._closed:
            raise RuntimeError("WebSocket not connected")
        await self._ws.send_str(message)
    
    async def receive(self) -> str:
        """Receive a message from the server."""
        if self._ws is None or self._closed:
            raise RuntimeError("WebSocket not connected")
        
        msg = await self._ws.receive()
        
        if msg.type == aiohttp.WSMsgType.TEXT:
            return msg.data
        elif msg.type == aiohttp.WSMsgType.BINARY:
            return msg.data.decode("utf-8")
        elif msg.type == aiohttp.WSMsgType.CLOSE:
            self._closed = True
            raise ConnectionError(f"WebSocket closed: {msg.data}")
        elif msg.type == aiohttp.WSMsgType.ERROR:
            self._closed = True
            raise ConnectionError(f"WebSocket error: {self._ws.exception()}")
        else:
            raise ValueError(f"Unexpected message type: {msg.type}")
    
    def abort(self, reason: Exception) -> None:
        """Abort the connection."""
        self._closed = True
        if self._ws:
            asyncio.create_task(self._ws.close())
    
    async def close(self) -> None:
        """Close the connection."""
        self._closed = True
        if self._ws:
            await self._ws.close()
            self._ws = None
        if self._session:
            await self._session.close()
            self._session = None


class WebSocketServerTransport:
    """WebSocket transport for server-side bidirectional RPC.
    
    Wraps an aiohttp WebSocketResponse for use with BidirectionalSession.
    """
    
    def __init__(self, ws: aiohttp.web.WebSocketResponse) -> None:
        """Initialize the transport.
        
        Args:
            ws: The aiohttp WebSocketResponse from the server handler
        """
        self._ws = ws
        self._closed = False
        self._receive_queue: asyncio.Queue[str] = asyncio.Queue()
        self._error: Exception | None = None
    
    def feed_message(self, message: str) -> None:
        """Feed a message received by the server handler."""
        self._receive_queue.put_nowait(message)
    
    def set_error(self, error: Exception) -> None:
        """Signal that an error occurred."""
        self._error = error
        self._closed = True
    
    def set_closed(self) -> None:
        """Signal that the connection was closed."""
        self._closed = True
    
    async def send(self, message: str) -> None:
        """Send a message to the client."""
        if self._closed:
            raise RuntimeError("WebSocket closed")
        await self._ws.send_str(message)
    
    async def receive(self) -> str:
        """Receive a message from the client."""
        if self._error:
            raise self._error
        if self._closed and self._receive_queue.empty():
            raise ConnectionError("WebSocket closed")
        
        return await self._receive_queue.get()
    
    def abort(self, reason: Exception) -> None:
        """Abort the connection."""
        self._closed = True
        asyncio.create_task(self._ws.close())
