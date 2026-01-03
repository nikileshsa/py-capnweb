"""HTTP Batch RPC transport and session helpers.

This module provides HTTP batch RPC support, allowing multiple RPC calls to be
batched into a single HTTP POST request/response. This is useful for:
- Environments without WebSocket support
- Serverless functions (single request/response)
- Reducing connection overhead

Based on the TypeScript reference implementation in batch.ts.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Awaitable

from capnweb.rpc_session import BidirectionalSession, RpcSessionOptions
from capnweb.stubs import RpcStub
from capnweb.hooks import PayloadStubHook
from capnweb.payload import RpcPayload

if TYPE_CHECKING:
    from aiohttp import ClientSession
    from aiohttp.web import Request, Response


SendBatchFunc = Callable[[list[str]], Awaitable[list[str]]]


class BatchClientTransport:
    """In-memory transport for HTTP batch RPC client.
    
    Collects outgoing messages until the batch is sent, then provides
    incoming messages from the response.
    """
    
    __slots__ = (
        '_send_batch', '_batch_to_send', '_batch_to_receive',
        '_batch_sent', '_aborted', '_receive_index'
    )
    
    def __init__(self, send_batch: SendBatchFunc) -> None:
        self._send_batch = send_batch
        self._batch_to_send: list[str] = []
        self._batch_to_receive: list[str] | None = None
        self._batch_sent = asyncio.Event()
        self._aborted: Exception | None = None
        self._receive_index = 0
        
        # Schedule the batch to be sent after microtask queue clears
        asyncio.create_task(self._schedule_batch())
    
    async def send(self, message: str) -> None:
        """Queue a message to be sent in the batch."""
        if self._batch_to_receive is not None:
            # Batch already sent, ignore further messages
            return
        if self._aborted:
            return
        self._batch_to_send.append(message)
    
    async def receive(self) -> str:
        """Receive a message from the batch response."""
        if not self._batch_sent.is_set():
            await self._batch_sent.wait()
        
        if self._aborted:
            raise self._aborted
        
        if self._batch_to_receive is None:
            raise RuntimeError("Batch not yet received")
        
        if self._receive_index < len(self._batch_to_receive):
            msg = self._batch_to_receive[self._receive_index]
            self._receive_index += 1
            return msg
        
        # No more messages - signal end of batch
        raise BatchEndError("Batch RPC request ended.")
    
    def abort(self, reason: Exception) -> None:
        """Abort the transport."""
        self._aborted = reason
        self._batch_sent.set()
    
    async def _schedule_batch(self) -> None:
        """Wait for microtask queue to clear, then send the batch."""
        # Wait a tick to allow all calls to be queued
        await asyncio.sleep(0)
        
        if self._aborted:
            return
        
        try:
            batch = self._batch_to_send
            self._batch_to_send = []  # Clear to prevent further additions
            self._batch_to_receive = await self._send_batch(batch)
        except Exception as e:
            self._aborted = e
        finally:
            self._batch_sent.set()


class BatchServerTransport:
    """In-memory transport for HTTP batch RPC server.
    
    Receives messages from the request batch and collects responses.
    """
    
    __slots__ = (
        '_batch_to_receive', '_batch_to_send', '_receive_index',
        '_all_received', '_aborted'
    )
    
    def __init__(self, batch: list[str]) -> None:
        self._batch_to_receive = batch
        self._batch_to_send: list[str] = []
        self._receive_index = 0
        self._all_received = asyncio.Event()
        self._aborted: Exception | None = None
    
    async def send(self, message: str) -> None:
        """Queue a message for the response."""
        self._batch_to_send.append(message)
    
    async def receive(self) -> str:
        """Get the next message from the request batch."""
        if self._receive_index < len(self._batch_to_receive):
            msg = self._batch_to_receive[self._receive_index]
            self._receive_index += 1
            return msg
        
        # No more messages
        self._all_received.set()
        # Return a future that never resolves (session will drain)
        await asyncio.Event().wait()
        raise RuntimeError("Unreachable")
    
    def abort(self, reason: Exception) -> None:
        """Abort the transport."""
        self._aborted = reason
        self._all_received.set()
    
    async def when_all_received(self) -> None:
        """Wait until all request messages have been received."""
        await self._all_received.wait()
        if self._aborted:
            raise self._aborted
    
    def get_response_body(self) -> str:
        """Get the response body (newline-separated messages)."""
        return "\n".join(self._batch_to_send)


class BatchEndError(Exception):
    """Raised when the batch has ended."""
    pass


async def new_http_batch_rpc_session(
    url: str,
    *,
    http_client: "ClientSession | None" = None,
    options: RpcSessionOptions | None = None,
) -> RpcStub:
    """Start an HTTP batch RPC session as a client.
    
    This creates a session that batches all RPC calls made synchronously
    into a single HTTP POST request.
    
    Args:
        url: The URL to POST the batch to
        http_client: Optional aiohttp ClientSession to use
        options: Optional RPC session options
        
    Returns:
        An RpcStub for the remote main object
        
    Example:
        ```python
        async with aiohttp.ClientSession() as client:
            stub = await new_http_batch_rpc_session(
                "https://api.example.com/rpc",
                http_client=client
            )
            # All these calls are batched into one request
            result1 = await stub.method1()
            result2 = await stub.method2()
        ```
    """
    import aiohttp
    
    own_client = http_client is None
    if own_client:
        http_client = aiohttp.ClientSession()
    
    async def send_batch(batch: list[str]) -> list[str]:
        try:
            body = "\n".join(batch)
            async with http_client.post(url, data=body) as response:
                if not response.ok:
                    text = await response.text()
                    raise RuntimeError(
                        f"RPC request failed: {response.status} {response.reason} - {text}"
                    )
                text = await response.text()
                return text.split("\n") if text else []
        finally:
            if own_client:
                await http_client.close()
    
    transport = BatchClientTransport(send_batch)
    session = BidirectionalSession(transport, None, options)
    session.start()
    
    # Wrap the main hook in an RpcStub for the user
    return RpcStub(session.get_main_stub())


async def new_http_batch_rpc_response(
    request_body: str,
    local_main: Any,
    options: RpcSessionOptions | None = None,
) -> str:
    """Handle an HTTP batch RPC request on the server side.
    
    This processes a batch of RPC messages and returns the response.
    
    Args:
        request_body: The request body (newline-separated messages)
        local_main: The main object to expose to the client
        options: Optional RPC session options
        
    Returns:
        The response body (newline-separated messages)
        
    Example:
        ```python
        @app.post("/rpc")
        async def handle_rpc(request: Request):
            body = await request.text()
            response_body = await new_http_batch_rpc_response(body, MyApi())
            return Response(text=response_body)
        ```
    """
    # Filter empty messages
    batch = [msg for msg in request_body.split("\n") if msg.strip()] if request_body else []
    
    # If no messages, return empty response
    if not batch:
        return ""
    
    transport = BatchServerTransport(batch)
    session = BidirectionalSession(transport, local_main, options)
    
    # Start the session's read loop
    session.start()
    
    # Wait for all messages to be received (with timeout protection)
    try:
        await asyncio.wait_for(transport.when_all_received(), timeout=30.0)
    except asyncio.TimeoutError:
        pass  # Continue with what we have
    
    # Wait for all pending operations to complete
    try:
        await asyncio.wait_for(session.drain(), timeout=30.0)
    except asyncio.TimeoutError:
        pass  # Continue with what we have
    
    return transport.get_response_body()


async def aiohttp_batch_rpc_handler(
    request: "Request",
    local_main: Any,
    options: RpcSessionOptions | None = None,
) -> "Response":
    """AIOHTTP handler for HTTP batch RPC.
    
    This is a convenience function for use with aiohttp web servers.
    
    Args:
        request: The aiohttp Request object
        local_main: The main object to expose to the client
        options: Optional RPC session options
        
    Returns:
        An aiohttp Response object
        
    Example:
        ```python
        from aiohttp import web
        from capnweb.batch import aiohttp_batch_rpc_handler
        
        async def rpc_handler(request):
            return await aiohttp_batch_rpc_handler(request, MyApi())
        
        app = web.Application()
        app.router.add_post('/rpc', rpc_handler)
        ```
    """
    from aiohttp import web
    
    if request.method != "POST":
        return web.Response(
            text="This endpoint only accepts POST requests.",
            status=405
        )
    
    body = await request.text()
    response_body = await new_http_batch_rpc_response(body, local_main, options)
    
    response = web.Response(text=response_body)
    # Allow cross-origin requests (same as TypeScript implementation)
    response.headers["Access-Control-Allow-Origin"] = "*"
    return response


async def fastapi_batch_rpc_handler(
    request_body: str,
    local_main: Any,
    options: RpcSessionOptions | None = None,
) -> str:
    """FastAPI handler for HTTP batch RPC.
    
    This is a convenience function for use with FastAPI.
    
    Args:
        request_body: The request body as a string
        local_main: The main object to expose to the client
        options: Optional RPC session options
        
    Returns:
        The response body as a string
        
    Example:
        ```python
        from fastapi import FastAPI, Request
        from fastapi.responses import PlainTextResponse
        from capnweb.batch import fastapi_batch_rpc_handler
        
        app = FastAPI()
        
        @app.post("/rpc")
        async def rpc_endpoint(request: Request):
            body = await request.body()
            response = await fastapi_batch_rpc_handler(
                body.decode(), MyApi()
            )
            return PlainTextResponse(response)
        ```
    """
    return await new_http_batch_rpc_response(request_body, local_main, options)
