"""WebTransport implementation for Cap'n Web.

This module provides WebTransport server and client using aioquic.
WebTransport offers high-performance bidirectional communication over HTTP/3/QUIC.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, Self
from urllib.parse import urlparse

# Optional aioquic dependency
try:
    from aioquic.asyncio import (  # type: ignore[import-not-found]
        QuicConnectionProtocol,
        connect,
        serve,
    )
    from aioquic.asyncio.protocol import (
        QuicConnectionProtocol as QuicProtocol,  # type: ignore[import-not-found]
    )
    from aioquic.h3.connection import H3_ALPN, H3Connection  # type: ignore[import-not-found]
    from aioquic.h3.events import (  # type: ignore[import-not-found]
        DatagramReceived,
        DataReceived,
        H3Event,
        HeadersReceived,
        WebTransportStreamDataReceived,
    )
    from aioquic.quic.connection import stream_is_unidirectional  # type: ignore[import-not-found]
    from aioquic.quic.configuration import (
        QuicConfiguration,  # type: ignore[import-not-found]
    )
    from aioquic.quic.events import (  # type: ignore[import-not-found]
        ProtocolNegotiated,
        StreamReset,
    )

    WEBTRANSPORT_AVAILABLE = True
except ImportError:
    WEBTRANSPORT_AVAILABLE = False
    QuicConnectionProtocol = object  # type: ignore[assignment,misc]
    QuicProtocol = object  # type: ignore[assignment,misc]
    H3Connection = object  # type: ignore[assignment,misc]
    H3_ALPN = ["h3"]  # type: ignore[misc]

if TYPE_CHECKING:
    from aioquic.quic.events import QuicEvent  # type: ignore[import-not-found]

logger = logging.getLogger(__name__)


class WebTransportClientProtocol(QuicConnectionProtocol):  # type: ignore[misc,valid-type]
    """QUIC client protocol for WebTransport.
    
    Based on Google's reference implementation pattern.
    Uses ProtocolNegotiated event to initialize H3Connection.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._http: H3Connection | None = None
        self._receive_queue: asyncio.Queue[bytes] = asyncio.Queue()
        self._datagram_queue: asyncio.Queue[bytes] = asyncio.Queue()
        self._stream_id: int | None = None
        self._session_id: int | None = None
        self._session_ready = asyncio.Event()

    def quic_event_received(self, event: QuicEvent) -> None:
        """Handle QUIC events.

        Args:
            event: QUIC event from the connection
        """
        # Initialize H3Connection on ProtocolNegotiated (like reference impl)
        if isinstance(event, ProtocolNegotiated):
            self._http = H3Connection(self._quic, enable_webtransport=True)  # type: ignore[attr-defined]

        # Process through H3 if initialized
        if self._http is not None:
            for h3_event in self._http.handle_event(event):  # type: ignore[attr-defined]
                self._h3_event_received(h3_event)

    def _h3_event_received(self, event: H3Event) -> None:
        """Handle HTTP/3 events.

        Args:
            event: HTTP/3 event
        """
        if isinstance(event, HeadersReceived):
            # WebTransport session established
            logger.debug(
                "WebTransport session established on stream %s", event.stream_id
            )
            self._session_id = event.stream_id

        elif isinstance(event, DatagramReceived):
            # Datagram received
            logger.debug("Client received datagram: %d bytes", len(event.data))
            self._datagram_queue.put_nowait(event.data)

        elif isinstance(event, (DataReceived, WebTransportStreamDataReceived)):
            # Received data on a stream
            logger.debug(
                "Received %d bytes on stream %s", len(event.data), event.stream_id
            )
            self._receive_queue.put_nowait(event.data)

    async def connect_webtransport(self, path: str) -> int:
        """Establish a WebTransport session via HTTP/3 CONNECT.

        Args:
            path: The path for the WebTransport endpoint (e.g., "/rpc/wt")

        Returns:
            The session stream ID
        """
        # Wait for H3Connection to be initialized via ProtocolNegotiated
        # Give it a short time to initialize
        for _ in range(50):  # 5 seconds max
            if self._http is not None:
                break
            await asyncio.sleep(0.1)

        if self._http is None:
            msg = "H3Connection not initialized - protocol negotiation may have failed"
            raise RuntimeError(msg)

        # Create a new stream for the CONNECT request
        stream_id = self._quic.get_next_available_stream_id(is_unidirectional=False)  # type: ignore[attr-defined]
        self._session_id = stream_id

        # Send HTTP/3 CONNECT request with :protocol=webtransport
        # This follows RFC 9220 (WebTransport over HTTP/3)
        self._http.send_headers(  # type: ignore[attr-defined]
            stream_id=stream_id,
            headers=[
                (b":method", b"CONNECT"),
                (b":protocol", b"webtransport"),
                (b":scheme", b"https"),
                (b":authority", self._authority.encode() if hasattr(self, '_authority') else b"localhost"),
                (b":path", path.encode()),
                (b"sec-webtransport-http3-draft", b"draft02"),
            ],
        )
        self.transmit()  # type: ignore[attr-defined]

        return stream_id

    async def send_data(self, data: bytes, end_stream: bool = False) -> None:
        """Send data on a WebTransport stream.

        Args:
            data: Data to send
            end_stream: Whether to end the stream after sending
        """
        if self._http is None:
            msg = "H3Connection not initialized - call connect_webtransport first"
            raise RuntimeError(msg)

        if self._session_id is None:
            msg = "WebTransport session not established - call connect_webtransport first"
            raise RuntimeError(msg)

        # Create a new bidirectional stream within the WebTransport session
        if self._stream_id is None:
            self._stream_id = self._http.create_webtransport_stream(  # type: ignore[attr-defined]
                session_id=self._session_id,
                is_unidirectional=False
            )
            # Register the stream in H3's stream tracking so responses are processed
            # This is needed because create_webtransport_stream doesn't register bidi streams
            with self._http._get_or_create_stream(self._stream_id) as stream:  # type: ignore[attr-defined]
                from aioquic.h3.connection import StreamType, FrameType
                stream.stream_type = StreamType.WEBTRANSPORT
                stream.frame_type = FrameType.WEBTRANSPORT_STREAM
                stream.session_id = self._session_id

        # Send data on the stream
        self._http._quic.send_stream_data(self._stream_id, data, end_stream=end_stream)  # type: ignore[attr-defined]
        self.transmit()  # type: ignore[attr-defined]

    async def receive_data(self, timeout: float | None = None) -> bytes:
        """Receive data from WebTransport stream.

        Args:
            timeout: Optional timeout in seconds

        Returns:
            Received data

        Raises:
            asyncio.TimeoutError: If timeout expires
        """
        if timeout:
            return await asyncio.wait_for(self._receive_queue.get(), timeout=timeout)
        return await self._receive_queue.get()

    async def send_datagram(self, data: bytes) -> None:
        """Send a datagram on the WebTransport session.

        Args:
            data: Data to send as datagram
        """
        if self._http is None or self._session_id is None:
            msg = "WebTransport session not established"
            raise RuntimeError(msg)

        self._http.send_datagram(self._session_id, data)  # type: ignore[attr-defined]
        self.transmit()  # type: ignore[attr-defined]

    async def receive_datagram(self, timeout: float | None = None) -> bytes:
        """Receive a datagram from the WebTransport session.

        Args:
            timeout: Optional timeout in seconds

        Returns:
            Received datagram data

        Raises:
            asyncio.TimeoutError: If timeout expires
        """
        if timeout:
            return await asyncio.wait_for(self._datagram_queue.get(), timeout=timeout)
        return await self._datagram_queue.get()

    async def create_unidirectional_stream(self) -> int:
        """Create a new unidirectional stream for sending data.

        Returns:
            The new stream ID
        """
        if self._http is None or self._session_id is None:
            msg = "WebTransport session not established"
            raise RuntimeError(msg)

        stream_id = self._http.create_webtransport_stream(  # type: ignore[attr-defined]
            session_id=self._session_id,
            is_unidirectional=True
        )
        return stream_id

    async def send_on_stream(self, stream_id: int, data: bytes, end_stream: bool = False) -> None:
        """Send data on a specific stream.

        Args:
            stream_id: Stream ID to send on
            data: Data to send
            end_stream: Whether to end the stream after sending
        """
        if self._http is None:
            msg = "WebTransport session not established"
            raise RuntimeError(msg)

        self._http._quic.send_stream_data(stream_id, data, end_stream=end_stream)  # type: ignore[attr-defined]
        self.transmit()  # type: ignore[attr-defined]


class WebTransportServerProtocol(QuicConnectionProtocol):  # type: ignore[misc,valid-type]
    """QUIC server protocol for WebTransport.
    
    Based on Google's reference implementation:
    https://github.com/niccolozanotti/niccolozanotti.github.io/blob/main/webtransport/samples/echo/py-server/ipv4_echo_server.py
    
    Key features:
    - Uses ProtocolNegotiated event to initialize H3Connection
    - Validates CONNECT request with :method=CONNECT and :protocol=webtransport
    - Tracks session_id for each WebTransport session
    - Routes WebTransportStreamDataReceived to the correct handler
    """

    def __init__(self, *args: Any, handler: Any = None, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._http: H3Connection | None = None
        self._handler = handler
        self._session_id: int | None = None
        self._sessions: dict[int, asyncio.Queue[Any]] = {}
        # Payload accumulation for streams (like Google reference)
        self._payloads: dict[int, bytearray] = {}
        # Datagram queue
        self._datagram_queue: asyncio.Queue[bytes] = asyncio.Queue()

    def quic_event_received(self, event: QuicEvent) -> None:
        """Handle QUIC events.

        Args:
            event: QUIC event from the connection
        """
        # Initialize H3Connection on ProtocolNegotiated (like reference impl)
        if isinstance(event, ProtocolNegotiated):
            self._http = H3Connection(self._quic, enable_webtransport=True)  # type: ignore[attr-defined]
        elif isinstance(event, StreamReset) and self._session_id is not None:
            # Handle stream reset
            if event.stream_id in self._sessions:
                del self._sessions[event.stream_id]

        # Process through H3 if initialized
        if self._http is not None:
            for h3_event in self._http.handle_event(event):  # type: ignore[attr-defined]
                self._h3_event_received(h3_event)

    def _h3_event_received(self, event: H3Event) -> None:
        """Handle HTTP/3 events.

        Args:
            event: HTTP/3 event
        """
        if isinstance(event, HeadersReceived):
            # Parse headers into dict
            headers: dict[bytes, bytes] = {}
            for header, value in event.headers:
                headers[header] = value

            # Check for WebTransport CONNECT request
            if (headers.get(b":method") == b"CONNECT" and
                    headers.get(b":protocol") == b"webtransport"):
                self._handshake_webtransport(event.stream_id, headers)
            else:
                # Not a WebTransport request - send 400
                self._send_response(event.stream_id, 400, end_stream=True)

        elif isinstance(event, DatagramReceived):
            # Datagram received (like Google reference)
            logger.debug("Received datagram: %d bytes", len(event.data))
            self._datagram_queue.put_nowait(event.data)

        elif isinstance(event, WebTransportStreamDataReceived):
            # Data received on a WebTransport stream
            logger.debug(
                "Received %d bytes on stream %s (session %s, ended=%s)",
                len(event.data), event.stream_id, self._session_id, event.stream_ended
            )

            # Accumulate payload (like Google reference)
            if event.stream_id not in self._payloads:
                self._payloads[event.stream_id] = bytearray()
            self._payloads[event.stream_id] += event.data

            # Route to the stream's queue
            if event.stream_id not in self._sessions:
                self._sessions[event.stream_id] = asyncio.Queue()
            
            # Include stream_ended flag in the tuple
            self._sessions[event.stream_id].put_nowait((
                event.stream_id, 
                event.data, 
                event.stream_ended,
                stream_is_unidirectional(event.stream_id)
            ))

        elif isinstance(event, DataReceived):
            # Regular H3 data (not WebTransport stream)
            logger.debug(
                "Received H3 data %d bytes on stream %s",
                len(event.data), event.stream_id
            )
            # Route to session queue if we have a session
            if self._session_id is not None:
                if self._session_id not in self._sessions:
                    self._sessions[self._session_id] = asyncio.Queue()
                self._sessions[self._session_id].put_nowait(event.data)

    def _handshake_webtransport(
        self,
        stream_id: int,
        request_headers: dict[bytes, bytes]
    ) -> None:
        """Handle WebTransport CONNECT handshake.

        Args:
            stream_id: The stream ID for this session
            request_headers: The request headers
        """
        authority = request_headers.get(b":authority")
        path = request_headers.get(b":path")

        if authority is None or path is None:
            # :authority and :path must be provided
            self._send_response(stream_id, 400, end_stream=True)
            return

        logger.info(
            "WebTransport session request: authority=%s, path=%s, stream=%s",
            authority.decode(), path.decode(), stream_id
        )

        # Accept the session
        self._session_id = stream_id
        self._sessions[stream_id] = asyncio.Queue()
        self._send_response(stream_id, 200)

        # Notify handler if available
        if self._handler:
            asyncio.create_task(self._handler(self, stream_id))

    def _send_response(
        self,
        stream_id: int,
        status_code: int,
        end_stream: bool = False
    ) -> None:
        """Send HTTP/3 response.

        Args:
            stream_id: Stream ID to respond on
            status_code: HTTP status code
            end_stream: Whether to end the stream
        """
        headers = [(b":status", str(status_code).encode())]
        if status_code == 200:
            headers.append((b"sec-webtransport-http3-draft", b"draft02"))

        if self._http:
            self._http.send_headers(  # type: ignore[attr-defined]
                stream_id=stream_id,
                headers=headers,
                end_stream=end_stream
            )
            self.transmit()  # type: ignore[attr-defined]

    async def send_data(self, stream_id: int, data: bytes, end_stream: bool = False) -> None:
        """Send data on a WebTransport stream.

        Args:
            stream_id: Stream ID to send on (use session_id for bidirectional echo)
            data: Data to send
            end_stream: Whether to end the stream after sending
        """
        if self._http:
            self._http._quic.send_stream_data(stream_id, data, end_stream=end_stream)  # type: ignore[attr-defined]
            self.transmit()  # type: ignore[attr-defined]

    async def receive_data(self, stream_id: int, timeout: float | None = None) -> bytes:
        """Receive data from a specific WebTransport stream.

        Args:
            stream_id: Stream ID to receive from
            timeout: Optional timeout in seconds

        Returns:
            Received data

        Raises:
            asyncio.TimeoutError: If timeout expires
            KeyError: If stream doesn't exist
        """
        if stream_id not in self._sessions:
            msg = f"Stream {stream_id} not found"
            raise KeyError(msg)

        queue = self._sessions[stream_id]
        if timeout:
            result = await asyncio.wait_for(queue.get(), timeout=timeout)
        else:
            result = await queue.get()
        
        # Handle both old format (bytes) and new format (stream_id, bytes)
        if isinstance(result, tuple):
            return result[1]
        return result

    async def receive_any(self, timeout: float | None = None) -> tuple[int, bytes, bool, bool]:
        """Receive data from any WebTransport stream.

        This waits for data on any stream and returns the stream_id,
        data, stream_ended flag, and is_unidirectional flag.

        Args:
            timeout: Optional timeout in seconds

        Returns:
            Tuple of (stream_id, data, stream_ended, is_unidirectional)

        Raises:
            asyncio.TimeoutError: If timeout expires
        """
        async def wait_for_any() -> tuple[int, bytes, bool, bool]:
            while True:
                for sid, queue in list(self._sessions.items()):
                    if not queue.empty():
                        result = queue.get_nowait()
                        if isinstance(result, tuple) and len(result) == 4:
                            return result
                        elif isinstance(result, tuple) and len(result) == 2:
                            return (result[0], result[1], False, False)
                        return (sid, result, False, False)
                await asyncio.sleep(0.01)

        if timeout:
            return await asyncio.wait_for(wait_for_any(), timeout=timeout)
        return await wait_for_any()

    async def send_datagram(self, data: bytes) -> None:
        """Send a datagram on the WebTransport session.

        Args:
            data: Data to send as datagram
        """
        if self._http and self._session_id is not None:
            self._http.send_datagram(self._session_id, data)  # type: ignore[attr-defined]
            self.transmit()  # type: ignore[attr-defined]

    async def receive_datagram(self, timeout: float | None = None) -> bytes:
        """Receive a datagram from the WebTransport session.

        Args:
            timeout: Optional timeout in seconds

        Returns:
            Received datagram data

        Raises:
            asyncio.TimeoutError: If timeout expires
        """
        if timeout:
            return await asyncio.wait_for(self._datagram_queue.get(), timeout=timeout)
        return await self._datagram_queue.get()

    def get_accumulated_payload(self, stream_id: int) -> bytes:
        """Get the accumulated payload for a stream.

        Args:
            stream_id: Stream ID to get payload for

        Returns:
            Accumulated payload bytes
        """
        return bytes(self._payloads.get(stream_id, bytearray()))

    def clear_payload(self, stream_id: int) -> None:
        """Clear the accumulated payload for a stream.

        Args:
            stream_id: Stream ID to clear payload for
        """
        if stream_id in self._payloads:
            del self._payloads[stream_id]

    async def create_unidirectional_stream(self) -> int:
        """Create a new unidirectional stream for sending data.

        Returns:
            The new stream ID
        """
        if self._http and self._session_id is not None:
            return self._http.create_webtransport_stream(  # type: ignore[attr-defined]
                session_id=self._session_id,
                is_unidirectional=True
            )
        raise RuntimeError("Session not established")


class WebTransportClient:
    """WebTransport client for Cap'n Web.

    Example:
        ```python
        async with WebTransportClient("https://localhost:4433/rpc/wt") as client:
            await client.send(b"hello")
            response = await client.receive()
        ```
    """

    def __init__(
        self,
        url: str,
        cert_path: str | None = None,
        verify_mode: bool = False,
    ) -> None:
        """Initialize WebTransport client.

        Args:
            url: WebTransport URL (must use https://)
            cert_path: Path to CA certificate for verification
            verify_mode: Whether to verify server certificate (default: False for development)
        """
        if not WEBTRANSPORT_AVAILABLE:
            msg = "WebTransport requires aioquic: pip install aioquic"
            raise RuntimeError(msg)

        self.url = url
        self.cert_path = cert_path
        self.verify_mode = verify_mode
        self._protocol: WebTransportClientProtocol | None = None
        self._task: asyncio.Task | None = None

    async def __aenter__(self) -> Self:
        """Enter async context manager."""
        await self.connect()
        return self

    async def __aexit__(self, *args: object) -> None:
        """Exit async context manager."""
        await self.close()

    async def connect(self) -> None:
        """Establish WebTransport connection."""
        # Parse URL
        parsed = urlparse(self.url)
        if parsed.scheme != "https":
            msg = "WebTransport requires HTTPS URL"
            raise ValueError(msg)

        host = parsed.hostname or "localhost"
        port = parsed.port or 4433
        path = parsed.path or "/"
        authority = f"{host}:{port}"

        # Create QUIC configuration
        # NOTE: max_datagram_frame_size is required for WebTransport to work
        # Without it, the connection closes during connect() (GitHub Issue #3)
        configuration = QuicConfiguration(
            is_client=True,
            alpn_protocols=["h3"],
            max_datagram_frame_size=65536,
        )

        if not self.verify_mode:
            configuration.verify_mode = False
        elif self.cert_path:
            configuration.load_verify_locations(self.cert_path)

        # Connect
        logger.info("Connecting to %s:%s", host, port)

        # Event to signal when WebTransport session is established
        self._connected = asyncio.Event()

        # Store protocol for later use
        async def run_client() -> None:
            async with connect(
                host,
                port,
                configuration=configuration,
                create_protocol=WebTransportClientProtocol,
            ) as protocol:
                self._protocol = protocol  # type: ignore[assignment]
                # Store authority for CONNECT request
                protocol._authority = authority  # type: ignore[attr-defined]
                
                # Send WebTransport CONNECT request to establish session
                await protocol.connect_webtransport(path)
                
                # Signal that we're connected
                self._connected.set()
                
                # Keep connection alive
                await asyncio.Event().wait()

        self._task = asyncio.create_task(run_client())

        # Wait for WebTransport session to be established
        # Use longer timeout (10s) to handle system load during parallel test execution
        try:
            await asyncio.wait_for(self._connected.wait(), timeout=10.0)
        except asyncio.TimeoutError:
            logger.warning("WebTransport connection timed out after 10s")
            raise

    async def send(self, data: bytes) -> None:
        """Send data over WebTransport.

        Args:
            data: Data to send

        Raises:
            RuntimeError: If not connected
        """
        if not self._protocol:
            msg = "Not connected"
            raise RuntimeError(msg)

        await self._protocol.send_data(data)

    async def receive(self, timeout: float | None = None) -> bytes:
        """Receive data from WebTransport.

        Args:
            timeout: Optional timeout in seconds

        Returns:
            Received data

        Raises:
            RuntimeError: If not connected
            asyncio.TimeoutError: If timeout expires
        """
        if not self._protocol:
            msg = "Not connected"
            raise RuntimeError(msg)

        return await self._protocol.receive_data(timeout=timeout)

    async def send_and_receive(
        self, data: bytes, timeout: float | None = None
    ) -> bytes:
        """Send data and wait for response.

        Args:
            data: Data to send
            timeout: Optional timeout in seconds

        Returns:
            Response data
        """
        await self.send(data)
        return await self.receive(timeout=timeout)

    async def close(self) -> None:
        """Close the WebTransport connection."""
        if self._protocol:
            self._protocol._quic.close()
            self._protocol = None

        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None


class WebTransportServer:
    """WebTransport server for Cap'n Web.

    Example:
        ```python
        async def handler(protocol, stream_id):
            data = await protocol.receive_data(stream_id)
            await protocol.send_data(stream_id, b"response")

        server = WebTransportServer("localhost", 4433, cert_path, key_path, handler)
        await server.serve()
        ```
    """

    def __init__(
        self,
        host: str,
        port: int,
        cert_path: str | Path,
        key_path: str | Path,
        handler: Any = None,
    ) -> None:
        """Initialize WebTransport server.

        Args:
            host: Host to bind to
            port: Port to bind to
            cert_path: Path to SSL certificate
            key_path: Path to SSL private key
            handler: Async handler function(protocol, stream_id)
        """
        if not WEBTRANSPORT_AVAILABLE:
            msg = "WebTransport requires aioquic: pip install aioquic"
            raise RuntimeError(msg)

        self.host = host
        self.port = port
        self.cert_path = Path(cert_path)
        self.key_path = Path(key_path)
        self.handler = handler
        self._server: Any = None  # QuicServer from aioquic

    async def serve(self) -> None:
        """Start serving WebTransport connections.

        This method runs forever until the server is closed.
        """
        # Create QUIC configuration
        # NOTE: max_datagram_frame_size is required for WebTransport to work
        # Without it, the connection closes during connect() (GitHub Issue #3)
        configuration = QuicConfiguration(
            is_client=False,
            alpn_protocols=["h3"],
            max_datagram_frame_size=65536,
        )

        # Load certificate and key
        configuration.load_cert_chain(self.cert_path, self.key_path)  # type: ignore[arg-type]

        # Create protocol factory
        def create_protocol(*args: Any, **kwargs: Any) -> WebTransportServerProtocol:
            return WebTransportServerProtocol(*args, handler=self.handler, **kwargs)

        # Start server
        logger.info("Starting WebTransport server on %s:%s", self.host, self.port)

        self._server = await serve(
            self.host,
            self.port,
            configuration=configuration,
            create_protocol=create_protocol,
        )

        # Wait forever (server runs in background)
        await asyncio.Event().wait()

    async def close(self) -> None:
        """Stop the server."""
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
