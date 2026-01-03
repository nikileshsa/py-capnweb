"""Pytest configuration for all tests."""

import asyncio

# Re-export aiohttp_server fixture for production feature tests
pytest_plugins = ["aiohttp.pytest_plugin"]


class MockTransport:
    """In-memory transport for testing."""

    def __init__(self) -> None:
        self.peer: "MockTransport | None" = None
        self.inbox: asyncio.Queue[str] = asyncio.Queue()
        self.closed = False
        self.message_count = 0

    async def send(self, message: str) -> None:
        """Send message to peer."""
        if self.peer and not self.peer.closed:
            self.message_count += 1
            await self.peer.inbox.put(message)

    async def receive(self) -> str:
        """Receive message from inbox."""
        return await self.inbox.get()

    async def close(self) -> None:
        """Close the transport."""
        self.closed = True


def create_transport_pair() -> tuple[MockTransport, MockTransport]:
    """Create a pair of connected transports."""
    a = MockTransport()
    b = MockTransport()
    a.peer = b
    b.peer = a
    return a, b
