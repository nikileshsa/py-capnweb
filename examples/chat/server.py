"""Real-time chat server using WebSocket transport.

This example demonstrates:
- WebSocket transport for persistent connections
- Bidirectional RPC (server can call client methods)
- Multiple connected clients
- Broadcasting messages to all clients

Run:
    uv run python examples/chat/server.py
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from aiohttp import web

from capnweb import RpcError, RpcTarget, RpcStub, BidirectionalSession
from capnweb.ws_transport import WebSocketServerTransport

if TYPE_CHECKING:
    pass

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class ChatRoom(RpcTarget):
    """Chat room that manages multiple clients and broadcasts messages."""

    clients: dict[str, tuple[RpcStub, BidirectionalSession]] = field(default_factory=dict)
    message_history: list[dict[str, str]] = field(default_factory=list)

    async def call(self, method: str, args: list[Any]) -> Any:
        """Handle RPC method calls."""
        match method:
            case "join":
                return await self._join(args[0], args[1])
            case "leave":
                return await self._leave(args[0])
            case "sendMessage":
                return await self._send_message(args[0], args[1])
            case "getHistory":
                return self._get_history()
            case "listUsers":
                return self._list_users()
            case _:
                raise RpcError.not_found(f"Method '{method}' not found")

    async def get_property(self, name: str) -> Any:
        """Handle property access."""
        match name:
            case "userCount":
                return len(self.clients)
            case "messageCount":
                return len(self.message_history)
            case _:
                raise RpcError.not_found(f"Property '{name}' not found")

    async def _join(self, username: str, client_callback: RpcStub) -> dict[str, Any]:
        """Add a client to the chat room."""
        if username in self.clients:
            raise RpcError.bad_request(f"Username '{username}' is already taken")

        # Store client callback (we don't have session reference here, store None)
        self.clients[username] = (client_callback, None)  # type: ignore

        # Notify all other clients
        join_msg = f"{username} joined the chat"
        await self._broadcast_system_message(join_msg, exclude=username)

        logger.info("User %s joined (total: %d users)", username, len(self.clients))

        return {
            "message": f"Welcome to the chat, {username}!",
            "userCount": len(self.clients),
            "users": list(self.clients.keys()),
        }

    async def _leave(self, username: str) -> dict[str, str]:
        """Remove a client from the chat room."""
        if username not in self.clients:
            raise RpcError.not_found(f"User '{username}' not found")

        # Remove client
        client_callback, _ = self.clients.pop(username)
        client_callback.dispose()

        # Notify remaining clients
        leave_msg = f"{username} left the chat"
        await self._broadcast_system_message(leave_msg)

        logger.info("User %s left (remaining: %d users)", username, len(self.clients))

        return {"message": f"Goodbye, {username}!"}

    async def _send_message(self, username: str, text: str) -> dict[str, Any]:
        """Broadcast a message to all clients."""
        if username not in self.clients:
            raise RpcError.not_found(f"User '{username}' not in chat room")

        # Store in history
        message = {"username": username, "text": text, "type": "chat"}
        self.message_history.append(message)

        # Broadcast to all clients
        await self._broadcast_message(message)

        logger.info("[%s] %s", username, text)

        return {"status": "sent", "timestamp": len(self.message_history)}

    def _get_history(self) -> list[dict[str, str]]:
        """Get recent message history."""
        return self.message_history[-50:]

    def _list_users(self) -> list[str]:
        """Get list of connected users."""
        return list(self.clients.keys())

    async def _broadcast_message(self, message: dict[str, str]) -> None:
        """Broadcast a message to all clients."""
        for username, (client_callback, _) in list(self.clients.items()):
            try:
                # Call the client's onMessage method via RPC (public API)
                await client_callback.onMessage(message)
            except Exception as e:
                logger.error("Error broadcasting to %s: %s", username, e)
                # Remove unreachable client
                if username in self.clients:
                    self.clients.pop(username)[0].dispose()

    async def _broadcast_system_message(self, text: str, exclude: str | None = None) -> None:
        """Broadcast a system message to all clients."""
        message = {"username": "System", "text": text, "type": "system"}
        self.message_history.append(message)

        for username, (client_callback, _) in list(self.clients.items()):
            if username != exclude:
                try:
                    # Call the client's onMessage method via RPC (public API)
                    await client_callback.onMessage(message)
                except Exception as e:
                    logger.error("Error sending system message to %s: %s", username, e)


# Global chat room instance
chat_room = ChatRoom()


async def handle_websocket(request: web.Request) -> web.WebSocketResponse:
    """Handle WebSocket connections for chat."""
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    # Create transport and session
    transport = WebSocketServerTransport(ws)
    session = BidirectionalSession(transport, chat_room)
    session.start()

    logger.info("New WebSocket connection")

    try:
        # Read messages from WebSocket and feed to transport
        async for msg in ws:
            if msg.type == web.WSMsgType.TEXT:
                transport.feed_message(msg.data)
            elif msg.type == web.WSMsgType.BINARY:
                transport.feed_message(msg.data.decode("utf-8"))
            elif msg.type == web.WSMsgType.ERROR:
                transport.set_error(ws.exception() or Exception("WebSocket error"))
                break
    except Exception as e:
        logger.error("WebSocket error: %s", e)
        transport.set_error(e)
    finally:
        transport.set_closed()
        logger.info("WebSocket connection closed")

    return ws


async def main() -> None:
    """Run the chat server."""
    app = web.Application()
    app.router.add_get("/rpc/ws", handle_websocket)

    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(runner, "127.0.0.1", 8080)
    await site.start()

    logger.info("💬 Chat server running on http://127.0.0.1:8080")
    logger.info("   WebSocket endpoint: ws://127.0.0.1:8080/rpc/ws")
    logger.info("")
    logger.info("Run clients with: uv run python examples/chat/client.py")
    logger.info("Press Ctrl+C to stop")

    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        logger.info("\nShutting down chat server...")
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
