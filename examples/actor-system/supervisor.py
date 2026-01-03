"""Supervisor - Manages worker actors in a distributed system.

Uses WebSocket transport for persistent connections, enabling
capability passing (returning Worker objects that clients can call).

Run:
    uv run python examples/actor-system/supervisor.py
"""

import asyncio
import logging
from dataclasses import dataclass
from typing import Any

from aiohttp import web

from capnweb.error import RpcError
from capnweb.rpc_session import BidirectionalSession
from capnweb.types import RpcTarget
from capnweb.ws_transport import WebSocketServerTransport

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class Worker(RpcTarget):
    """A worker actor with a counter."""

    name: str
    count: int = 0

    async def call(self, method: str, args: list[Any]) -> Any:
        """Handle RPC calls."""
        match method:
            case "increment":
                self.count += 1
                print(f"  [{self.name}] increment → {self.count}")
                return self.count
            case "decrement":
                self.count -= 1
                print(f"  [{self.name}] decrement → {self.count}")
                return self.count
            case "get_count":
                return self.count
            case "reset":
                self.count = 0
                print(f"  [{self.name}] reset → 0")
                return self.count
            case "get_name":
                return self.name
            case _:
                raise RpcError.not_found(f"Method '{method}' not found")

    async def get_property(self, name: str) -> Any:
        """Handle property access."""
        match name:
            case "name":
                return self.name
            case "count":
                return self.count
            case _:
                raise RpcError.not_found(f"Property '{name}' not found")


class Supervisor(RpcTarget):
    """Supervisor that manages worker actors."""

    def __init__(self) -> None:
        self.workers: dict[str, Worker] = {}

    async def call(self, method: str, args: list[Any]) -> Any:
        """Handle RPC calls."""
        match method:
            case "spawn_worker":
                return self._spawn_worker(args[0] if args else "Worker")
            case "list_workers":
                return self._list_workers()
            case "get_worker":
                return self._get_worker(args[0] if args else "")
            case "terminate_worker":
                return self._terminate_worker(args[0] if args else "")
            case _:
                raise RpcError.not_found(f"Method '{method}' not found")

    async def get_property(self, name: str) -> Any:
        """Handle property access."""
        match name:
            case "worker_count":
                return len(self.workers)
            case _:
                raise RpcError.not_found(f"Property '{name}' not found")

    def _spawn_worker(self, name: str) -> Worker:
        """Spawn a new worker actor."""
        if name in self.workers:
            raise RpcError.bad_request(f"Worker '{name}' already exists")

        worker = Worker(name=name)
        self.workers[name] = worker
        print(f"✓ Spawned worker: {name}")
        return worker

    def _list_workers(self) -> list[str]:
        """List all worker names."""
        return list(self.workers.keys())

    def _get_worker(self, name: str) -> Worker:
        """Get a worker by name."""
        worker = self.workers.get(name)
        if not worker:
            raise RpcError.not_found(f"Worker '{name}' not found")
        return worker

    def _terminate_worker(self, name: str) -> dict[str, str]:
        """Terminate a worker."""
        if name not in self.workers:
            raise RpcError.not_found(f"Worker '{name}' not found")

        del self.workers[name]
        print(f"✗ Terminated worker: {name}")
        return {"status": "terminated", "name": name}


async def handle_websocket(request: web.Request) -> web.WebSocketResponse:
    """Handle WebSocket connections."""
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    # Create transport and session with supervisor
    transport = WebSocketServerTransport(ws)
    session = BidirectionalSession(transport, supervisor)
    session.start()

    logger.info("New client connected")

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
        logger.info("Client disconnected")

    return ws


# Global supervisor instance
supervisor = Supervisor()


async def main() -> None:
    """Run the Supervisor."""
    app = web.Application()
    app.router.add_get("/rpc/ws", handle_websocket)

    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(runner, "127.0.0.1", 8080)
    await site.start()

    print("🎭 Actor System Supervisor running on http://127.0.0.1:8080")
    print("   WebSocket endpoint: ws://127.0.0.1:8080/rpc/ws")
    print()
    print("Methods:")
    print("  - spawn_worker(name) → Worker capability")
    print("  - list_workers() → [names]")
    print("  - get_worker(name) → Worker capability")
    print("  - terminate_worker(name) → status")
    print()
    print("Run client with: uv run python examples/actor-system/client.py")
    print("Press Ctrl+C to stop")

    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
