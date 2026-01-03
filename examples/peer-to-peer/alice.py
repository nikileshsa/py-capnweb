"""Alice - A peer in the Cap'n Web network.

Run this alongside bob.py to demonstrate peer-to-peer communication.

Usage:
    uv run python examples/peer-to-peer/alice.py
"""

import asyncio
from typing import Any

import aiohttp
from aiohttp import web

from capnweb.batch import aiohttp_batch_rpc_handler, new_http_batch_rpc_session
from capnweb.error import RpcError
from capnweb.types import RpcTarget


class Alice(RpcTarget):
    """Alice's capabilities."""

    def __init__(self) -> None:
        self.name = "Alice"
        self.message_count = 0

    async def call(self, method: str, args: list[Any]) -> Any:
        """Handle RPC calls."""
        match method:
            case "greet":
                return f"Hello! I'm {self.name}."

            case "chat":
                message = args[0] if args else ""
                self.message_count += 1
                print(f"📨 Alice received: {message}")
                return f"Alice says: Thanks for the message #{self.message_count}!"

            case "get_stats":
                return {
                    "name": self.name,
                    "messages_received": self.message_count,
                }

            case _:
                raise RpcError.not_found(f"Method '{method}' not found")

    async def get_property(self, name: str) -> Any:
        """Get property value."""
        if name == "name":
            return self.name
        raise RpcError.not_found(f"Property '{name}' not found")


async def call_bob(http_session: aiohttp.ClientSession) -> None:
    """Connect to Bob and make some calls."""
    print("🔗 Connecting to Bob at http://127.0.0.1:8081...")

    try:
        url = "http://127.0.0.1:8081/rpc/batch"

        # Call Bob.greet()
        print("📞 Alice calls Bob.greet()...")
        stub = await new_http_batch_rpc_session(url, http_client=http_session)
        greeting = await stub.greet()
        print(f"   ← {greeting}")

        # Send Bob a message
        print("📞 Alice calls Bob.chat('Hi Bob!')...")
        stub = await new_http_batch_rpc_session(url, http_client=http_session)
        response = await stub.chat("Hi Bob, this is Alice!")
        print(f"   ← {response}")

        # Get Bob's stats
        print("📞 Alice calls Bob.get_stats()...")
        stub = await new_http_batch_rpc_session(url, http_client=http_session)
        stats = await stub.get_stats()
        print(f"   ← {stats}")

    except Exception as e:
        print(f"❌ Could not connect to Bob: {e}")
        print("   Make sure bob.py is running!")


async def main() -> None:
    """Run Alice's server and demonstrate calling Bob."""
    print("🚀 Starting Alice on port 8080...")

    # Create Alice's server
    alice = Alice()

    async def rpc_handler(request: web.Request) -> web.Response:
        return await aiohttp_batch_rpc_handler(request, alice)

    app = web.Application()
    app.router.add_post("/rpc/batch", rpc_handler)

    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(runner, "127.0.0.1", 8080)
    await site.start()

    print("✅ Alice is running!")
    print("   - Alice exports her capabilities at http://127.0.0.1:8080/rpc/batch")
    print("   - Alice can receive calls from Bob")
    print()

    # Wait a moment for Bob to start
    await asyncio.sleep(1)

    # Try to connect to Bob
    async with aiohttp.ClientSession() as http_session:
        await call_bob(http_session)

    # Keep server running
    print()
    print("⏳ Alice is waiting for calls from Bob...")
    print("   (Press Ctrl+C to stop)")

    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        print("\n👋 Alice shutting down...")
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
