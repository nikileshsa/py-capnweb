"""Bob - A peer in the Cap'n Web network.

Run this alongside alice.py to demonstrate peer-to-peer communication.

Usage:
    uv run python examples/peer-to-peer/bob.py
"""

import asyncio
from typing import Any

import aiohttp
from aiohttp import web

from capnweb.batch import aiohttp_batch_rpc_handler, new_http_batch_rpc_session
from capnweb.error import RpcError
from capnweb.types import RpcTarget


class Bob(RpcTarget):
    """Bob's capabilities."""

    def __init__(self) -> None:
        self.name = "Bob"
        self.message_count = 0

    async def call(self, method: str, args: list[Any]) -> Any:
        """Handle RPC calls."""
        match method:
            case "greet":
                return f"Hey there! I'm {self.name}."

            case "chat":
                message = args[0] if args else ""
                self.message_count += 1
                print(f"📨 Bob received: {message}")
                return f"Bob says: Got your message #{self.message_count}!"

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


async def call_alice(http_session: aiohttp.ClientSession) -> None:
    """Connect to Alice and make some calls."""
    print("🔗 Connecting to Alice at http://127.0.0.1:8080...")

    try:
        url = "http://127.0.0.1:8080/rpc/batch"

        # Call Alice.greet()
        print("📞 Bob calls Alice.greet()...")
        stub = await new_http_batch_rpc_session(url, http_client=http_session)
        greeting = await stub.greet()
        print(f"   ← {greeting}")

        # Send Alice a message
        print("📞 Bob calls Alice.chat('Hi Alice!')...")
        stub = await new_http_batch_rpc_session(url, http_client=http_session)
        response = await stub.chat("Hi Alice, this is Bob!")
        print(f"   ← {response}")

        # Get Alice's stats
        print("📞 Bob calls Alice.get_stats()...")
        stub = await new_http_batch_rpc_session(url, http_client=http_session)
        stats = await stub.get_stats()
        print(f"   ← {stats}")

    except Exception as e:
        print(f"❌ Could not connect to Alice: {e}")
        print("   Make sure alice.py is running!")


async def main() -> None:
    """Run Bob's server and demonstrate calling Alice."""
    print("🚀 Starting Bob on port 8081...")

    # Create Bob's server
    bob = Bob()

    async def rpc_handler(request: web.Request) -> web.Response:
        return await aiohttp_batch_rpc_handler(request, bob)

    app = web.Application()
    app.router.add_post("/rpc/batch", rpc_handler)

    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(runner, "127.0.0.1", 8081)
    await site.start()

    print("✅ Bob is running!")
    print("   - Bob exports his capabilities at http://127.0.0.1:8081/rpc/batch")
    print("   - Bob can receive calls from Alice")
    print()

    # Wait a moment
    await asyncio.sleep(2)

    # Try to connect to Alice
    async with aiohttp.ClientSession() as http_session:
        await call_alice(http_session)

    # Keep server running
    print()
    print("⏳ Bob is waiting for calls from Alice...")
    print("   (Press Ctrl+C to stop)")

    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        print("\n👋 Bob shutting down...")
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
