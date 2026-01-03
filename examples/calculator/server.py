"""Simple calculator server using HTTP batch RPC.

Run:
    uv run python examples/calculator/server.py
"""

import asyncio
from typing import Any

from aiohttp import web

from capnweb.batch import aiohttp_batch_rpc_handler
from capnweb.error import RpcError
from capnweb.types import RpcTarget


class Calculator(RpcTarget):
    """A simple calculator that supports basic arithmetic."""

    async def call(self, method: str, args: list[Any]) -> Any:
        """Handle RPC method calls."""
        match method:
            case "add":
                return args[0] + args[1]
            case "subtract":
                return args[0] - args[1]
            case "multiply":
                return args[0] * args[1]
            case "divide":
                if args[1] == 0:
                    raise RpcError.bad_request("Division by zero")
                return args[0] / args[1]
            case _:
                raise RpcError.not_found(f"Method '{method}' not found")

    async def get_property(self, name: str) -> Any:
        """Handle property access."""
        raise RpcError.not_found(f"Property '{name}' not found")


async def main() -> None:
    """Run the calculator server."""
    calculator = Calculator()

    # Create aiohttp app with batch RPC handler
    async def rpc_handler(request: web.Request) -> web.Response:
        return await aiohttp_batch_rpc_handler(request, calculator)

    app = web.Application()
    app.router.add_post("/rpc/batch", rpc_handler)

    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(runner, "127.0.0.1", 8080)
    await site.start()

    print("🧮 Calculator server running on http://127.0.0.1:8080")
    print("   Endpoint: http://127.0.0.1:8080/rpc/batch")
    print()
    print("Run client with: uv run python examples/calculator/client.py")
    print("Press Ctrl+C to stop")

    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
