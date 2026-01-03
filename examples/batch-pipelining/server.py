"""HTTP batch RPC server demonstrating pipelining.

Usage:
    uv run python examples/batch-pipelining/server.py
"""

import asyncio
import os
from typing import Any

from aiohttp import web

from capnweb.batch import aiohttp_batch_rpc_handler
from capnweb.error import RpcError
from capnweb.types import RpcTarget

# Simple in-memory data
USERS = {
    "cookie-123": {"id": "u_1", "name": "Ada Lovelace"},
    "cookie-456": {"id": "u_2", "name": "Alan Turing"},
}

PROFILES = {
    "u_1": {"id": "u_1", "bio": "Mathematician & first programmer"},
    "u_2": {"id": "u_2", "bio": "Mathematician & computer science pioneer"},
}

NOTIFICATIONS = {
    "u_1": ["Welcome to capnweb!", "You have 2 new followers"],
    "u_2": ["New feature: pipelining!", "Security tips for your account"],
}


class Api(RpcTarget):
    """Server-side API implementation with simulated delays."""

    async def call(self, method: str, args: list[Any]) -> Any:
        """Handle RPC calls by delegating to specific methods."""
        match method:
            case "authenticate":
                return await self._authenticate(args[0] if args else "")
            case "getUserProfile":
                return await self._get_user_profile(args[0] if args else "")
            case "getNotifications":
                return await self._get_notifications(args[0] if args else "")
            case _:
                raise RpcError.not_found(f"Method '{method}' not found")

    async def get_property(self, name: str) -> Any:
        """Get property value."""
        raise RpcError.not_found(f"Property '{name}' not found")

    async def _authenticate(self, session_token: str) -> dict[str, str]:
        """Simulate authentication from a session cookie/token."""
        delay_ms = int(os.getenv("DELAY_AUTH_MS", "80"))
        await asyncio.sleep(delay_ms / 1000.0)

        user = USERS.get(session_token)
        if not user:
            raise RpcError.bad_request("Invalid session token")
        return user

    async def _get_user_profile(self, user_id: str) -> dict[str, str]:
        """Get user profile by ID."""
        delay_ms = int(os.getenv("DELAY_PROFILE_MS", "120"))
        await asyncio.sleep(delay_ms / 1000.0)

        profile = PROFILES.get(user_id)
        if not profile:
            raise RpcError.not_found(f"User '{user_id}' not found")
        return profile

    async def _get_notifications(self, user_id: str) -> list[str]:
        """Get notifications for user."""
        delay_ms = int(os.getenv("DELAY_NOTIFS_MS", "120"))
        await asyncio.sleep(delay_ms / 1000.0)

        return NOTIFICATIONS.get(user_id, [])


async def main() -> None:
    """Run the RPC server."""
    port = int(os.getenv("PORT", "3000"))

    api = Api()

    async def rpc_handler(request: web.Request) -> web.Response:
        return await aiohttp_batch_rpc_handler(request, api)

    app = web.Application()
    app.router.add_post("/rpc/batch", rpc_handler)

    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(runner, "localhost", port)
    await site.start()

    print(f"🚀 RPC server listening on http://localhost:{port}/rpc/batch")
    print()
    print("Available methods:")
    print("  - authenticate(token) → {id, name}")
    print("  - getUserProfile(userId) → {id, bio}")
    print("  - getNotifications(userId) → [...]")
    print()
    print("Run client with: uv run python examples/batch-pipelining/client.py")
    print("Press Ctrl+C to stop")

    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
