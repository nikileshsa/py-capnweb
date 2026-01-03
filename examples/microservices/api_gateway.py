"""API Gateway - Routes requests to backend services.

Run:
    uv run python examples/microservices/api_gateway.py
"""

import asyncio
from typing import Any

import aiohttp
from aiohttp import web

from capnweb.batch import aiohttp_batch_rpc_handler, new_http_batch_rpc_session
from capnweb.error import RpcError
from capnweb.types import RpcTarget

USER_SERVICE_URL = "http://127.0.0.1:8081/rpc/batch"
ORDER_SERVICE_URL = "http://127.0.0.1:8082/rpc/batch"


class ApiGateway(RpcTarget):
    """API Gateway - orchestrates requests to backend services."""

    def __init__(self, http_session: aiohttp.ClientSession) -> None:
        self.http_session = http_session

    async def call(self, method: str, args: list[Any]) -> Any:
        """Handle RPC calls."""
        match method:
            case "login":
                return await self._login(args[0], args[1])
            case "getUserProfile":
                return await self._get_user_profile(args[0])
            case "listUsers":
                return await self._list_users(args[0])
            case "createOrder":
                return await self._create_order(args[0], args[1])
            case "listOrders":
                return await self._list_orders(args[0])
            case "cancelOrder":
                return await self._cancel_order(args[0], args[1])
            case _:
                raise RpcError.not_found(f"Method '{method}' not found")

    async def get_property(self, name: str) -> Any:
        """Handle property access."""
        raise RpcError.not_found(f"Property '{name}' not found")

    async def _login(self, username: str, password: str) -> dict[str, Any]:
        """Login via User Service."""
        stub = await new_http_batch_rpc_session(USER_SERVICE_URL, http_client=self.http_session)
        return await stub.authenticate(username, password)

    async def _get_user_profile(self, token: str) -> dict[str, Any]:
        """Get user profile via User Service."""
        stub = await new_http_batch_rpc_session(USER_SERVICE_URL, http_client=self.http_session)
        return await stub.getUserProfile(token)

    async def _list_users(self, token: str) -> list[dict[str, str]]:
        """List users via User Service."""
        # Verify token first
        stub = await new_http_batch_rpc_session(USER_SERVICE_URL, http_client=self.http_session)
        await stub.getUserByToken(token)  # Throws if invalid

        # List users
        stub2 = await new_http_batch_rpc_session(USER_SERVICE_URL, http_client=self.http_session)
        return await stub2.listUsers()

    async def _create_order(self, token: str, items: list[dict[str, Any]]) -> dict[str, Any]:
        """Create order via Order Service."""
        # Pass token to Order Service (it will validate with User Service)
        stub = await new_http_batch_rpc_session(ORDER_SERVICE_URL, http_client=self.http_session)
        return await stub.createOrder(token, items)

    async def _list_orders(self, token: str) -> list[dict[str, Any]]:
        """List orders via Order Service."""
        stub = await new_http_batch_rpc_session(ORDER_SERVICE_URL, http_client=self.http_session)
        return await stub.listOrders(token)

    async def _cancel_order(self, token: str, order_id: str) -> dict[str, Any]:
        """Cancel order via Order Service."""
        stub = await new_http_batch_rpc_session(ORDER_SERVICE_URL, http_client=self.http_session)
        return await stub.cancelOrder(token, order_id)


# Global HTTP session and gateway
http_session: aiohttp.ClientSession | None = None
gateway: ApiGateway | None = None


async def handle_batch(request: web.Request) -> web.Response:
    """Handle batch RPC requests."""
    global gateway
    if gateway is None:
        return web.Response(text="Gateway not initialized", status=500)
    
    return await aiohttp_batch_rpc_handler(request, gateway)


async def main() -> None:
    """Run the API Gateway."""
    global http_session, gateway

    http_session = aiohttp.ClientSession()
    gateway = ApiGateway(http_session)

    app = web.Application()
    app.router.add_post("/rpc/batch", handle_batch)

    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(runner, "127.0.0.1", 8080)
    await site.start()

    print("🌐 API Gateway running on http://127.0.0.1:8080")
    print("   Endpoint: http://127.0.0.1:8080/rpc/batch")
    print()
    print("Backend services:")
    print(f"   User Service:  {USER_SERVICE_URL}")
    print(f"   Order Service: {ORDER_SERVICE_URL}")
    print()
    print("Run client with: uv run python examples/microservices/client.py")
    print("Press Ctrl+C to stop")

    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        await http_session.close()
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
