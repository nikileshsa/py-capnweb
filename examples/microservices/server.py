"""Microservices Server - All services in one process with WebSocket transport.

This demonstrates capability-based security where:
- User Service returns User capabilities
- Order Service accepts User capabilities for authorization
- Capabilities are passed between services over WebSocket

Run:
    uv run python examples/microservices/server.py
"""

import asyncio
import logging
from dataclasses import dataclass
from typing import Any

from aiohttp import web

from capnweb.error import RpcError
from capnweb.rpc_session import BidirectionalSession
from capnweb.stubs import RpcStub
from capnweb.types import RpcTarget
from capnweb.ws_transport import WebSocketServerTransport

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =============================================================================
# User Service
# =============================================================================

USERS_DB = {
    "alice": {"id": "user1", "username": "alice", "password": "alice123", "email": "alice@example.com", "role": "admin"},
    "bob": {"id": "user2", "username": "bob", "password": "bob123", "email": "bob@example.com", "role": "user"},
    "charlie": {"id": "user3", "username": "charlie", "password": "charlie123", "email": "charlie@example.com", "role": "user"},
}


@dataclass
class User(RpcTarget):
    """User capability - represents a logged-in user."""

    user_id: str
    username: str
    email: str
    role: str

    async def call(self, method: str, args: list[Any]) -> Any:
        match method:
            case "getId":
                return self.user_id
            case "getUsername":
                return self.username
            case "getEmail":
                return self.email
            case "getRole":
                return self.role
            case "hasPermission":
                return self._has_permission(args[0] if args else "")
            case _:
                raise RpcError.not_found(f"Method '{method}' not found")

    async def get_property(self, name: str) -> Any:
        match name:
            case "id": return self.user_id
            case "username": return self.username
            case "email": return self.email
            case "role": return self.role
            case _: raise RpcError.not_found(f"Property '{name}' not found")

    def _has_permission(self, permission: str) -> bool:
        admin_permissions = {"user.create", "user.delete", "order.cancel", "system.admin"}
        user_permissions = {"order.create", "order.view", "profile.edit"}
        if self.role == "admin":
            return True
        elif self.role == "user":
            return permission in user_permissions
        return False


# =============================================================================
# Order Service
# =============================================================================

@dataclass
class Order:
    order_id: str
    user_id: str
    items: list[dict[str, Any]]
    total: float
    status: str = "pending"


# =============================================================================
# Gateway - Unified service that exposes both User and Order functionality
# =============================================================================

class Gateway(RpcTarget):
    """API Gateway that provides access to User and Order services."""

    def __init__(self) -> None:
        self.sessions: dict[str, User] = {}
        self.token_counter = 0
        self.orders: dict[str, Order] = {}
        self.order_counter = 0

    async def call(self, method: str, args: list[Any]) -> Any:
        match method:
            # User Service methods
            case "login":
                return await self._login(args[0], args[1])
            case "getUserByToken":
                return self._get_user_by_token(args[0])
            case "getUserProfile":
                return self._get_user_profile(args[0])
            case "listUsers":
                return self._list_users()
            # Order Service methods
            case "createOrder":
                return await self._create_order(args[0], args[1])
            case "listOrders":
                return await self._list_orders(args[0])
            case "cancelOrder":
                return await self._cancel_order(args[0], args[1])
            case _:
                raise RpcError.not_found(f"Method '{method}' not found")

    async def get_property(self, name: str) -> Any:
        raise RpcError.not_found(f"Property '{name}' not found")

    # --- User Service methods ---

    async def _login(self, username: str, password: str) -> dict[str, Any]:
        user_data = USERS_DB.get(username)
        if not user_data or user_data["password"] != password:
            raise RpcError.bad_request("Invalid username or password")

        self.token_counter += 1
        token = f"token_{username}_{self.token_counter}"

        user = User(
            user_id=user_data["id"],
            username=user_data["username"],
            email=user_data["email"],
            role=user_data["role"],
        )
        self.sessions[token] = user

        return {
            "token": token,
            "userId": user_data["id"],
            "username": user_data["username"],
            "role": user_data["role"],
        }

    def _get_user_by_token(self, token: str) -> User:
        """Returns a User CAPABILITY."""
        user = self.sessions.get(token)
        if not user:
            raise RpcError.bad_request("Invalid or expired token")
        return user

    def _get_user_profile(self, token: str) -> dict[str, Any]:
        """Returns user profile data (not a capability)."""
        user = self.sessions.get(token)
        if not user:
            raise RpcError.bad_request("Invalid or expired token")
        return {
            "userId": user.user_id,
            "username": user.username,
            "email": user.email,
            "role": user.role,
        }

    def _list_users(self) -> list[dict[str, str]]:
        return [
            {"id": u["id"], "username": u["username"], "role": u["role"]}
            for u in USERS_DB.values()
        ]

    # --- Order Service methods ---

    async def _create_order(self, user_cap: RpcStub, items: list[dict[str, Any]]) -> dict[str, Any]:
        """Create order using User capability for authorization."""
        # Call methods on the capability to get user info and check permission
        user_id = await user_cap.getId()
        has_permission = await user_cap.hasPermission("order.create")

        if not has_permission:
            raise RpcError.permission_denied("You don't have permission to create orders")

        total = sum(item.get("price", 0) * item.get("quantity", 1) for item in items)

        self.order_counter += 1
        order_id = f"order{self.order_counter}"
        order = Order(
            order_id=order_id,
            user_id=user_id,
            items=items,
            total=total,
            status="pending",
        )
        self.orders[order_id] = order

        return {
            "orderId": order_id,
            "total": total,
            "status": order.status,
            "itemCount": len(items),
        }

    async def _list_orders(self, user_cap: RpcStub) -> list[dict[str, Any]]:
        """List orders using User capability."""
        user_id = await user_cap.getId()
        role = await user_cap.getRole()

        if role == "admin":
            orders = list(self.orders.values())
        else:
            orders = [o for o in self.orders.values() if o.user_id == user_id]

        return [
            {"orderId": o.order_id, "total": o.total, "status": o.status}
            for o in orders
        ]

    async def _cancel_order(self, user_cap: RpcStub, order_id: str) -> dict[str, Any]:
        """Cancel order (admin only)."""
        has_permission = await user_cap.hasPermission("order.cancel")

        if not has_permission:
            raise RpcError.permission_denied("You don't have permission to cancel orders")

        order = self.orders.get(order_id)
        if not order:
            raise RpcError.not_found(f"Order '{order_id}' not found")

        order.status = "cancelled"
        return {"orderId": order_id, "status": order.status}


# Global gateway instance
gateway = Gateway()


async def handle_websocket(request: web.Request) -> web.WebSocketResponse:
    """Handle WebSocket connections."""
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    transport = WebSocketServerTransport(ws)
    session = BidirectionalSession(transport, gateway)
    session.start()

    logger.info("Client connected")

    try:
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


async def main() -> None:
    """Run the microservices server."""
    app = web.Application()
    app.router.add_get("/rpc/ws", handle_websocket)

    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(runner, "127.0.0.1", 8080)
    await site.start()

    print("🏢 Microservices Server running on http://127.0.0.1:8080")
    print("   WebSocket endpoint: ws://127.0.0.1:8080/rpc/ws")
    print()
    print("Available users:")
    for username, data in USERS_DB.items():
        print(f"  - {username}:{data['password']} ({data['role']})")
    print()
    print("Run client with: uv run python examples/microservices/client.py")
    print("Press Ctrl+C to stop")

    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
