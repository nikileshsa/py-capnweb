"""Order Service - Manages orders with capability-based authorization.

Run:
    uv run python examples/microservices/order_service.py
"""

import asyncio
from dataclasses import dataclass, field
from typing import Any

from aiohttp import web

from capnweb import RpcError, RpcTarget, RpcStub, aiohttp_batch_rpc_handler


@dataclass
class Order:
    """Order data."""

    order_id: str
    user_id: str
    items: list[dict[str, Any]]
    total: float
    status: str = "pending"


class OrderService(RpcTarget):
    """Order Service - manages orders with capability-based authorization."""

    def __init__(self) -> None:
        self.orders: dict[str, Order] = {}
        self.order_counter = 0

    async def call(self, method: str, args: list[Any]) -> Any:
        """Handle RPC calls."""
        match method:
            case "createOrder":
                return await self._create_order(args[0], args[1])
            case "listOrders":
                return await self._list_orders(args[0])
            case "getOrder":
                return self._get_order(args[0])
            case "cancelOrder":
                return await self._cancel_order(args[0], args[1])
            case _:
                raise RpcError.not_found(f"Method '{method}' not found")

    async def get_property(self, name: str) -> Any:
        """Handle property access."""
        raise RpcError.not_found(f"Property '{name}' not found")

    async def _create_order(self, user_capability: RpcStub, items: list[dict[str, Any]]) -> dict[str, Any]:
        """Create a new order.
        
        Args:
            user_capability: User capability for authorization
            items: List of items to order
        """
        # Get user ID from capability (public API)
        user_id = await user_capability.getId()

        # Check permission (public API)
        has_permission = await user_capability.hasPermission("order.create")

        if not has_permission:
            raise RpcError.permission_denied("You don't have permission to create orders")

        # Calculate total
        total = sum(item.get("price", 0) * item.get("quantity", 1) for item in items)

        # Create order
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

    async def _list_orders(self, user_capability: RpcStub) -> list[dict[str, Any]]:
        """List orders for a user."""
        # Get user ID from capability (public API)
        user_id = await user_capability.getId()

        # Get user role (public API)
        role = await user_capability.getRole()

        # Admins see all orders, users see only their own
        if role == "admin":
            orders = list(self.orders.values())
        else:
            orders = [o for o in self.orders.values() if o.user_id == user_id]

        return [
            {"orderId": o.order_id, "total": o.total, "status": o.status}
            for o in orders
        ]

    def _get_order(self, order_id: str) -> dict[str, Any]:
        """Get order details."""
        order = self.orders.get(order_id)
        if not order:
            raise RpcError.not_found(f"Order '{order_id}' not found")

        return {
            "orderId": order.order_id,
            "userId": order.user_id,
            "items": order.items,
            "total": order.total,
            "status": order.status,
        }

    async def _cancel_order(self, order_id: str, user_capability: RpcStub) -> dict[str, Any]:
        """Cancel an order (admin only)."""
        # Check permission (public API)
        has_permission = await user_capability.hasPermission("order.cancel")

        if not has_permission:
            raise RpcError.permission_denied("You don't have permission to cancel orders")

        order = self.orders.get(order_id)
        if not order:
            raise RpcError.not_found(f"Order '{order_id}' not found")

        order.status = "cancelled"

        return {
            "orderId": order_id,
            "status": order.status,
        }


async def main() -> None:
    """Run the Order Service."""
    order_service = OrderService()

    async def rpc_handler(request: web.Request) -> web.Response:
        return await aiohttp_batch_rpc_handler(request, order_service)

    app = web.Application()
    app.router.add_post("/rpc/batch", rpc_handler)

    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(runner, "127.0.0.1", 8082)
    await site.start()

    print("📦 Order Service running on http://127.0.0.1:8082")
    print("   Endpoint: http://127.0.0.1:8082/rpc/batch")
    print()
    print("Press Ctrl+C to stop")

    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
