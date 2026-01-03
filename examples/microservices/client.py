"""Microservices Client - Demonstrates capability-based security.

This client connects via WebSocket to demonstrate capability passing:
- Login returns a token
- getUserByToken returns a User CAPABILITY (not just data!)
- The capability is passed to createOrder/listOrders for authorization
- Order Service calls methods on the User capability

Run (after starting server.py):
    uv run python examples/microservices/client.py
"""

import asyncio
from typing import Any

from capnweb.error import RpcError
from capnweb.rpc_session import BidirectionalSession
from capnweb.stubs import RpcStub
from capnweb.types import RpcTarget
from capnweb.ws_transport import WebSocketClientTransport

SERVER_URL = "ws://127.0.0.1:8080/rpc/ws"


class DummyTarget(RpcTarget):
    """Dummy target for client side."""
    async def call(self, method: str, args: list[Any]) -> Any:
        raise RpcError.not_found(f"Method '{method}' not found")
    async def get_property(self, name: str) -> Any:
        raise RpcError.not_found(f"Property '{name}' not found")


async def main() -> None:
    """Run the microservices demo."""
    print("=== Microservices Demo (Capability-Based Security) ===")
    print()

    # Connect via WebSocket
    transport = WebSocketClientTransport(SERVER_URL)
    await transport.connect()
    
    session = BidirectionalSession(transport, DummyTarget())
    session.start()
    
    # Get the gateway stub
    gateway = RpcStub(session.get_main_stub())

    try:
        # 1. Login as Bob (regular user)
        print("1. Login as Bob (regular user)")
        bob_login = await gateway.login("bob", "bob123")
        bob_token = bob_login["token"]
        print(f"   ✓ Logged in as {bob_login['username']} (role: {bob_login['role']})")

        # 2. Get User CAPABILITY (not just profile data!)
        print("\n2. Get User CAPABILITY from token")
        bob_cap = await gateway.getUserByToken(bob_token)
        print(f"   ✓ Got User capability: {type(bob_cap).__name__}")
        
        # Verify we can call methods on the capability
        user_id = await bob_cap.getId()
        username = await bob_cap.getUsername()
        print(f"   ✓ Capability works - User ID: {user_id}, Username: {username}")

        # 3. Create an order using the User CAPABILITY
        print("\n3. Create order with User capability (capability-based auth)")
        items = [
            {"name": "Laptop", "price": 999.99, "quantity": 1},
            {"name": "Mouse", "price": 29.99, "quantity": 2},
        ]
        # Pass the capability to createOrder - server will call methods on it!
        order = await gateway.createOrder(bob_cap, items)
        bob_order_id = order["orderId"]
        print(f"   ✓ Order created: {bob_order_id}")
        print(f"   Total: ${order['total']:.2f}")
        print(f"   Status: {order['status']}")

        # 4. List orders using the User CAPABILITY
        print("\n4. List orders with User capability")
        orders = await gateway.listOrders(bob_cap)
        print(f"   Found {len(orders)} order(s):")
        for o in orders:
            print(f"     - {o['orderId']}: ${o['total']:.2f} ({o['status']})")

        # 5. Try to cancel order as Bob (should fail - no permission)
        print("\n5. Try to cancel order as Bob (should fail)")
        try:
            await gateway.cancelOrder(bob_cap, bob_order_id)
            print("   ✗ Unexpected success!")
        except RpcError as e:
            print(f"   ✓ Expected failure: {e.message}")

        # 6. Login as Alice (admin) and get her capability
        print("\n6. Login as Alice (admin)")
        alice_login = await gateway.login("alice", "alice123")
        alice_cap = await gateway.getUserByToken(alice_login["token"])
        print(f"   ✓ Logged in as {alice_login['username']} (role: {alice_login['role']})")
        print(f"   ✓ Got Alice's User capability")

        # 7. Alice cancels Bob's order (should succeed - admin)
        print("\n7. Alice cancels Bob's order (admin has permission)")
        result = await gateway.cancelOrder(alice_cap, bob_order_id)
        print(f"   ✓ Order cancelled: {result['orderId']}")
        print(f"   Status: {result['status']}")

        # 8. Verify Bob's order was cancelled
        print("\n8. Verify Bob's order was cancelled")
        orders = await gateway.listOrders(bob_cap)
        for o in orders:
            if o["orderId"] == bob_order_id:
                print(f"   Order {bob_order_id} status: {o['status']}")
                if o["status"] == "cancelled":
                    print("   ✓ Order successfully cancelled by admin")

        print("\n=== Demo Complete ===")
        print()
        print("This demo showed CAPABILITY-BASED SECURITY:")
        print("  • getUserByToken returns a User CAPABILITY (not just data)")
        print("  • Capabilities are passed to createOrder/listOrders/cancelOrder")
        print("  • Server calls methods on the capability for authorization")
        print("  • Permissions are enforced by the capability itself")
        print("  • No token validation needed - the capability IS the authorization")

    finally:
        await session.stop()
        await transport.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print()
        print("Make sure the server is running:")
        print("  uv run python examples/microservices/server.py")
