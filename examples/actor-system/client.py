"""Client demonstrating the distributed actor system.

This client connects to the supervisor via WebSocket, spawns workers,
and communicates with them directly, demonstrating location transparency.

Run (after starting supervisor.py):
    uv run python examples/actor-system/client.py
"""

import asyncio
from typing import Any

from capnweb.error import RpcError
from capnweb.rpc_session import BidirectionalSession
from capnweb.stubs import RpcStub
from capnweb.types import RpcTarget
from capnweb.ws_transport import WebSocketClientTransport

SUPERVISOR_URL = "ws://127.0.0.1:8080/rpc/ws"


class DummyTarget(RpcTarget):
    """Dummy target for client side (not exposing any methods)."""

    async def call(self, method: str, args: list[Any]) -> Any:
        raise RpcError.not_found(f"Method '{method}' not found")

    async def get_property(self, name: str) -> Any:
        raise RpcError.not_found(f"Property '{name}' not found")


async def main() -> None:
    """Main application logic that uses the distributed actor system."""
    print("--- Distributed Actor System Demo ---")
    print()

    # Connect to supervisor via WebSocket
    transport = WebSocketClientTransport(SUPERVISOR_URL)
    await transport.connect()
    
    session = BidirectionalSession(transport, DummyTarget())
    session.start()

    # Get the supervisor stub (wrap ImportHook in RpcStub for ergonomic API)
    supervisor = RpcStub(session.get_main_stub())

    try:
        # === Step 1: Spawn two workers via the supervisor ===
        print("1. Spawning two workers...")

        worker_a = await supervisor.spawn_worker("Worker-A")
        worker_b = await supervisor.spawn_worker("Worker-B")

        print("   ✓ Spawned Worker-A and Worker-B")

        # === Step 2: Interact with workers directly ===
        # The client now has direct references to the workers via capabilities
        print("\n2. Interacting directly with workers...")

        # Send increment messages to workers
        print("\n3. Sending 'increment' messages to workers...")

        # Increment Worker-A twice (worker_a is an RpcStub)
        await worker_a.increment()
        await worker_a.increment()

        # Increment Worker-B once
        await worker_b.increment()

        print("   ✓ Sent two increments to Worker-A, one to Worker-B")

        # === Step 3: Verify the final state of the workers ===
        print("\n4. Verifying final worker states...")

        count_a = await worker_a.get_count()
        count_b = await worker_b.get_count()

        print(f"   - Final count for Worker-A: {count_a}")
        print(f"   - Final count for Worker-B: {count_b}")

        # Verify expected values
        if count_a == 2 and count_b == 1:
            print("\n✅ Demo finished successfully!")
        else:
            print(f"\n❌ Unexpected counts: Worker-A={count_a}, Worker-B={count_b}")

        # === Step 4: List all workers ===
        print("\n5. Listing all workers via supervisor...")
        workers = await supervisor.list_workers()
        print(f"   Active workers: {workers}")

    finally:
        await session.stop()
        await transport.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"\n❌ An error occurred: {e}")
        print("   Please ensure the supervisor is running:")
        print("   uv run python examples/actor-system/supervisor.py")
