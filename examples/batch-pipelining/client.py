"""Client demonstrating batching vs sequential calls.

Usage (separate terminal from server):
    uv run python examples/batch-pipelining/client.py
"""

import asyncio
import time

import aiohttp

from capnweb.batch import new_http_batch_rpc_session


async def run_batched() -> dict:
    """Run with batching (multiple calls in single session)."""
    t0 = time.perf_counter()

    url = "http://localhost:3000/rpc/batch"

    async with aiohttp.ClientSession() as http_session:
        # Each new_http_batch_rpc_session returns an RpcStub directly
        # Authenticate first
        stub = await new_http_batch_rpc_session(url, http_client=http_session)
        user = await stub.authenticate("cookie-123")

        # Then get profile and notifications
        stub2 = await new_http_batch_rpc_session(url, http_client=http_session)
        profile = await stub2.getUserProfile(user["id"])

        stub3 = await new_http_batch_rpc_session(url, http_client=http_session)
        notifications = await stub3.getNotifications(user["id"])

    t1 = time.perf_counter()
    return {
        "user": user,
        "profile": profile,
        "notifications": notifications,
        "ms": (t1 - t0) * 1000,
        "requests": 3,
    }


async def run_sequential() -> dict:
    """Run with separate HTTP requests (no batching)."""
    t0 = time.perf_counter()

    url = "http://localhost:3000/rpc/batch"

    async with aiohttp.ClientSession() as http_session:
        # 1) Authenticate (1st request)
        stub1 = await new_http_batch_rpc_session(url, http_client=http_session)
        user = await stub1.authenticate("cookie-123")

        # 2) Get profile (2nd request)
        stub2 = await new_http_batch_rpc_session(url, http_client=http_session)
        profile = await stub2.getUserProfile(user["id"])

        # 3) Get notifications (3rd request)
        stub3 = await new_http_batch_rpc_session(url, http_client=http_session)
        notifications = await stub3.getNotifications(user["id"])

    t1 = time.perf_counter()
    return {
        "user": user,
        "profile": profile,
        "notifications": notifications,
        "ms": (t1 - t0) * 1000,
        "requests": 3,
    }


async def main() -> None:
    """Run both batched and sequential examples."""
    print("=" * 60)
    print("HTTP Batch RPC - Pipelining Demo")
    print("=" * 60)

    print("\n--- Running sequential calls ---")
    sequential = await run_sequential()
    print(f"HTTP requests: {sequential['requests']}")
    print(f"Time: {sequential['ms']:.2f} ms")
    print(f"Authenticated user: {sequential['user']}")
    print(f"Profile: {sequential['profile']}")
    print(f"Notifications: {sequential['notifications']}")

    print("\n--- Running batched calls ---")
    print("NOTE: True pipelining (dependent calls in one batch)")
    print("      is a future enhancement")
    batched = await run_batched()
    print(f"HTTP requests: {batched['requests']}")
    print(f"Time: {batched['ms']:.2f} ms")
    print(f"Authenticated user: {batched['user']}")
    print(f"Profile: {batched['profile']}")
    print(f"Notifications: {batched['notifications']}")

    print("\n" + "=" * 60)
    print("Summary:")
    print(f"  Sequential: {sequential['requests']} request(s), {sequential['ms']:.2f} ms")
    print(f"  Batched:    {batched['requests']} request(s), {batched['ms']:.2f} ms")
    print("=" * 60)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("Make sure the server is running:")
        print("  uv run python examples/batch-pipelining/server.py")
