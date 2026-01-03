"""Simple calculator client using HTTP batch RPC.

Run (after starting server):
    uv run python examples/calculator/client.py
"""

import asyncio

import aiohttp

from capnweb.batch import new_http_batch_rpc_session


async def main() -> None:
    """Run the calculator client."""
    print("🧮 Calculator Client")
    print("=" * 40)

    url = "http://127.0.0.1:8080/rpc/batch"

    async with aiohttp.ClientSession() as http_session:
        # Test addition
        print("\nTesting add(5, 3)...")
        stub = await new_http_batch_rpc_session(url, http_client=http_session)
        result = await stub.add(5, 3)
        print(f"  5 + 3 = {result}")

        # Test subtraction
        print("\nTesting subtract(10, 4)...")
        stub = await new_http_batch_rpc_session(url, http_client=http_session)
        result = await stub.subtract(10, 4)
        print(f"  10 - 4 = {result}")

        # Test multiplication
        print("\nTesting multiply(7, 6)...")
        stub = await new_http_batch_rpc_session(url, http_client=http_session)
        result = await stub.multiply(7, 6)
        print(f"  7 × 6 = {result}")

        # Test division
        print("\nTesting divide(20, 4)...")
        stub = await new_http_batch_rpc_session(url, http_client=http_session)
        result = await stub.divide(20, 4)
        print(f"  20 ÷ 4 = {result}")

        # Test division by zero (should fail)
        print("\nTesting divide(10, 0) - should fail...")
        try:
            stub = await new_http_batch_rpc_session(url, http_client=http_session)
            result = await stub.divide(10, 0)
            print(f"  Unexpected success: {result}")
        except Exception as e:
            print(f"  Expected error: {e}")

    print("\n" + "=" * 40)
    print("✅ All tests completed!")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("Make sure the server is running: uv run python examples/calculator/server.py")
