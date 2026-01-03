"""Bank Account Client - Demonstrating Capability Attenuation.

This client demonstrates how attenuated capabilities work:

1. Creates an account (gets FULL capability)
2. Creates a READ-ONLY view (attenuated - can only read)
3. Creates a LIMITED WITHDRAWAL cap (attenuated - spending limit)
4. Shows that attenuated caps cannot exceed their permissions

Run:
    # First start the server:
    python examples/capability-security/bank_account/server.py

    # Then run this client:
    python examples/capability-security/bank_account/client.py
"""

from __future__ import annotations

import asyncio
import logging

from capnweb import UnifiedClient, UnifiedClientConfig, RpcError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main():
    """Demonstrate capability attenuation with bank accounts."""
    config = UnifiedClientConfig(url="ws://127.0.0.1:8080/rpc/ws")

    async with UnifiedClient(config) as client:
        print("=" * 60)
        print("CAPABILITY ATTENUATION DEMO")
        print("=" * 60)
        print()

        # =====================================================================
        # Step 1: Create an account (get FULL capability)
        # =====================================================================
        print("1. Creating account with $1000 initial balance...")
        # Use the main stub to call createAccount - returns an account stub
        main = client.get_main_stub()
        account = await main.createAccount(1000.0)
        
        balance = await account.getBalance()
        print(f"   Account created")
        print(f"   Balance: ${balance:.2f}")
        print()

        # =====================================================================
        # Step 2: Full capability - can do everything
        # =====================================================================
        print("2. Using FULL capability:")
        print("   - Depositing $500...")
        result = await account.deposit(500.0)
        print(f"     New balance: ${result['newBalance']:.2f}")

        print("   - Withdrawing $200...")
        result = await account.withdraw(200.0)
        print(f"     New balance: ${result['newBalance']:.2f}")
        print()

        # =====================================================================
        # Step 3: Create READ-ONLY view (attenuated capability)
        # =====================================================================
        print("3. Creating READ-ONLY view (attenuated capability):")
        read_only = await account.createReadOnlyView()
        
        ro_balance = await read_only.getBalance()
        print(f"   Read-only capability created")
        print(f"   Can read balance: ${ro_balance:.2f}")

        print("   - Attempting to deposit (should FAIL)...")
        try:
            await read_only.deposit(100.0)
            print("     ERROR: Should have been denied!")
        except RpcError as e:
            print(f"     BLOCKED: {e.message}")

        print("   - Attempting to withdraw (should FAIL)...")
        try:
            await read_only.withdraw(100.0)
            print("     ERROR: Should have been denied!")
        except RpcError as e:
            print(f"     BLOCKED: {e.message}")
        print()

        # =====================================================================
        # Step 4: Create LIMITED WITHDRAWAL cap (attenuated with state)
        # =====================================================================
        print("4. Creating LIMITED WITHDRAWAL capability (max $300):")
        limited = await account.createLimitedWithdrawal(300.0)
        
        remaining = await limited.getRemainingLimit()
        print(f"   Limited withdrawal capability created")
        print(f"   Remaining limit: ${remaining:.2f}")

        print("   - Withdrawing $100...")
        result = await limited.withdraw(100.0)
        print(f"     Success! Remaining limit: ${result['remainingLimit']:.2f}")

        print("   - Withdrawing another $150...")
        result = await limited.withdraw(150.0)
        print(f"     Success! Remaining limit: ${result['remainingLimit']:.2f}")

        print("   - Attempting to withdraw $100 (exceeds limit, should FAIL)...")
        try:
            await limited.withdraw(100.0)
            print("     ERROR: Should have been denied!")
        except RpcError as e:
            print(f"     BLOCKED: {e.message}")
        print()

        # =====================================================================
        # Step 5: Demonstrate revocation via freeze
        # =====================================================================
        print("5. Demonstrating REVOCATION (freeze account):")
        balance = await account.getBalance()
        print(f"   Current balance: ${balance:.2f}")
        
        print("   - Freezing account...")
        await account.freeze()
        print("   Account frozen!")

        print("   - Attempting to check balance (should FAIL)...")
        try:
            await account.getBalance()
            print("     ERROR: Should have been denied!")
        except RpcError as e:
            print(f"     BLOCKED: {e.message}")

        print("   - Unfreezing account...")
        await account.unfreeze()
        balance = await account.getBalance()
        print(f"   Balance accessible again: ${balance:.2f}")
        print()

        # Graceful shutdown
        await client.drain()

        # =====================================================================
        # Summary
        # =====================================================================
        print("=" * 60)
        print("SECURITY PROPERTIES DEMONSTRATED:")
        print("=" * 60)
        print()
        print("ATTENUATION:")
        print("  - Read-only view cannot deposit or withdraw")
        print("  - Limited withdrawal cap enforces spending limit")
        print("  - Attenuated caps have STRICTLY FEWER permissions")
        print()
        print("POLA (Principle of Least Authority):")
        print("  - Give read-only view to code that only needs to check balance")
        print("  - Give limited withdrawal to untrusted code")
        print()
        print("REVOCATION:")
        print("  - Freezing account immediately blocks all operations")
        print("  - Even existing capabilities stop working")
        print()


if __name__ == "__main__":
    asyncio.run(main())
