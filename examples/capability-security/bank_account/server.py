"""Bank Account Server - Demonstrating Capability Attenuation.

This example shows how capabilities can be attenuated (reduced) to
provide limited access. A full account capability can create:

1. READ-ONLY views (can only check balance)
2. LIMITED WITHDRAWAL caps (can only withdraw up to a limit)

Security Properties Demonstrated:
- ATTENUATION: Derived capabilities have fewer permissions than the original
- POLA: Grant only the permissions needed for the task
- REVOCATION: Freeze accounts to revoke access

Run:
    python examples/capability-security/bank_account/server.py
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from capnweb.error import RpcError
from capnweb.ws_session import WebSocketRpcServer
from capnweb.types import RpcTarget

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BankAccount(RpcTarget):
    """Full access bank account capability."""

    _next_id = 1

    def __init__(self, initial_balance: float = 0.0):
        self.account_id = f"ACC{BankAccount._next_id:04d}"
        BankAccount._next_id += 1
        self._balance = initial_balance
        self._frozen = False

    async def call(self, method: str, args: list[Any]) -> Any:
        match method:
            case "getBalance":
                return await self._get_balance()
            case "deposit":
                return await self._deposit(args[0])
            case "withdraw":
                return await self._withdraw(args[0])
            case "freeze":
                return self._freeze()
            case "unfreeze":
                return self._unfreeze()
            case "createReadOnlyView":
                return self._create_read_only_view()
            case "createLimitedWithdrawal":
                return self._create_limited_withdrawal(args[0])
            case _:
                raise RpcError.not_found(f"Method {method} not found")

    async def get_property(self, prop: str) -> Any:
        match prop:
            case "accountId":
                return self.account_id
            case "balance":
                return await self._get_balance()
            case "isFrozen":
                return self._frozen
            case _:
                raise RpcError.not_found(f"Property {prop} not found")

    async def _get_balance(self) -> float:
        if self._frozen:
            raise RpcError.permission_denied("Account is frozen")
        return self._balance

    async def _deposit(self, amount: float) -> dict[str, Any]:
        if self._frozen:
            raise RpcError.permission_denied("Account is frozen")
        if amount <= 0:
            raise RpcError.bad_request("Amount must be positive")

        self._balance += amount
        logger.info("[%s] Deposited $%.2f, new balance: $%.2f",
                    self.account_id, amount, self._balance)
        return {"newBalance": self._balance, "deposited": amount}

    async def _withdraw(self, amount: float) -> dict[str, Any]:
        if self._frozen:
            raise RpcError.permission_denied("Account is frozen")
        if amount <= 0:
            raise RpcError.bad_request("Amount must be positive")
        if amount > self._balance:
            raise RpcError.bad_request(
                f"Insufficient funds. Balance: ${self._balance:.2f}"
            )

        self._balance -= amount
        logger.info("[%s] Withdrew $%.2f, new balance: $%.2f",
                    self.account_id, amount, self._balance)
        return {"newBalance": self._balance, "withdrawn": amount}

    def _freeze(self) -> dict[str, str]:
        self._frozen = True
        logger.info("[%s] Account FROZEN", self.account_id)
        return {"status": "frozen"}

    def _unfreeze(self) -> dict[str, str]:
        self._frozen = False
        logger.info("[%s] Account UNFROZEN", self.account_id)
        return {"status": "active"}

    def _create_read_only_view(self) -> "ReadOnlyAccountView":
        """Create an ATTENUATED capability with read-only access."""
        logger.info("[%s] Created READ-ONLY view", self.account_id)
        return ReadOnlyAccountView(self)

    def _create_limited_withdrawal(self, max_amount: float) -> "LimitedWithdrawal":
        """Create an ATTENUATED capability with withdrawal limit."""
        logger.info("[%s] Created LIMITED WITHDRAWAL cap (max: $%.2f)",
                    self.account_id, max_amount)
        return LimitedWithdrawal(self, max_amount)


class ReadOnlyAccountView(RpcTarget):
    """Attenuated capability - can only read balance, not modify."""

    def __init__(self, account: BankAccount):
        self._account = account

    async def call(self, method: str, args: list[Any]) -> Any:
        match method:
            case "getBalance":
                return await self._account._get_balance()
            case "deposit":
                raise RpcError.permission_denied("Read-only view cannot deposit")
            case "withdraw":
                raise RpcError.permission_denied("Read-only view cannot withdraw")
            case _:
                raise RpcError.not_found(f"Method {method} not found")

    async def get_property(self, prop: str) -> Any:
        match prop:
            case "accountId":
                return self._account.account_id
            case "balance":
                return await self._account._get_balance()
            case "isReadOnly":
                return True
            case _:
                raise RpcError.not_found(f"Property {prop} not found")


class LimitedWithdrawal(RpcTarget):
    """Attenuated capability - can withdraw up to a limit."""

    def __init__(self, account: BankAccount, max_amount: float):
        self._account = account
        self._max_amount = max_amount
        self._withdrawn = 0.0

    async def call(self, method: str, args: list[Any]) -> Any:
        match method:
            case "getBalance":
                return await self._account._get_balance()
            case "withdraw":
                return await self._withdraw(args[0])
            case "getRemainingLimit":
                return self._max_amount - self._withdrawn
            case "deposit":
                raise RpcError.permission_denied("Limited withdrawal cap cannot deposit")
            case _:
                raise RpcError.not_found(f"Method {method} not found")

    async def get_property(self, prop: str) -> Any:
        match prop:
            case "accountId":
                return self._account.account_id
            case "balance":
                return await self._account._get_balance()
            case "maxAmount":
                return self._max_amount
            case "remainingLimit":
                return self._max_amount - self._withdrawn
            case _:
                raise RpcError.not_found(f"Property {prop} not found")

    async def _withdraw(self, amount: float) -> dict[str, Any]:
        remaining = self._max_amount - self._withdrawn
        if amount > remaining:
            raise RpcError.permission_denied(
                f"Exceeds withdrawal limit. Remaining: ${remaining:.2f}"
            )

        result = await self._account._withdraw(amount)
        self._withdrawn += amount
        result["remainingLimit"] = self._max_amount - self._withdrawn
        return result


class BankService(RpcTarget):
    """Main bank service - creates accounts."""

    async def call(self, method: str, args: list[Any]) -> Any:
        match method:
            case "createAccount":
                initial_balance = args[0] if args else 0.0
                account = BankAccount(initial_balance)
                logger.info("Created account %s with balance $%.2f",
                           account.account_id, initial_balance)
                return account
            case _:
                raise RpcError.not_found(f"Method {method} not found")

    async def get_property(self, prop: str) -> Any:
        raise RpcError.not_found(f"Property {prop} not found")


async def main():
    """Run the bank server."""
    bank = BankService()
    server = WebSocketRpcServer(
        local_main=bank,
        host="127.0.0.1",
        port=8080,
        path="/rpc/ws",
    )

    await server.start()
    
    logger.info("=" * 60)
    logger.info("BANK SERVER - Capability Attenuation Demo")
    logger.info("=" * 60)
    logger.info("")
    logger.info("Security Properties Demonstrated:")
    logger.info("  - ATTENUATION: Derived caps have fewer permissions")
    logger.info("  - POLA: Grant only needed permissions")
    logger.info("  - REVOCATION: Freeze accounts to revoke access")
    logger.info("")
    logger.info("Endpoint: ws://127.0.0.1:8080/rpc/ws")
    logger.info("")
    logger.info("Run client: python examples/capability-security/bank_account/client.py")
    logger.info("Press Ctrl+C to stop")
    logger.info("")

    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        logger.info("\nShutting down bank server...")
    finally:
        await server.stop()


if __name__ == "__main__":
    asyncio.run(main())
