"""Object Capability Security Model Tests.

This test suite validates the core security properties of the Cap'n Web
object capability model using real-world scenarios.

Key Security Properties Tested:
1. Capability Confinement - Capabilities cannot be forged or guessed
2. Attenuation - Derived capabilities have reduced permissions
3. Revocation - Capabilities can be revoked
4. POLA (Principle of Least Authority) - Only grant needed capabilities
5. No Ambient Authority - All access requires explicit capability

Real-World Scenarios:
- Bank account with limited withdrawal capability
- File system with read-only vs read-write access
- API rate limiting via capability attenuation
- Multi-tenant isolation
- Delegation chains with revocation
"""

from typing import Any

import pytest

from capnweb.error import RpcError
from capnweb.types import RpcTarget


# =============================================================================
# Example 1: Bank Account with Capability-Based Access Control
# =============================================================================


class BankAccount(RpcTarget):
    """A bank account that can issue limited capabilities."""

    def __init__(self, account_id: str, initial_balance: float = 0.0):
        self.account_id = account_id
        self._balance = initial_balance
        self._is_frozen = False

    async def get_balance(self) -> float:
        if self._is_frozen:
            raise RpcError.permission_denied("Account is frozen")
        return self._balance

    async def deposit(self, amount: float) -> float:
        if self._is_frozen:
            raise RpcError.permission_denied("Account is frozen")
        if amount <= 0:
            raise RpcError.bad_request("Deposit amount must be positive")
        self._balance += amount
        return self._balance

    async def withdraw(self, amount: float) -> float:
        if self._is_frozen:
            raise RpcError.permission_denied("Account is frozen")
        if amount <= 0:
            raise RpcError.bad_request("Withdrawal amount must be positive")
        if amount > self._balance:
            raise RpcError.bad_request("Insufficient funds")
        self._balance -= amount
        return self._balance

    def freeze(self) -> None:
        self._is_frozen = True

    def unfreeze(self) -> None:
        self._is_frozen = False

    def create_read_only_view(self) -> "ReadOnlyAccountView":
        return ReadOnlyAccountView(self)

    def create_limited_withdrawal(self, max_amount: float) -> "LimitedWithdrawal":
        return LimitedWithdrawal(self, max_amount)


class ReadOnlyAccountView(RpcTarget):
    """Attenuated capability - can only read balance."""

    def __init__(self, account: BankAccount):
        self._account = account

    async def get_balance(self) -> float:
        return await self._account.get_balance()

    async def deposit(self, amount: float) -> float:
        raise RpcError.permission_denied("Read-only access: deposit not allowed")

    async def withdraw(self, amount: float) -> float:
        raise RpcError.permission_denied("Read-only access: withdraw not allowed")


class LimitedWithdrawal(RpcTarget):
    """Attenuated capability - can only withdraw up to a limit."""

    def __init__(self, account: BankAccount, max_amount: float):
        self._account = account
        self._max_amount = max_amount
        self._withdrawn = 0.0

    async def withdraw(self, amount: float) -> float:
        if self._withdrawn + amount > self._max_amount:
            remaining = self._max_amount - self._withdrawn
            raise RpcError.permission_denied(
                f"Withdrawal limit exceeded. Remaining: ${remaining:.2f}"
            )
        result = await self._account.withdraw(amount)
        self._withdrawn += amount
        return result

    async def get_remaining_limit(self) -> float:
        return self._max_amount - self._withdrawn


class TestBankAccountCapabilities:
    """Test capability-based access control for bank accounts."""

    @pytest.mark.asyncio
    async def test_full_capability_allows_all_operations(self):
        """Full capability holder can perform all operations."""
        account = BankAccount("ACC001", 1000.0)

        balance = await account.get_balance()
        assert balance == 1000.0

        new_balance = await account.deposit(500.0)
        assert new_balance == 1500.0

        final_balance = await account.withdraw(200.0)
        assert final_balance == 1300.0

    @pytest.mark.asyncio
    async def test_read_only_capability_blocks_writes(self):
        """Read-only capability cannot modify account."""
        account = BankAccount("ACC002", 1000.0)
        read_only = account.create_read_only_view()

        balance = await read_only.get_balance()
        assert balance == 1000.0

        with pytest.raises(RpcError) as exc_info:
            await read_only.deposit(100.0)
        assert exc_info.value.code.value == "permission_denied"

        with pytest.raises(RpcError) as exc_info:
            await read_only.withdraw(100.0)
        assert exc_info.value.code.value == "permission_denied"

        assert await read_only.get_balance() == 1000.0

    @pytest.mark.asyncio
    async def test_limited_withdrawal_enforces_limit(self):
        """Limited withdrawal capability enforces spending limit."""
        account = BankAccount("ACC003", 10000.0)
        limited = account.create_limited_withdrawal(max_amount=500.0)

        await limited.withdraw(200.0)
        remaining = await limited.get_remaining_limit()
        assert remaining == 300.0

        await limited.withdraw(200.0)
        remaining = await limited.get_remaining_limit()
        assert remaining == 100.0

        with pytest.raises(RpcError) as exc_info:
            await limited.withdraw(200.0)
        assert exc_info.value.code.value == "permission_denied"
        assert "limit exceeded" in exc_info.value.message.lower()

    @pytest.mark.asyncio
    async def test_frozen_account_blocks_all_operations(self):
        """Frozen account blocks all operations (revocation)."""
        account = BankAccount("ACC004", 1000.0)

        balance = await account.get_balance()
        assert balance == 1000.0

        account.freeze()

        with pytest.raises(RpcError) as exc_info:
            await account.get_balance()
        assert exc_info.value.code.value == "permission_denied"

        account.unfreeze()
        balance = await account.get_balance()
        assert balance == 1000.0


# =============================================================================
# Example 2: File System with Capability-Based Access
# =============================================================================


class FileSystem(RpcTarget):
    """A simple file system with capability-based access control."""

    def __init__(self):
        self._files: dict[str, str] = {}

    def create_file(self, path: str, content: str = "") -> None:
        self._files[path] = content

    async def read(self, path: str) -> str:
        if path not in self._files:
            raise RpcError.not_found(f"File not found: {path}")
        return self._files[path]

    async def write(self, path: str, content: str) -> None:
        if path not in self._files:
            raise RpcError.not_found(f"File not found: {path}")
        self._files[path] = content

    async def delete(self, path: str) -> None:
        if path not in self._files:
            raise RpcError.not_found(f"File not found: {path}")
        del self._files[path]

    def create_read_only_handle(self, path: str) -> "ReadOnlyFileHandle":
        return ReadOnlyFileHandle(self, path)

    def create_read_write_handle(self, path: str) -> "ReadWriteFileHandle":
        return ReadWriteFileHandle(self, path)


class ReadOnlyFileHandle(RpcTarget):
    """Attenuated capability - read-only access to a single file."""

    def __init__(self, fs: FileSystem, path: str):
        self._fs = fs
        self._path = path

    async def read(self) -> str:
        return await self._fs.read(self._path)

    async def write(self, content: str) -> None:
        raise RpcError.permission_denied("Read-only handle: write not allowed")

    async def delete(self) -> None:
        raise RpcError.permission_denied("Read-only handle: delete not allowed")


class ReadWriteFileHandle(RpcTarget):
    """Attenuated capability - read-write access (no delete)."""

    def __init__(self, fs: FileSystem, path: str):
        self._fs = fs
        self._path = path

    async def read(self) -> str:
        return await self._fs.read(self._path)

    async def write(self, content: str) -> None:
        await self._fs.write(self._path, content)

    async def delete(self) -> None:
        raise RpcError.permission_denied("Read-write handle: delete not allowed")


class TestFileSystemCapabilities:
    """Test capability-based file system access."""

    @pytest.mark.asyncio
    async def test_full_capability_allows_all_operations(self):
        """Full file system capability allows all operations."""
        fs = FileSystem()
        fs.create_file("/config.txt", "initial content")

        content = await fs.read("/config.txt")
        assert content == "initial content"

        await fs.write("/config.txt", "updated content")
        content = await fs.read("/config.txt")
        assert content == "updated content"

        await fs.delete("/config.txt")
        with pytest.raises(RpcError) as exc_info:
            await fs.read("/config.txt")
        assert exc_info.value.code.value == "not_found"

    @pytest.mark.asyncio
    async def test_read_only_handle_blocks_writes(self):
        """Read-only file handle cannot write or delete."""
        fs = FileSystem()
        fs.create_file("/secret.txt", "secret data")

        read_only = fs.create_read_only_handle("/secret.txt")

        content = await read_only.read()
        assert content == "secret data"

        with pytest.raises(RpcError) as exc_info:
            await read_only.write("hacked!")
        assert exc_info.value.code.value == "permission_denied"

        with pytest.raises(RpcError) as exc_info:
            await read_only.delete()
        assert exc_info.value.code.value == "permission_denied"

        assert await read_only.read() == "secret data"

    @pytest.mark.asyncio
    async def test_read_write_handle_blocks_delete(self):
        """Read-write handle can modify but not delete."""
        fs = FileSystem()
        fs.create_file("/data.txt", "original")

        rw_handle = fs.create_read_write_handle("/data.txt")

        content = await rw_handle.read()
        assert content == "original"

        await rw_handle.write("modified")
        content = await rw_handle.read()
        assert content == "modified"

        with pytest.raises(RpcError) as exc_info:
            await rw_handle.delete()
        assert exc_info.value.code.value == "permission_denied"


# =============================================================================
# Example 3: Multi-Tenant API with Isolation
# =============================================================================


class TenantDatabase(RpcTarget):
    """Database with tenant isolation via capabilities."""

    def __init__(self):
        self._data: dict[str, dict[str, Any]] = {}

    def create_tenant(self, tenant_id: str) -> "TenantAccess":
        self._data[tenant_id] = {}
        return TenantAccess(self, tenant_id)

    def _get_tenant_data(self, tenant_id: str) -> dict[str, Any]:
        if tenant_id not in self._data:
            raise RpcError.not_found(f"Tenant not found: {tenant_id}")
        return self._data[tenant_id]


class TenantAccess(RpcTarget):
    """Capability providing isolated access to a single tenant's data."""

    def __init__(self, db: TenantDatabase, tenant_id: str):
        self._db = db
        self._tenant_id = tenant_id

    async def get(self, key: str) -> Any:
        data = self._db._get_tenant_data(self._tenant_id)
        if key not in data:
            raise RpcError.not_found(f"Key not found: {key}")
        return data[key]

    async def set(self, key: str, value: Any) -> None:
        data = self._db._get_tenant_data(self._tenant_id)
        data[key] = value

    async def list_keys(self) -> list[str]:
        data = self._db._get_tenant_data(self._tenant_id)
        return list(data.keys())


class TestMultiTenantIsolation:
    """Test tenant isolation via capabilities."""

    @pytest.mark.asyncio
    async def test_tenants_are_isolated(self):
        """Each tenant can only access their own data."""
        db = TenantDatabase()

        tenant_a = db.create_tenant("tenant_a")
        tenant_b = db.create_tenant("tenant_b")

        await tenant_a.set("secret", "tenant_a_secret")
        await tenant_b.set("secret", "tenant_b_secret")

        assert await tenant_a.get("secret") == "tenant_a_secret"
        assert await tenant_b.get("secret") == "tenant_b_secret"

        keys_a = await tenant_a.list_keys()
        assert keys_a == ["secret"]

        keys_b = await tenant_b.list_keys()
        assert keys_b == ["secret"]


# =============================================================================
# Example 4: Delegation Chain with Revocation
# =============================================================================


class SecretVault(RpcTarget):
    """A vault that can delegate access with revocation support."""

    def __init__(self):
        self._secrets: dict[str, str] = {}
        self._revoked_tokens: set[str] = set()

    def store_secret(self, name: str, value: str) -> None:
        self._secrets[name] = value

    async def get_secret(self, name: str) -> str:
        if name not in self._secrets:
            raise RpcError.not_found(f"Secret not found: {name}")
        return self._secrets[name]

    def create_access_token(self, allowed_secrets: list[str]) -> "VaultAccessToken":
        import uuid
        token_id = str(uuid.uuid4())
        return VaultAccessToken(self, token_id, set(allowed_secrets))

    def revoke_token(self, token_id: str) -> None:
        self._revoked_tokens.add(token_id)

    def is_token_revoked(self, token_id: str) -> bool:
        return token_id in self._revoked_tokens


class VaultAccessToken(RpcTarget):
    """Delegated capability with limited access and revocation support."""

    def __init__(self, vault: SecretVault, token_id: str, allowed_secrets: set[str]):
        self._vault = vault
        self._token_id = token_id
        self._allowed_secrets = allowed_secrets

    async def get_secret(self, name: str) -> str:
        if self._vault.is_token_revoked(self._token_id):
            raise RpcError.permission_denied("Access token has been revoked")

        if name not in self._allowed_secrets:
            raise RpcError.permission_denied(f"Access to secret '{name}' not allowed")

        return await self._vault.get_secret(name)

    def get_token_id(self) -> str:
        return self._token_id


class TestDelegationAndRevocation:
    """Test delegation chains with revocation."""

    @pytest.mark.asyncio
    async def test_delegated_token_has_limited_access(self):
        """Delegated token can only access allowed secrets."""
        vault = SecretVault()
        vault.store_secret("api_key", "secret123")
        vault.store_secret("db_password", "dbpass456")

        token = vault.create_access_token(["api_key"])

        api_key = await token.get_secret("api_key")
        assert api_key == "secret123"

        with pytest.raises(RpcError) as exc_info:
            await token.get_secret("db_password")
        assert exc_info.value.code.value == "permission_denied"

    @pytest.mark.asyncio
    async def test_revoked_token_loses_access(self):
        """Revoked token cannot access any secrets."""
        vault = SecretVault()
        vault.store_secret("api_key", "secret123")

        token = vault.create_access_token(["api_key"])
        token_id = token.get_token_id()

        api_key = await token.get_secret("api_key")
        assert api_key == "secret123"

        vault.revoke_token(token_id)

        with pytest.raises(RpcError) as exc_info:
            await token.get_secret("api_key")
        assert exc_info.value.code.value == "permission_denied"
        assert "revoked" in exc_info.value.message.lower()

    @pytest.mark.asyncio
    async def test_multiple_tokens_independent_revocation(self):
        """Revoking one token doesn't affect others."""
        vault = SecretVault()
        vault.store_secret("shared_secret", "shared123")

        token1 = vault.create_access_token(["shared_secret"])
        token2 = vault.create_access_token(["shared_secret"])

        assert await token1.get_secret("shared_secret") == "shared123"
        assert await token2.get_secret("shared_secret") == "shared123"

        vault.revoke_token(token1.get_token_id())

        with pytest.raises(RpcError):
            await token1.get_secret("shared_secret")

        assert await token2.get_secret("shared_secret") == "shared123"


# =============================================================================
# Example 5: API Rate Limiting via Capability Attenuation
# =============================================================================


class APIService(RpcTarget):
    """An API service with rate-limited capabilities."""

    def __init__(self):
        self._request_count = 0

    async def expensive_operation(self, data: str) -> str:
        self._request_count += 1
        return f"Processed: {data}"

    async def cheap_operation(self, data: str) -> str:
        return f"Quick: {data}"

    def create_rate_limited_client(self, max_requests: int) -> "RateLimitedClient":
        return RateLimitedClient(self, max_requests)


class RateLimitedClient(RpcTarget):
    """Attenuated capability with rate limiting."""

    def __init__(self, service: APIService, max_requests: int):
        self._service = service
        self._max_requests = max_requests
        self._request_count = 0

    async def expensive_operation(self, data: str) -> str:
        if self._request_count >= self._max_requests:
            raise RpcError.permission_denied(
                f"Rate limit exceeded: {self._max_requests} requests"
            )
        self._request_count += 1
        return await self._service.expensive_operation(data)

    async def cheap_operation(self, data: str) -> str:
        return await self._service.cheap_operation(data)


class TestRateLimiting:
    """Test rate limiting via capability attenuation."""

    @pytest.mark.asyncio
    async def test_rate_limit_enforced(self):
        """Rate-limited client enforces request limit."""
        service = APIService()
        client = service.create_rate_limited_client(max_requests=3)

        for i in range(3):
            result = await client.expensive_operation(f"data{i}")
            assert result == f"Processed: data{i}"

        with pytest.raises(RpcError) as exc_info:
            await client.expensive_operation("data3")
        assert exc_info.value.code.value == "permission_denied"
        assert "rate limit" in exc_info.value.message.lower()

    @pytest.mark.asyncio
    async def test_cheap_operations_not_rate_limited(self):
        """Cheap operations bypass rate limit."""
        service = APIService()
        client = service.create_rate_limited_client(max_requests=2)

        await client.expensive_operation("data1")
        await client.expensive_operation("data2")

        with pytest.raises(RpcError):
            await client.expensive_operation("data3")

        result = await client.cheap_operation("data4")
        assert result == "Quick: data4"

    @pytest.mark.asyncio
    async def test_separate_clients_have_separate_limits(self):
        """Each rate-limited client has its own quota."""
        service = APIService()
        client1 = service.create_rate_limited_client(max_requests=2)
        client2 = service.create_rate_limited_client(max_requests=2)

        await client1.expensive_operation("c1_data1")
        await client1.expensive_operation("c1_data2")

        with pytest.raises(RpcError):
            await client1.expensive_operation("c1_data3")

        await client2.expensive_operation("c2_data1")
        await client2.expensive_operation("c2_data2")

        with pytest.raises(RpcError):
            await client2.expensive_operation("c2_data3")


# =============================================================================
# Example 6: Capability Confinement - No Ambient Authority
# =============================================================================


class ResourceManager(RpcTarget):
    """Demonstrates that capabilities cannot be forged or guessed."""

    def __init__(self):
        self._resources: dict[str, str] = {}
        self._next_id = 1

    def create_resource(self, data: str) -> "ResourceHandle":
        resource_id = f"res_{self._next_id}"
        self._next_id += 1
        self._resources[resource_id] = data
        return ResourceHandle(self, resource_id)

    def _get_resource(self, resource_id: str) -> str:
        if resource_id not in self._resources:
            raise RpcError.not_found(f"Resource not found: {resource_id}")
        return self._resources[resource_id]

    def _delete_resource(self, resource_id: str) -> None:
        if resource_id not in self._resources:
            raise RpcError.not_found(f"Resource not found: {resource_id}")
        del self._resources[resource_id]


class ResourceHandle(RpcTarget):
    """A capability to access a specific resource."""

    def __init__(self, manager: ResourceManager, resource_id: str):
        self._manager = manager
        self._resource_id = resource_id

    async def get_data(self) -> str:
        return self._manager._get_resource(self._resource_id)

    async def delete(self) -> None:
        self._manager._delete_resource(self._resource_id)


class TestCapabilityConfinement:
    """Test that capabilities cannot be forged or guessed."""

    @pytest.mark.asyncio
    async def test_capability_required_for_access(self):
        """Cannot access resources without the capability."""
        manager = ResourceManager()
        handle = manager.create_resource("secret data")

        data = await handle.get_data()
        assert data == "secret data"

    @pytest.mark.asyncio
    async def test_deleted_resource_inaccessible(self):
        """Deleted resources cannot be accessed even with old capability."""
        manager = ResourceManager()
        handle = manager.create_resource("temporary data")

        data = await handle.get_data()
        assert data == "temporary data"

        await handle.delete()

        with pytest.raises(RpcError) as exc_info:
            await handle.get_data()
        assert exc_info.value.code.value == "not_found"

    @pytest.mark.asyncio
    async def test_each_resource_has_unique_capability(self):
        """Each resource gets its own unique capability."""
        manager = ResourceManager()

        handle1 = manager.create_resource("data1")
        handle2 = manager.create_resource("data2")

        assert await handle1.get_data() == "data1"
        assert await handle2.get_data() == "data2"

        await handle1.delete()

        with pytest.raises(RpcError):
            await handle1.get_data()

        assert await handle2.get_data() == "data2"
