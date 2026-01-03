"""User Service - Manages users and authentication.

Run:
    uv run python examples/microservices/user_service.py
"""

import asyncio
from dataclasses import dataclass
from typing import Any

from aiohttp import web

from capnweb.batch import aiohttp_batch_rpc_handler
from capnweb.error import RpcError
from capnweb.types import RpcTarget

# In-memory user database
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
        """Handle RPC calls."""
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
        """Handle property access."""
        match name:
            case "id":
                return self.user_id
            case "username":
                return self.username
            case "email":
                return self.email
            case "role":
                return self.role
            case _:
                raise RpcError.not_found(f"Property '{name}' not found")

    def _has_permission(self, permission: str) -> bool:
        """Check if user has a permission."""
        admin_permissions = {"user.create", "user.delete", "order.cancel", "system.admin"}
        user_permissions = {"order.create", "order.view", "profile.edit"}

        if self.role == "admin":
            return True  # Admin has all permissions
        elif self.role == "user":
            return permission in user_permissions
        return False


class UserService(RpcTarget):
    """User Service - manages users and authentication."""

    def __init__(self) -> None:
        self.sessions: dict[str, User] = {}
        self.token_counter = 0

    async def call(self, method: str, args: list[Any]) -> Any:
        """Handle RPC calls."""
        match method:
            case "authenticate":
                return await self._authenticate(args[0], args[1])
            case "getUser":
                return self._get_user(args[0])
            case "getUserByToken":
                return self._get_user_by_token(args[0])
            case "getUserProfile":
                return self._get_user_profile(args[0])
            case "validateToken":
                return self._validate_token(args[0], args[1] if len(args) > 1 else None)
            case "listUsers":
                return self._list_users()
            case _:
                raise RpcError.not_found(f"Method '{method}' not found")

    async def get_property(self, name: str) -> Any:
        """Handle property access."""
        raise RpcError.not_found(f"Property '{name}' not found")

    async def _authenticate(self, username: str, password: str) -> dict[str, Any]:
        """Authenticate a user and return session info."""
        user_data = USERS_DB.get(username)
        if not user_data or user_data["password"] != password:
            raise RpcError.bad_request("Invalid username or password")

        # Create session token
        self.token_counter += 1
        token = f"token_{username}_{self.token_counter}"

        # Create User capability
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

    def _get_user(self, user_id: str) -> User:
        """Get a User capability by ID."""
        for user_data in USERS_DB.values():
            if user_data["id"] == user_id:
                return User(
                    user_id=user_data["id"],
                    username=user_data["username"],
                    email=user_data["email"],
                    role=user_data["role"],
                )
        raise RpcError.not_found(f"User '{user_id}' not found")

    def _get_user_by_token(self, token: str) -> User:
        """Get a User capability by session token."""
        user = self.sessions.get(token)
        if not user:
            raise RpcError.bad_request("Invalid or expired token")
        return user

    def _list_users(self) -> list[dict[str, str]]:
        """List all users (without sensitive data)."""
        return [
            {"id": u["id"], "username": u["username"], "role": u["role"]}
            for u in USERS_DB.values()
        ]

    def _get_user_profile(self, token: str) -> dict[str, Any]:
        """Get user profile data (not a capability) by token."""
        user = self.sessions.get(token)
        if not user:
            raise RpcError.bad_request("Invalid or expired token")
        return {
            "userId": user.user_id,
            "username": user.username,
            "email": user.email,
            "role": user.role,
        }

    def _validate_token(self, token: str, permission: str | None = None) -> dict[str, Any]:
        """Validate a token and optionally check permission. Returns user info."""
        user = self.sessions.get(token)
        if not user:
            raise RpcError.bad_request("Invalid or expired token")
        
        result = {
            "valid": True,
            "userId": user.user_id,
            "username": user.username,
            "role": user.role,
        }
        
        if permission:
            result["hasPermission"] = user._has_permission(permission)
        
        return result


async def main() -> None:
    """Run the User Service."""
    user_service = UserService()

    async def rpc_handler(request: web.Request) -> web.Response:
        return await aiohttp_batch_rpc_handler(request, user_service)

    app = web.Application()
    app.router.add_post("/rpc/batch", rpc_handler)

    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(runner, "127.0.0.1", 8081)
    await site.start()

    print("🔐 User Service running on http://127.0.0.1:8081")
    print("   Endpoint: http://127.0.0.1:8081/rpc/batch")
    print()
    print("Available users:")
    for username, data in USERS_DB.items():
        print(f"  - {username} ({data['role']})")
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
