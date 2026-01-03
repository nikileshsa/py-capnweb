"""Core type definitions for Cap'n Web protocol."""

from __future__ import annotations

import asyncio
import inspect
from abc import ABC
from typing import Any, Protocol, get_type_hints


class RpcTarget(ABC):
    """Base class for RPC capability implementations.

    Capabilities are objects that can receive method calls and property access
    over the RPC protocol.

    Usage (ergonomic style - recommended):
        class MyApi(RpcTarget):
            def hello(self, name: str) -> str:
                return f"Hello, {name}!"

            async def fetch_data(self, id: int) -> dict:
                return {"id": id, "data": "..."}

    Usage (explicit style - for custom dispatch):
        class MyApi(RpcTarget):
            async def call(self, method: str, args: list[Any]) -> Any:
                match method:
                    case "hello":
                        return f"Hello, {args[0]}!"
                    case _:
                        raise RpcError.not_found(f"Unknown method: {method}")

    Both styles are fully supported. If you override `call()`, it takes precedence.
    Otherwise, public methods (not starting with '_') are automatically exposed.
    """

    # Methods that should never be exposed as RPC endpoints
    _rpc_reserved_methods = frozenset({
        'call', 'get_property', 'dispose',
        # Python special methods
        '__init__', '__new__', '__del__', '__repr__', '__str__',
        '__hash__', '__eq__', '__ne__', '__lt__', '__le__', '__gt__', '__ge__',
        '__getattr__', '__setattr__', '__delattr__', '__getattribute__',
        '__class__', '__dict__', '__doc__', '__module__', '__weakref__',
    })

    async def call(self, method: str, args: list[Any]) -> Any:
        """Call a method on this capability.

        Default implementation dispatches to public methods on the class.
        Override this method for custom dispatch logic.

        Args:
            method: The method name to call
            args: List of arguments for the method

        Returns:
            The result of the method call

        Raises:
            RpcError: If the method call fails
        """
        from capnweb.error import RpcError

        # Check if method exists and is callable
        if method.startswith('_'):
            raise RpcError.not_found(f"Method not found: {method}")

        if method in self._rpc_reserved_methods:
            raise RpcError.not_found(f"Method not found: {method}")

        func = getattr(self, method, None)
        if func is None or not callable(func):
            raise RpcError.not_found(f"Method not found: {method}")

        # Call the method with args
        try:
            result = func(*args)
            # Handle both sync and async methods
            if asyncio.iscoroutine(result):
                result = await result
            return result
        except TypeError as e:
            # Convert argument errors to RPC errors
            raise RpcError.bad_request(str(e)) from e

    async def get_property(self, prop: str) -> Any:
        """Get a property from this capability.

        Default implementation returns public attributes.
        Override this method for custom property access.

        Args:
            prop: The property name to access

        Returns:
            The property value

        Raises:
            RpcError: If the property access fails
        """
        from capnweb.error import RpcError

        if prop.startswith('_'):
            raise RpcError.not_found(f"Property not found: {prop}")

        if not hasattr(self, prop):
            raise RpcError.not_found(f"Property not found: {prop}")

        value = getattr(self, prop)

        # Don't expose methods as properties
        if callable(value):
            raise RpcError.not_found(f"Property not found: {prop}")

        return value


class Transport(Protocol):
    """Protocol for RPC transports."""

    async def send(self, data: bytes) -> None:
        """Send data over the transport.

        Args:
            data: The data to send

        Raises:
            Exception: If sending fails
        """
        ...

    async def receive(self) -> bytes:
        """Receive data from the transport.

        Returns:
            The received data

        Raises:
            Exception: If receiving fails
        """
        ...

    async def close(self) -> None:
        """Close the transport connection.

        Raises:
            Exception: If closing fails
        """
        ...
