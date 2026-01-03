"""Tests for ergonomic RpcTarget API.

Tests the new TypeScript-style ergonomic API where methods are automatically
exposed as RPC endpoints without needing to implement call() manually.
"""

import asyncio
import pytest
from typing import Any

from capnweb.types import RpcTarget
from capnweb.error import RpcError
from capnweb.rpc_session import BidirectionalSession
from capnweb.payload import RpcPayload


# =============================================================================
# Test Infrastructure
# =============================================================================

class InMemoryTransport:
    def __init__(self, send_queue: asyncio.Queue, recv_queue: asyncio.Queue):
        self.send_queue = send_queue
        self.recv_queue = recv_queue
        self._closed = False
    
    async def send(self, message: str) -> None:
        if self._closed:
            raise ConnectionError("Transport closed")
        await self.send_queue.put(message)
    
    async def receive(self) -> str:
        if self._closed and self.recv_queue.empty():
            raise ConnectionError("Transport closed")
        return await self.recv_queue.get()
    
    def abort(self, reason: Exception) -> None:
        self._closed = True
    
    def close(self) -> None:
        self._closed = True


def create_transport_pair() -> tuple[InMemoryTransport, InMemoryTransport]:
    queue_a_to_b = asyncio.Queue()
    queue_b_to_a = asyncio.Queue()
    return (
        InMemoryTransport(queue_a_to_b, queue_b_to_a),
        InMemoryTransport(queue_b_to_a, queue_a_to_b),
    )


# =============================================================================
# Ergonomic RpcTarget Examples
# =============================================================================

class ErgonomicApi(RpcTarget):
    """Example API using the new ergonomic style."""
    
    def __init__(self):
        self.call_log: list[str] = []
    
    def hello(self, name: str) -> str:
        """Sync method - automatically exposed."""
        self.call_log.append(f"hello({name})")
        return f"Hello, {name}!"
    
    def add(self, a: int, b: int) -> int:
        """Sync method with multiple args."""
        self.call_log.append(f"add({a}, {b})")
        return a + b
    
    async def fetch_data(self, id: int) -> dict:
        """Async method - automatically exposed."""
        self.call_log.append(f"fetch_data({id})")
        await asyncio.sleep(0.01)  # Simulate async work
        return {"id": id, "data": f"data-{id}"}
    
    async def slow_operation(self, delay: float) -> str:
        """Async method with delay."""
        self.call_log.append(f"slow_operation({delay})")
        await asyncio.sleep(delay)
        return "done"
    
    def no_args(self) -> str:
        """Method with no arguments."""
        self.call_log.append("no_args()")
        return "no args called"
    
    def _private_method(self) -> str:
        """Private method - should NOT be exposed."""
        return "private"
    
    def __dunder_method__(self) -> str:
        """Dunder method - should NOT be exposed."""
        return "dunder"


class ExplicitApi(RpcTarget):
    """Example API using the explicit call() style (backward compatible)."""
    
    def __init__(self):
        self.call_log: list[str] = []
    
    async def call(self, method: str, args: list[Any]) -> Any:
        """Explicit dispatch - takes precedence over automatic dispatch."""
        self.call_log.append(f"call({method}, {args})")
        match method:
            case "custom_hello":
                return f"Custom Hello, {args[0]}!"
            case "custom_add":
                return args[0] + args[1]
            case _:
                raise RpcError.not_found(f"Unknown method: {method}")
    
    async def get_property(self, prop: str) -> Any:
        if prop == "version":
            return "1.0.0"
        raise RpcError.not_found(f"Unknown property: {prop}")


class MixedApi(RpcTarget):
    """API that has both public methods and overrides call() for some."""
    
    def __init__(self):
        self.intercepted: list[str] = []
    
    async def call(self, method: str, args: list[Any]) -> Any:
        # Intercept specific methods, delegate others to default
        if method == "intercepted":
            self.intercepted.append(args[0] if args else "")
            return "intercepted!"
        # Fall back to default dispatch for other methods
        return await super().call(method, args)
    
    def regular_method(self, x: int) -> int:
        """Regular method - dispatched by super().call()"""
        return x * 2


class PropertyApi(RpcTarget):
    """API with properties."""
    
    def __init__(self):
        self.name = "PropertyApi"
        self.version = "2.0"
        self.count = 42
        self._private_data = "secret"
    
    def get_info(self) -> dict:
        return {"name": self.name, "version": self.version}


class ClientCallback(RpcTarget):
    """Simple client callback for bidirectional tests."""
    
    def __init__(self):
        self.messages: list[str] = []
    
    def on_message(self, msg: str) -> str:
        self.messages.append(msg)
        return f"ack-{len(self.messages)}"


# =============================================================================
# Unit Tests for RpcTarget
# =============================================================================

@pytest.mark.asyncio
class TestErgonomicRpcTarget:
    """Test the ergonomic RpcTarget API."""
    
    async def test_sync_method_call(self) -> None:
        """Sync methods should be callable via call()."""
        api = ErgonomicApi()
        result = await api.call("hello", ["World"])
        assert result == "Hello, World!"
        assert api.call_log == ["hello(World)"]
    
    async def test_sync_method_multiple_args(self) -> None:
        """Sync methods with multiple args should work."""
        api = ErgonomicApi()
        result = await api.call("add", [5, 3])
        assert result == 8
        assert api.call_log == ["add(5, 3)"]
    
    async def test_async_method_call(self) -> None:
        """Async methods should be callable via call()."""
        api = ErgonomicApi()
        result = await api.call("fetch_data", [123])
        assert result == {"id": 123, "data": "data-123"}
        assert api.call_log == ["fetch_data(123)"]
    
    async def test_no_args_method(self) -> None:
        """Methods with no args should work."""
        api = ErgonomicApi()
        result = await api.call("no_args", [])
        assert result == "no args called"
    
    async def test_private_method_not_exposed(self) -> None:
        """Private methods (starting with _) should not be exposed."""
        api = ErgonomicApi()
        with pytest.raises(RpcError) as exc_info:
            await api.call("_private_method", [])
        assert "not found" in str(exc_info.value).lower()
    
    async def test_dunder_method_not_exposed(self) -> None:
        """Dunder methods should not be exposed."""
        api = ErgonomicApi()
        with pytest.raises(RpcError) as exc_info:
            await api.call("__dunder_method__", [])
        assert "not found" in str(exc_info.value).lower()
    
    async def test_reserved_method_not_exposed(self) -> None:
        """Reserved methods like 'call' should not be exposed."""
        api = ErgonomicApi()
        with pytest.raises(RpcError) as exc_info:
            await api.call("call", ["hello", []])
        assert "not found" in str(exc_info.value).lower()
    
    async def test_nonexistent_method(self) -> None:
        """Calling nonexistent method should raise RpcError."""
        api = ErgonomicApi()
        with pytest.raises(RpcError) as exc_info:
            await api.call("nonexistent", [])
        assert "not found" in str(exc_info.value).lower()
    
    async def test_wrong_number_of_args(self) -> None:
        """Wrong number of args should raise RpcError."""
        api = ErgonomicApi()
        with pytest.raises(RpcError) as exc_info:
            await api.call("hello", [])  # Missing required arg
        # Should be a bad_request error with info about missing argument
        assert exc_info.value.code.value == "bad_request"


@pytest.mark.asyncio
class TestExplicitRpcTarget:
    """Test backward compatibility with explicit call() style."""
    
    async def test_explicit_call_works(self) -> None:
        """Explicit call() implementation should work."""
        api = ExplicitApi()
        result = await api.call("custom_hello", ["Alice"])
        assert result == "Custom Hello, Alice!"
    
    async def test_explicit_call_multiple_args(self) -> None:
        """Explicit call() with multiple args should work."""
        api = ExplicitApi()
        result = await api.call("custom_add", [10, 20])
        assert result == 30
    
    async def test_explicit_unknown_method(self) -> None:
        """Explicit call() should handle unknown methods."""
        api = ExplicitApi()
        with pytest.raises(RpcError) as exc_info:
            await api.call("unknown", [])
        assert exc_info.value.code.value == "not_found"


@pytest.mark.asyncio
class TestMixedRpcTarget:
    """Test mixed explicit/automatic dispatch."""
    
    async def test_intercepted_method(self) -> None:
        """Intercepted methods should use custom logic."""
        api = MixedApi()
        result = await api.call("intercepted", ["test"])
        assert result == "intercepted!"
        assert api.intercepted == ["test"]
    
    async def test_regular_method_via_super(self) -> None:
        """Regular methods should be dispatched via super().call()."""
        api = MixedApi()
        result = await api.call("regular_method", [5])
        assert result == 10


@pytest.mark.asyncio
class TestPropertyAccess:
    """Test property access via get_property()."""
    
    async def test_public_property(self) -> None:
        """Public properties should be accessible."""
        api = PropertyApi()
        assert await api.get_property("name") == "PropertyApi"
        assert await api.get_property("version") == "2.0"
        assert await api.get_property("count") == 42
    
    async def test_private_property_not_exposed(self) -> None:
        """Private properties should not be exposed."""
        api = PropertyApi()
        with pytest.raises(RpcError) as exc_info:
            await api.get_property("_private_data")
        assert "not found" in str(exc_info.value).lower()
    
    async def test_nonexistent_property(self) -> None:
        """Nonexistent properties should raise RpcError."""
        api = PropertyApi()
        with pytest.raises(RpcError) as exc_info:
            await api.get_property("nonexistent")
        assert "not found" in str(exc_info.value).lower()
    
    async def test_method_not_exposed_as_property(self) -> None:
        """Methods should not be exposed as properties."""
        api = PropertyApi()
        with pytest.raises(RpcError) as exc_info:
            await api.get_property("get_info")
        assert "not found" in str(exc_info.value).lower()


# =============================================================================
# E2E Tests with BidirectionalSession
# =============================================================================

@pytest.mark.asyncio
class TestErgonomicE2E:
    """End-to-end tests with ergonomic RpcTarget over BidirectionalSession."""
    
    async def test_ergonomic_api_over_session(self) -> None:
        """Ergonomic API should work over BidirectionalSession."""
        transport_a, transport_b = create_transport_pair()
        
        server_api = ErgonomicApi()
        server = BidirectionalSession(transport_a, server_api)
        client = BidirectionalSession(transport_b, ClientCallback())
        
        server.start()
        client.start()
        
        try:
            server_stub = client.get_main_stub()
            
            # Call sync method (call is now synchronous)
            result_hook = server_stub.call(["hello"], RpcPayload.owned(["World"]))
            result = await asyncio.wait_for(result_hook.pull(), timeout=5.0)
            assert result.value == "Hello, World!"
            
            # Call async method
            result_hook = server_stub.call(["fetch_data"], RpcPayload.owned([42]))
            result = await asyncio.wait_for(result_hook.pull(), timeout=5.0)
            assert result.value == {"id": 42, "data": "data-42"}
            
            # Call method with multiple args
            result_hook = server_stub.call(["add"], RpcPayload.owned([10, 20]))
            result = await asyncio.wait_for(result_hook.pull(), timeout=5.0)
            assert result.value == 30
            
        finally:
            await server.stop()
            await client.stop()
    
    async def test_ergonomic_bidirectional(self) -> None:
        """Ergonomic API should work bidirectionally."""
        transport_a, transport_b = create_transport_pair()
        
        class ServerWithCallback(RpcTarget):
            def __init__(self):
                self.client_callback = None
            
            def register(self, callback: Any) -> str:
                self.client_callback = callback
                return "registered"
            
            async def trigger(self) -> str:
                if self.client_callback:
                    from capnweb.stubs import RpcStub
                    hook = self.client_callback._hook if isinstance(self.client_callback, RpcStub) else self.client_callback
                    result_hook = hook.call(["on_message"], RpcPayload.owned(["hello from server"]))
                    result = await result_hook.pull()
                    return result.value
                return "no callback"
        
        server_api = ServerWithCallback()
        client_callback = ClientCallback()
        
        server = BidirectionalSession(transport_a, server_api)
        client = BidirectionalSession(transport_b, client_callback)
        
        server.start()
        client.start()
        
        try:
            server_stub = client.get_main_stub()
            
            # Register callback
            from capnweb.stubs import RpcStub
            callback_stub = RpcStub(client._exports[0].hook.dup())
            result_hook = server_stub.call(["register"], RpcPayload.owned([callback_stub]))
            result = await asyncio.wait_for(result_hook.pull(), timeout=5.0)
            assert result.value == "registered"
            
            # Trigger callback
            result_hook = server_stub.call(["trigger"], RpcPayload.owned([]))
            result = await asyncio.wait_for(result_hook.pull(), timeout=5.0)
            assert result.value == "ack-1"
            
            # Verify callback was called
            assert client_callback.messages == ["hello from server"]
            
        finally:
            await server.stop()
            await client.stop()
    
    async def test_multiple_ergonomic_calls(self) -> None:
        """Multiple calls to ergonomic API should work."""
        transport_a, transport_b = create_transport_pair()
        
        server_api = ErgonomicApi()
        server = BidirectionalSession(transport_a, server_api)
        client = BidirectionalSession(transport_b, ClientCallback())
        
        server.start()
        client.start()
        
        try:
            server_stub = client.get_main_stub()
            
            # Make 20 calls
            for i in range(20):
                result_hook = server_stub.call(["hello"], RpcPayload.owned([f"User{i}"]))
                result = await asyncio.wait_for(result_hook.pull(), timeout=5.0)
                assert result.value == f"Hello, User{i}!"
            
            assert len(server_api.call_log) == 20
            
        finally:
            await server.stop()
            await client.stop()
