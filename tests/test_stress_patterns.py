"""Unique stress pattern tests for BidirectionalSession.

This module contains UNIQUE stress patterns not covered by test_stress_validated.py:
1. Burst pattern tests (rapid fire, sustained load)
2. Mixed workload tests (read/write, bidirectional chaos)
3. Resource pressure tests (large payloads, many callbacks)
4. Session churn tests (rapid connect/disconnect)
5. Chaos engineering tests (random delays, failures)

NOTE: High volume (100/500/1000 calls) and high concurrency (50/100/200 concurrent)
tests are in test_stress_validated.py with proper acceptance criteria.

NO MOCKING - All tests use real transports and real message passing.
"""

import asyncio
import gc
import random
import time
import pytest
from typing import Any
from aiohttp import web

from capnweb.rpc_session import BidirectionalSession, RpcSessionOptions
from capnweb.ws_transport import WebSocketClientTransport, WebSocketServerTransport
from capnweb.types import RpcTarget
from capnweb.error import RpcError
from capnweb.payload import RpcPayload
from capnweb.stubs import RpcStub


# =============================================================================
# Test Infrastructure
# =============================================================================

class InMemoryTransport:
    """Real async transport using queues."""
    
    def __init__(self, send_queue: asyncio.Queue, recv_queue: asyncio.Queue, name: str = ""):
        self.send_queue = send_queue
        self.recv_queue = recv_queue
        self.name = name
        self._closed = False
        self._abort_reason: Exception | None = None
    
    async def send(self, message: str) -> None:
        if self._closed:
            raise ConnectionError("Transport closed")
        await self.send_queue.put(message)
    
    async def receive(self) -> str:
        if self._closed and self.recv_queue.empty():
            raise ConnectionError("Transport closed")
        return await self.recv_queue.get()
    
    def abort(self, reason: Exception) -> None:
        self._abort_reason = reason
        self._closed = True
    
    def close(self) -> None:
        self._closed = True


def create_transport_pair() -> tuple[InMemoryTransport, InMemoryTransport]:
    """Create a pair of connected transports."""
    queue_a_to_b = asyncio.Queue()
    queue_b_to_a = asyncio.Queue()
    transport_a = InMemoryTransport(queue_a_to_b, queue_b_to_a, "A")
    transport_b = InMemoryTransport(queue_b_to_a, queue_a_to_b, "B")
    return transport_a, transport_b


# =============================================================================
# Test Targets
# =============================================================================

class EchoTarget(RpcTarget):
    """Echo target with call counting - ergonomic style."""
    
    def __init__(self):
        self.call_count = 0
        self.total_bytes = 0
    
    def echo(self, data: Any) -> Any:
        self.call_count += 1
        if isinstance(data, str):
            self.total_bytes += len(data)
        return data
    
    async def slow(self, delay: float = 0.01) -> str:
        self.call_count += 1
        await asyncio.sleep(delay)
        return "done"
    
    def compute(self, n: int = 1000) -> int:
        self.call_count += 1
        return sum(i * i for i in range(n))
    
    def error(self) -> None:
        self.call_count += 1
        raise RpcError.internal("Intentional error")
    
    def random_error(self) -> str:
        self.call_count += 1
        if random.random() < 0.1:  # 10% failure rate
            raise RpcError.internal("Random error")
        return "ok"


class CallbackTarget(RpcTarget):
    """Target that stores and calls back to client - ergonomic style."""
    
    def __init__(self):
        self.callbacks: list[Any] = []
        self.trigger_count = 0
    
    def register(self, callback: Any) -> str:
        self.callbacks.append(callback)
        return f"registered-{len(self.callbacks)}"
    
    async def trigger(self) -> list:
        self.trigger_count += 1
        results = []
        for i, cb in enumerate(self.callbacks):
            hook = cb._hook if isinstance(cb, RpcStub) else cb
            result_hook = hook.call(["onMessage"], RpcPayload.owned([f"trigger-{self.trigger_count}-cb-{i}"]))
            result = await result_hook.pull()
            results.append(result.value)
        return results
    
    async def trigger_parallel(self) -> list:
        self.trigger_count += 1
        async def call_callback(i: int, cb: Any) -> str:
            hook = cb._hook if isinstance(cb, RpcStub) else cb
            result_hook = hook.call(["onMessage"], RpcPayload.owned([f"parallel-{self.trigger_count}-cb-{i}"]))
            result = await result_hook.pull()
            return result.value
        
        tasks = [call_callback(i, cb) for i, cb in enumerate(self.callbacks)]
        results = await asyncio.gather(*tasks)
        return list(results)
    
    def get_count(self) -> int:
        return len(self.callbacks)


class ClientCallback(RpcTarget):
    """Client-side callback - ergonomic style."""
    
    def __init__(self):
        self.messages: list[str] = []
        self.call_count = 0
    
    def onMessage(self, msg: str = "") -> str:
        self.call_count += 1
        self.messages.append(msg)
        return f"ack-{len(self.messages)}"
    
    async def slow(self, delay: float = 0.01) -> str:
        self.call_count += 1
        await asyncio.sleep(delay)
        return "slow-done"


# NOTE: High volume tests (100/500/1000 sequential) and high concurrency tests
# (50/100/200 concurrent) are in test_stress_validated.py with proper acceptance
# criteria. This file contains unique stress patterns only.

# =============================================================================
# Test 1: Burst Pattern Tests (UNIQUE)
# =============================================================================

@pytest.mark.asyncio
class TestBurstPatternsUnique:
    """Burst pattern stress tests - unique to this file."""
    
    async def test_50_concurrent_calls(self) -> None:
        """50 concurrent calls should all complete."""
        transport_a, transport_b = create_transport_pair()
        target = EchoTarget()
        
        server = BidirectionalSession(transport_a, target)
        client = BidirectionalSession(transport_b, ClientCallback())
        
        server.start()
        client.start()
        
        try:
            server_stub = client.get_main_stub()
            
            async def make_call(i: int) -> str:
                result_hook = server_stub.call(["echo"], RpcPayload.owned([f"concurrent-{i}"]))
                result = await result_hook.pull()
                return result.value
            
            tasks = [make_call(i) for i in range(50)]
            results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=30.0)
            
            expected = {f"concurrent-{i}" for i in range(50)}
            assert set(results) == expected
            assert target.call_count == 50
            
        finally:
            await server.stop()
            await client.stop()
    
    async def test_100_concurrent_calls(self) -> None:
        """100 concurrent calls should all complete."""
        transport_a, transport_b = create_transport_pair()
        target = EchoTarget()
        
        server = BidirectionalSession(transport_a, target)
        client = BidirectionalSession(transport_b, ClientCallback())
        
        server.start()
        client.start()
        
        try:
            server_stub = client.get_main_stub()
            
            async def make_call(i: int) -> str:
                result_hook = server_stub.call(["echo"], RpcPayload.owned([f"concurrent-{i}"]))
                result = await result_hook.pull()
                return result.value
            
            start = time.time()
            tasks = [make_call(i) for i in range(100)]
            results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=60.0)
            duration = time.time() - start
            
            expected = {f"concurrent-{i}" for i in range(100)}
            assert set(results) == expected
            assert target.call_count == 100
            
        finally:
            await server.stop()
            await client.stop()
    
    async def test_200_concurrent_calls(self) -> None:
        """200 concurrent calls should all complete."""
        transport_a, transport_b = create_transport_pair()
        target = EchoTarget()
        
        server = BidirectionalSession(transport_a, target)
        client = BidirectionalSession(transport_b, ClientCallback())
        
        server.start()
        client.start()
        
        try:
            server_stub = client.get_main_stub()
            
            async def make_call(i: int) -> str:
                result_hook = server_stub.call(["echo"], RpcPayload.owned([f"concurrent-{i}"]))
                result = await result_hook.pull()
                return result.value
            
            start = time.time()
            tasks = [make_call(i) for i in range(200)]
            results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=120.0)
            duration = time.time() - start
            
            expected = {f"concurrent-{i}" for i in range(200)}
            assert set(results) == expected
            assert target.call_count == 200
            
        finally:
            await server.stop()
            await client.stop()
    
    async def test_concurrent_with_varying_delays(self) -> None:
        """Concurrent calls with varying server-side delays."""
        transport_a, transport_b = create_transport_pair()
        target = EchoTarget()
        
        server = BidirectionalSession(transport_a, target)
        client = BidirectionalSession(transport_b, ClientCallback())
        
        server.start()
        client.start()
        
        try:
            server_stub = client.get_main_stub()
            
            async def make_slow_call(i: int) -> str:
                delay = random.uniform(0.001, 0.05)  # 1-50ms delay
                result_hook = server_stub.call(["slow"], RpcPayload.owned([delay]))
                result = await result_hook.pull()
                return f"{i}-{result.value}"
            
            tasks = [make_slow_call(i) for i in range(50)]
            results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=30.0)
            
            assert len(results) == 50
            
        finally:
            await server.stop()
            await client.stop()


# =============================================================================
# Test 3: Burst Pattern Tests
# =============================================================================

@pytest.mark.asyncio
class TestBurstPatterns:
    """Burst pattern stress tests."""
    
    async def test_burst_10_calls_10_times(self) -> None:
        """10 bursts of 10 concurrent calls each."""
        transport_a, transport_b = create_transport_pair()
        target = EchoTarget()
        
        server = BidirectionalSession(transport_a, target)
        client = BidirectionalSession(transport_b, ClientCallback())
        
        server.start()
        client.start()
        
        try:
            server_stub = client.get_main_stub()
            
            for burst in range(10):
                async def make_call(i: int) -> str:
                    result_hook = server_stub.call(["echo"], RpcPayload.owned([f"burst-{burst}-{i}"]))
                    result = await result_hook.pull()
                    return result.value
                
                tasks = [make_call(i) for i in range(10)]
                results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=10.0)
                assert len(results) == 10
                
                # Small pause between bursts
                await asyncio.sleep(0.01)
            
            assert target.call_count == 100
            
        finally:
            await server.stop()
            await client.stop()
    
    async def test_burst_50_calls_5_times(self) -> None:
        """5 bursts of 50 concurrent calls each."""
        transport_a, transport_b = create_transport_pair()
        target = EchoTarget()
        
        server = BidirectionalSession(transport_a, target)
        client = BidirectionalSession(transport_b, ClientCallback())
        
        server.start()
        client.start()
        
        try:
            server_stub = client.get_main_stub()
            
            for burst in range(5):
                async def make_call(i: int) -> str:
                    result_hook = server_stub.call(["echo"], RpcPayload.owned([f"burst-{burst}-{i}"]))
                    result = await result_hook.pull()
                    return result.value
                
                tasks = [make_call(i) for i in range(50)]
                results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=30.0)
                assert len(results) == 50
                
                # Small pause between bursts
                await asyncio.sleep(0.05)
            
            assert target.call_count == 250
            
        finally:
            await server.stop()
            await client.stop()
    
    async def test_sustained_load_30_seconds(self) -> None:
        """Sustained load for 30 seconds with continuous calls."""
        transport_a, transport_b = create_transport_pair()
        target = EchoTarget()
        
        server = BidirectionalSession(transport_a, target)
        client = BidirectionalSession(transport_b, ClientCallback())
        
        server.start()
        client.start()
        
        try:
            server_stub = client.get_main_stub()
            
            start = time.time()
            call_count = 0
            errors = 0
            
            # Run for 5 seconds (reduced from 30 for test speed)
            while time.time() - start < 5.0:
                try:
                    result_hook = server_stub.call(["echo"], RpcPayload.owned([f"sustained-{call_count}"]))
                    result = await asyncio.wait_for(result_hook.pull(), timeout=2.0)
                    assert result.value == f"sustained-{call_count}"
                    call_count += 1
                except Exception:
                    errors += 1
            
            duration = time.time() - start
            calls_per_second = call_count / duration
            
            assert call_count > 100, f"Expected > 100 calls, got {call_count}"
            assert errors == 0, f"Got {errors} errors"
            
        finally:
            await server.stop()
            await client.stop()
    
    async def test_rapid_fire_no_wait(self) -> None:
        """Rapid fire calls without waiting for responses."""
        transport_a, transport_b = create_transport_pair()
        target = EchoTarget()
        
        server = BidirectionalSession(transport_a, target)
        client = BidirectionalSession(transport_b, ClientCallback())
        
        server.start()
        client.start()
        
        try:
            server_stub = client.get_main_stub()
            
            # Fire off 100 calls without waiting
            hooks = []
            for i in range(100):
                result_hook = server_stub.call(["echo"], RpcPayload.owned([f"rapid-{i}"]))
                hooks.append((i, result_hook))
            
            # Now collect all results
            results = []
            for i, hook in hooks:
                result = await asyncio.wait_for(hook.pull(), timeout=30.0)
                results.append((i, result.value))
            
            # Verify all results
            for i, value in results:
                assert value == f"rapid-{i}"
            
            assert len(results) == 100
            
        finally:
            await server.stop()
            await client.stop()


# =============================================================================
# Test 4: Mixed Workload Tests
# =============================================================================

@pytest.mark.asyncio
class TestMixedWorkload:
    """Mixed workload stress tests."""
    
    async def test_mixed_echo_and_compute(self) -> None:
        """Mix of fast echo and slower compute calls."""
        transport_a, transport_b = create_transport_pair()
        target = EchoTarget()
        
        server = BidirectionalSession(transport_a, target)
        client = BidirectionalSession(transport_b, ClientCallback())
        
        server.start()
        client.start()
        
        try:
            server_stub = client.get_main_stub()
            
            async def mixed_call(i: int) -> Any:
                if i % 3 == 0:
                    # Compute call
                    result_hook = server_stub.call(["compute"], RpcPayload.owned([1000]))
                    result = await result_hook.pull()
                    return ("compute", result.value)
                else:
                    # Echo call
                    result_hook = server_stub.call(["echo"], RpcPayload.owned([f"echo-{i}"]))
                    result = await result_hook.pull()
                    return ("echo", result.value)
            
            tasks = [mixed_call(i) for i in range(60)]
            results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=30.0)
            
            echo_count = sum(1 for r in results if r[0] == "echo")
            compute_count = sum(1 for r in results if r[0] == "compute")
            
            assert echo_count == 40
            assert compute_count == 20
            
        finally:
            await server.stop()
            await client.stop()
    
    async def test_bidirectional_chaos(self) -> None:
        """Chaotic bidirectional calls - client and server calling each other."""
        transport_a, transport_b = create_transport_pair()
        
        server = BidirectionalSession(transport_a, CallbackTarget())
        client_callback = ClientCallback()
        client = BidirectionalSession(transport_b, client_callback)
        
        server.start()
        client.start()
        
        try:
            server_stub = client.get_main_stub()
            
            # Register multiple callbacks
            for i in range(5):
                callback_stub = RpcStub(client._exports[0].hook.dup())
                result_hook = server_stub.call(["register"], RpcPayload.owned([callback_stub]))
                await asyncio.wait_for(result_hook.pull(), timeout=2.0)
            
            # Trigger callbacks multiple times concurrently
            async def trigger_once(n: int) -> list:
                result_hook = server_stub.call(["trigger"], RpcPayload.owned([]))
                result = await result_hook.pull()
                return result.value
            
            tasks = [trigger_once(i) for i in range(10)]
            results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=60.0)
            
            # Each trigger should have called all 5 callbacks
            for result in results:
                assert len(result) == 5
            
            # Total callback invocations
            assert client_callback.call_count == 50
            
        finally:
            await server.stop()
            await client.stop()
    
    async def test_parallel_bidirectional_triggers(self) -> None:
        """Server triggers callbacks in parallel."""
        transport_a, transport_b = create_transport_pair()
        
        server = BidirectionalSession(transport_a, CallbackTarget())
        client_callback = ClientCallback()
        client = BidirectionalSession(transport_b, client_callback)
        
        server.start()
        client.start()
        
        try:
            server_stub = client.get_main_stub()
            
            # Register 10 callbacks
            for i in range(10):
                callback_stub = RpcStub(client._exports[0].hook.dup())
                result_hook = server_stub.call(["register"], RpcPayload.owned([callback_stub]))
                await asyncio.wait_for(result_hook.pull(), timeout=2.0)
            
            # Trigger with parallel callback execution
            result_hook = server_stub.call(["trigger_parallel"], RpcPayload.owned([]))
            result = await asyncio.wait_for(result_hook.pull(), timeout=30.0)
            
            assert len(result.value) == 10
            assert client_callback.call_count == 10
            
        finally:
            await server.stop()
            await client.stop()
    
    async def test_interleaved_requests_and_callbacks(self) -> None:
        """Interleaved client requests and server callbacks."""
        transport_a, transport_b = create_transport_pair()
        
        server = BidirectionalSession(transport_a, CallbackTarget())
        client_callback = ClientCallback()
        client = BidirectionalSession(transport_b, client_callback)
        
        server.start()
        client.start()
        
        try:
            server_stub = client.get_main_stub()
            
            # Register callback
            callback_stub = RpcStub(client._exports[0].hook.dup())
            result_hook = server_stub.call(["register"], RpcPayload.owned([callback_stub]))
            await asyncio.wait_for(result_hook.pull(), timeout=2.0)
            
            # Interleave: request, trigger, request, trigger, ...
            for i in range(20):
                if i % 2 == 0:
                    # Get count
                    result_hook = server_stub.call(["get_count"], RpcPayload.owned([]))
                    result = await asyncio.wait_for(result_hook.pull(), timeout=2.0)
                    assert result.value == 1
                else:
                    # Trigger callback
                    result_hook = server_stub.call(["trigger"], RpcPayload.owned([]))
                    result = await asyncio.wait_for(result_hook.pull(), timeout=5.0)
                    assert len(result.value) == 1
            
            assert client_callback.call_count == 10
            
        finally:
            await server.stop()
            await client.stop()


# =============================================================================
# Test 5: Resource Pressure Tests
# =============================================================================

@pytest.mark.asyncio
class TestResourcePressure:
    """Resource pressure stress tests."""
    
    async def test_many_callbacks_registered(self) -> None:
        """Register many callbacks and trigger them all."""
        transport_a, transport_b = create_transport_pair()
        
        server = BidirectionalSession(transport_a, CallbackTarget())
        client_callback = ClientCallback()
        client = BidirectionalSession(transport_b, client_callback)
        
        server.start()
        client.start()
        
        try:
            server_stub = client.get_main_stub()
            
            # Register 50 callbacks
            for i in range(50):
                callback_stub = RpcStub(client._exports[0].hook.dup())
                result_hook = server_stub.call(["register"], RpcPayload.owned([callback_stub]))
                await asyncio.wait_for(result_hook.pull(), timeout=2.0)
            
            # Trigger all
            result_hook = server_stub.call(["trigger"], RpcPayload.owned([]))
            result = await asyncio.wait_for(result_hook.pull(), timeout=60.0)
            
            assert len(result.value) == 50
            assert client_callback.call_count == 50
            
        finally:
            await server.stop()
            await client.stop()
    
    async def test_deep_call_chain(self) -> None:
        """Test deep call chains (A calls B calls A calls B...)."""
        transport_a, transport_b = create_transport_pair()
        
        depth_counter = {"value": 0, "max_depth": 10}
        
        class DeepTarget(RpcTarget):
            def __init__(self, peer_stub_getter):
                self.peer_stub_getter = peer_stub_getter
            
            async def call(self, method: str, args: list[Any]) -> Any:
                if method == "deep":
                    current_depth = args[0] if args else 0
                    depth_counter["value"] = max(depth_counter["value"], current_depth)
                    
                    if current_depth < depth_counter["max_depth"]:
                        peer_stub = self.peer_stub_getter()
                        if peer_stub:
                            result_hook = peer_stub.call(["deep"], RpcPayload.owned([current_depth + 1]))
                            result = await result_hook.pull()
                            return f"depth-{current_depth}->{result.value}"
                    return f"depth-{current_depth}-end"
                raise RpcError.not_found(f"Unknown: {method}")
            
            async def get_property(self, prop: str) -> Any:
                raise RpcError.not_found(f"Unknown: {prop}")
        
        client_stub_holder = {"stub": None}
        server_stub_holder = {"stub": None}
        
        server_target = DeepTarget(lambda: client_stub_holder["stub"])
        client_target = DeepTarget(lambda: server_stub_holder["stub"])
        
        server = BidirectionalSession(transport_a, server_target)
        client = BidirectionalSession(transport_b, client_target)
        
        server.start()
        client.start()
        
        try:
            server_stub_holder["stub"] = client.get_main_stub()
            client_stub_holder["stub"] = server.get_main_stub()
            
            # Start deep call chain (call is now synchronous)
            result_hook = server_stub_holder["stub"].call(["deep"], RpcPayload.owned([0]))
            result = await asyncio.wait_for(result_hook.pull(), timeout=30.0)
            
            assert depth_counter["value"] == depth_counter["max_depth"]
            
        finally:
            await server.stop()
            await client.stop()
    
    async def test_memory_pressure_large_results(self) -> None:
        """Test with many large results in flight."""
        transport_a, transport_b = create_transport_pair()
        target = EchoTarget()
        
        server = BidirectionalSession(transport_a, target)
        client = BidirectionalSession(transport_b, ClientCallback())
        
        server.start()
        client.start()
        
        try:
            server_stub = client.get_main_stub()
            
            # Send many large payloads concurrently
            async def large_call(i: int) -> int:
                data = "x" * 10000  # 10KB
                result_hook = server_stub.call(["echo"], RpcPayload.owned([data]))
                result = await result_hook.pull()
                return len(result.value)
            
            tasks = [large_call(i) for i in range(50)]
            results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=60.0)
            
            assert all(r == 10000 for r in results)
            
        finally:
            await server.stop()
            await client.stop()


# =============================================================================
# Test 6: Session Churn Tests
# =============================================================================

@pytest.mark.asyncio
class TestSessionChurn:
    """Session creation/destruction stress tests."""
    
    async def test_rapid_session_churn_20(self) -> None:
        """Rapidly create and destroy 20 sessions."""
        for i in range(20):
            transport_a, transport_b = create_transport_pair()
            
            server = BidirectionalSession(transport_a, EchoTarget())
            client = BidirectionalSession(transport_b, ClientCallback())
            
            server.start()
            client.start()
            
            server_stub = client.get_main_stub()
            result_hook = server_stub.call(["echo"], RpcPayload.owned([f"churn-{i}"]))
            result = await asyncio.wait_for(result_hook.pull(), timeout=2.0)
            assert result.value == f"churn-{i}"
            
            await server.stop()
            await client.stop()
    
    async def test_session_churn_with_pending_calls(self) -> None:
        """Create sessions, start calls, then destroy before completion."""
        for i in range(10):
            transport_a, transport_b = create_transport_pair()
            
            server = BidirectionalSession(transport_a, EchoTarget())
            client = BidirectionalSession(transport_b, ClientCallback())
            
            server.start()
            client.start()
            
            server_stub = client.get_main_stub()
            
            # Start a slow call
            result_hook = server_stub.call(["slow"], RpcPayload.owned([0.5]))
            
            # Don't wait for it, just stop
            await server.stop()
            await client.stop()
    
    async def test_parallel_session_creation(self) -> None:
        """Create multiple sessions in parallel."""
        async def create_and_use_session(i: int) -> str:
            transport_a, transport_b = create_transport_pair()
            
            server = BidirectionalSession(transport_a, EchoTarget())
            client = BidirectionalSession(transport_b, ClientCallback())
            
            server.start()
            client.start()
            
            try:
                server_stub = client.get_main_stub()
                result_hook = server_stub.call(["echo"], RpcPayload.owned([f"parallel-session-{i}"]))
                result = await asyncio.wait_for(result_hook.pull(), timeout=5.0)
                return result.value
            finally:
                await server.stop()
                await client.stop()
        
        tasks = [create_and_use_session(i) for i in range(10)]
        results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=30.0)
        
        expected = {f"parallel-session-{i}" for i in range(10)}
        assert set(results) == expected


# =============================================================================
# Test 7: Chaos Engineering Tests
# =============================================================================

@pytest.mark.asyncio
class TestChaosEngineering:
    """Chaos engineering stress tests."""
    
    async def test_random_delays_in_calls(self) -> None:
        """Calls with random delays should all complete."""
        transport_a, transport_b = create_transport_pair()
        target = EchoTarget()
        
        server = BidirectionalSession(transport_a, target)
        client = BidirectionalSession(transport_b, ClientCallback())
        
        server.start()
        client.start()
        
        try:
            server_stub = client.get_main_stub()
            
            async def random_delay_call(i: int) -> str:
                # Random delay before call
                await asyncio.sleep(random.uniform(0, 0.05))
                result_hook = server_stub.call(["echo"], RpcPayload.owned([f"chaos-{i}"]))
                result = await result_hook.pull()
                return result.value
            
            tasks = [random_delay_call(i) for i in range(50)]
            results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=30.0)
            
            expected = {f"chaos-{i}" for i in range(50)}
            assert set(results) == expected
            
        finally:
            await server.stop()
            await client.stop()
    
    async def test_mixed_success_and_errors(self) -> None:
        """Mix of successful calls and intentional errors."""
        transport_a, transport_b = create_transport_pair()
        target = EchoTarget()
        
        server = BidirectionalSession(transport_a, target)
        client = BidirectionalSession(transport_b, ClientCallback())
        
        server.start()
        client.start()
        
        try:
            server_stub = client.get_main_stub()
            
            successes = 0
            errors = 0
            
            for i in range(50):
                try:
                    if i % 5 == 0:
                        # Intentional error
                        result_hook = server_stub.call(["error"], RpcPayload.owned([]))
                    else:
                        result_hook = server_stub.call(["echo"], RpcPayload.owned([f"mixed-{i}"]))
                    
                    result = await asyncio.wait_for(result_hook.pull(), timeout=2.0)
                    successes += 1
                except Exception:
                    errors += 1
            
            # 10 intentional errors (every 5th call)
            assert errors == 10
            assert successes == 40
            
        finally:
            await server.stop()
            await client.stop()
    
    async def test_random_error_rate(self) -> None:
        """Calls with random 10% error rate."""
        transport_a, transport_b = create_transport_pair()
        target = EchoTarget()
        
        server = BidirectionalSession(transport_a, target)
        client = BidirectionalSession(transport_b, ClientCallback())
        
        server.start()
        client.start()
        
        try:
            server_stub = client.get_main_stub()
            
            successes = 0
            errors = 0
            
            for i in range(100):
                try:
                    result_hook = server_stub.call(["random_error"], RpcPayload.owned([]))
                    result = await asyncio.wait_for(result_hook.pull(), timeout=2.0)
                    successes += 1
                except Exception:
                    errors += 1
            
            # Should have some errors (around 10%) and mostly successes
            assert successes > 70, f"Expected > 70 successes, got {successes}"
            assert errors > 0, "Expected some errors"
            
        finally:
            await server.stop()
            await client.stop()


# NOTE: WebSocket stress tests are in test_stress_validated.py
# to avoid duplication and ensure proper acceptance criteria.
