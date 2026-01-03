"""Extreme stress tests to find the breaking point of BidirectionalSession.

These tests push the system to its limits:
1. Extreme volume: 5000, 10000+ sequential calls
2. Extreme concurrency: 500, 1000 concurrent calls
3. Extreme bidirectional: 100 callbacks, 100 triggers (10,000 callback invocations)
4. Extreme payloads: 1MB messages
5. Extreme sustained load: 30+ seconds continuous
6. Breaking point finder: Increase load until failure

These tests are marked with pytest.mark.slow for optional exclusion.
"""

import asyncio
import gc
import statistics
import time
import tracemalloc
import pytest
from dataclasses import dataclass, field
from typing import Any
from aiohttp import web

from capnweb.rpc_session import BidirectionalSession
from capnweb.ws_transport import WebSocketClientTransport, WebSocketServerTransport
from capnweb.types import RpcTarget
from capnweb.error import RpcError
from capnweb.payload import RpcPayload
from capnweb.stubs import RpcStub


# =============================================================================
# Metrics (reused from validated tests)
# =============================================================================

@dataclass
class StressMetrics:
    """Collects stress test metrics."""
    
    latencies_ms: list[float] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    start_time: float = 0.0
    end_time: float = 0.0
    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    corrupted_responses: int = 0
    memory_start_bytes: int = 0
    memory_end_bytes: int = 0
    memory_peak_bytes: int = 0
    
    def start(self) -> None:
        self.start_time = time.perf_counter()
        gc.collect()
        tracemalloc.start()
        self.memory_start_bytes = tracemalloc.get_traced_memory()[0]
    
    def stop(self) -> None:
        self.end_time = time.perf_counter()
        current, peak = tracemalloc.get_traced_memory()
        self.memory_end_bytes = current
        self.memory_peak_bytes = peak
        tracemalloc.stop()
    
    def record_success(self, latency_ms: float) -> None:
        self.total_calls += 1
        self.successful_calls += 1
        self.latencies_ms.append(latency_ms)
    
    def record_failure(self, latency_ms: float, error: str) -> None:
        self.total_calls += 1
        self.failed_calls += 1
        self.latencies_ms.append(latency_ms)
        self.errors.append(error)
    
    def record_corruption(self) -> None:
        self.corrupted_responses += 1
    
    @property
    def duration_seconds(self) -> float:
        return self.end_time - self.start_time
    
    @property
    def throughput_per_second(self) -> float:
        if self.duration_seconds == 0:
            return 0
        return self.successful_calls / self.duration_seconds
    
    @property
    def error_rate_percent(self) -> float:
        if self.total_calls == 0:
            return 0
        return (self.failed_calls / self.total_calls) * 100
    
    @property
    def p50_ms(self) -> float:
        if not self.latencies_ms:
            return 0
        return statistics.median(self.latencies_ms)
    
    @property
    def p95_ms(self) -> float:
        if not self.latencies_ms:
            return 0
        sorted_lat = sorted(self.latencies_ms)
        return sorted_lat[int(len(sorted_lat) * 0.95)]
    
    @property
    def p99_ms(self) -> float:
        if not self.latencies_ms:
            return 0
        sorted_lat = sorted(self.latencies_ms)
        return sorted_lat[int(len(sorted_lat) * 0.99)]
    
    @property
    def max_ms(self) -> float:
        return max(self.latencies_ms) if self.latencies_ms else 0
    
    @property
    def memory_growth_mb(self) -> float:
        return (self.memory_end_bytes - self.memory_start_bytes) / (1024 * 1024)
    
    @property
    def memory_peak_mb(self) -> float:
        return self.memory_peak_bytes / (1024 * 1024)
    
    def report(self) -> str:
        return f"""
╔══════════════════════════════════════════════════════════════╗
║                    EXTREME STRESS TEST RESULTS               ║
╠══════════════════════════════════════════════════════════════╣
║ Duration:        {self.duration_seconds:>10.2f} seconds                      ║
║ Total Calls:     {self.total_calls:>10,}                               ║
║ Successful:      {self.successful_calls:>10,}                               ║
║ Failed:          {self.failed_calls:>10,}                               ║
║ Corrupted:       {self.corrupted_responses:>10,}                               ║
╠══════════════════════════════════════════════════════════════╣
║ Throughput:      {self.throughput_per_second:>10.1f} calls/sec                    ║
║ Error Rate:      {self.error_rate_percent:>10.2f}%                             ║
╠══════════════════════════════════════════════════════════════╣
║ Latency (ms):                                                ║
║   p50:           {self.p50_ms:>10.2f}                                   ║
║   p95:           {self.p95_ms:>10.2f}                                   ║
║   p99:           {self.p99_ms:>10.2f}                                   ║
║   max:           {self.max_ms:>10.2f}                                   ║
╠══════════════════════════════════════════════════════════════╣
║ Memory:                                                      ║
║   Growth:        {self.memory_growth_mb:>10.2f} MB                            ║
║   Peak:          {self.memory_peak_mb:>10.2f} MB                            ║
╚══════════════════════════════════════════════════════════════╝
"""


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


class EchoTarget(RpcTarget):
    """Ergonomic style."""
    def __init__(self):
        self.call_count = 0
    
    def echo(self, value: Any) -> Any:
        self.call_count += 1
        return value


class CallbackTarget(RpcTarget):
    """Ergonomic style."""
    def __init__(self):
        self.callbacks: list[Any] = []
        self.trigger_count = 0
    
    def register(self, callback: Any) -> int:
        self.callbacks.append(callback)
        return len(self.callbacks)
    
    async def trigger(self) -> int:
        self.trigger_count += 1
        results = []
        for i, cb in enumerate(self.callbacks):
            hook = cb._hook if isinstance(cb, RpcStub) else cb
            result_hook = hook.call(["onMessage"], RpcPayload.owned([f"t{self.trigger_count}-c{i}"]))
            result = await result_hook.pull()
            results.append(result.value)
        return len(results)
    
    async def trigger_parallel(self) -> int:
        self.trigger_count += 1
        async def call_cb(i: int, cb: Any) -> str:
            hook = cb._hook if isinstance(cb, RpcStub) else cb
            result_hook = hook.call(["onMessage"], RpcPayload.owned([f"p{self.trigger_count}-c{i}"]))
            result = await result_hook.pull()
            return result.value
        tasks = [call_cb(i, cb) for i, cb in enumerate(self.callbacks)]
        results = await asyncio.gather(*tasks)
        return len(results)


class ClientCallback(RpcTarget):
    """Ergonomic style."""
    def __init__(self):
        self.call_count = 0
    
    def onMessage(self, msg: str = "") -> str:
        self.call_count += 1
        return f"ack-{self.call_count}"


# =============================================================================
# Extreme Volume Tests
# =============================================================================

@pytest.mark.asyncio
class TestExtremeVolume:
    """Extreme volume tests - thousands of calls."""
    
    async def test_5000_sequential_calls(self) -> None:
        """5,000 sequential calls - testing sustained throughput."""
        transport_a, transport_b = create_transport_pair()
        target = EchoTarget()
        
        server = BidirectionalSession(transport_a, target)
        client = BidirectionalSession(transport_b, ClientCallback())
        
        server.start()
        client.start()
        
        metrics = StressMetrics()
        
        try:
            server_stub = client.get_main_stub()
            metrics.start()
            
            for i in range(5000):
                call_start = time.perf_counter()
                try:
                    result_hook = server_stub.call(["echo"], RpcPayload.owned([f"msg-{i}"]))
                    result = await asyncio.wait_for(result_hook.pull(), timeout=10.0)
                    latency_ms = (time.perf_counter() - call_start) * 1000
                    
                    if result.value == f"msg-{i}":
                        metrics.record_success(latency_ms)
                    else:
                        metrics.record_success(latency_ms)
                        metrics.record_corruption()
                except Exception as e:
                    latency_ms = (time.perf_counter() - call_start) * 1000
                    metrics.record_failure(latency_ms, str(e))
            
            metrics.stop()
            
        finally:
            await server.stop()
            await client.stop()
        
        print(metrics.report())
        
        # Acceptance criteria for 5000 calls
        assert metrics.error_rate_percent == 0, f"Errors: {metrics.failed_calls}"
        assert metrics.corrupted_responses == 0, f"Corrupted: {metrics.corrupted_responses}"
        assert metrics.throughput_per_second >= 100, f"Throughput too low: {metrics.throughput_per_second:.1f}/s"
        assert target.call_count == 5000, f"Server received {target.call_count} calls"
    
    async def test_10000_sequential_calls(self) -> None:
        """10,000 sequential calls - testing long-running stability."""
        transport_a, transport_b = create_transport_pair()
        target = EchoTarget()
        
        server = BidirectionalSession(transport_a, target)
        client = BidirectionalSession(transport_b, ClientCallback())
        
        server.start()
        client.start()
        
        metrics = StressMetrics()
        
        try:
            server_stub = client.get_main_stub()
            metrics.start()
            
            for i in range(10000):
                call_start = time.perf_counter()
                try:
                    result_hook = server_stub.call(["echo"], RpcPayload.owned([f"msg-{i}"]))
                    result = await asyncio.wait_for(result_hook.pull(), timeout=10.0)
                    latency_ms = (time.perf_counter() - call_start) * 1000
                    
                    if result.value == f"msg-{i}":
                        metrics.record_success(latency_ms)
                    else:
                        metrics.record_success(latency_ms)
                        metrics.record_corruption()
                except Exception as e:
                    latency_ms = (time.perf_counter() - call_start) * 1000
                    metrics.record_failure(latency_ms, str(e))
            
            metrics.stop()
            
        finally:
            await server.stop()
            await client.stop()
        
        print(metrics.report())
        
        assert metrics.error_rate_percent == 0
        assert metrics.corrupted_responses == 0
        assert metrics.throughput_per_second >= 100
        assert target.call_count == 10000


# =============================================================================
# Extreme Concurrency Tests
# =============================================================================

@pytest.mark.asyncio
class TestExtremeConcurrency:
    """Extreme concurrency tests - hundreds of concurrent calls."""
    
    async def test_500_concurrent_calls(self) -> None:
        """500 concurrent calls - testing concurrent handling."""
        transport_a, transport_b = create_transport_pair()
        target = EchoTarget()
        
        server = BidirectionalSession(transport_a, target)
        client = BidirectionalSession(transport_b, ClientCallback())
        
        server.start()
        client.start()
        
        metrics = StressMetrics()
        lock = asyncio.Lock()
        
        async def make_call(i: int) -> None:
            call_start = time.perf_counter()
            try:
                result_hook = server_stub.call(["echo"], RpcPayload.owned([f"concurrent-{i}"]))
                result = await asyncio.wait_for(result_hook.pull(), timeout=60.0)
                latency_ms = (time.perf_counter() - call_start) * 1000
                
                async with lock:
                    if result.value == f"concurrent-{i}":
                        metrics.record_success(latency_ms)
                    else:
                        metrics.record_success(latency_ms)
                        metrics.record_corruption()
            except Exception as e:
                latency_ms = (time.perf_counter() - call_start) * 1000
                async with lock:
                    metrics.record_failure(latency_ms, str(e))
        
        try:
            server_stub = client.get_main_stub()
            metrics.start()
            
            tasks = [make_call(i) for i in range(500)]
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=120.0)
            
            metrics.stop()
            
        finally:
            await server.stop()
            await client.stop()
        
        print(metrics.report())
        
        assert metrics.error_rate_percent == 0
        assert metrics.corrupted_responses == 0
        assert target.call_count == 500
    
    async def test_1000_concurrent_calls(self) -> None:
        """1,000 concurrent calls - pushing concurrency limits."""
        transport_a, transport_b = create_transport_pair()
        target = EchoTarget()
        
        server = BidirectionalSession(transport_a, target)
        client = BidirectionalSession(transport_b, ClientCallback())
        
        server.start()
        client.start()
        
        metrics = StressMetrics()
        lock = asyncio.Lock()
        
        async def make_call(i: int) -> None:
            call_start = time.perf_counter()
            try:
                result_hook = server_stub.call(["echo"], RpcPayload.owned([f"concurrent-{i}"]))
                result = await asyncio.wait_for(result_hook.pull(), timeout=120.0)
                latency_ms = (time.perf_counter() - call_start) * 1000
                
                async with lock:
                    if result.value == f"concurrent-{i}":
                        metrics.record_success(latency_ms)
                    else:
                        metrics.record_success(latency_ms)
                        metrics.record_corruption()
            except Exception as e:
                latency_ms = (time.perf_counter() - call_start) * 1000
                async with lock:
                    metrics.record_failure(latency_ms, str(e))
        
        try:
            server_stub = client.get_main_stub()
            metrics.start()
            
            tasks = [make_call(i) for i in range(1000)]
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=300.0)
            
            metrics.stop()
            
        finally:
            await server.stop()
            await client.stop()
        
        print(metrics.report())
        
        assert metrics.error_rate_percent == 0
        assert metrics.corrupted_responses == 0
        assert target.call_count == 1000


# =============================================================================
# Extreme Bidirectional Tests
# =============================================================================

@pytest.mark.asyncio
class TestExtremeBidirectional:
    """Extreme bidirectional tests - massive callback invocations."""
    
    async def test_100_callbacks_10_triggers(self) -> None:
        """100 callbacks, 10 triggers = 1,000 callback invocations."""
        transport_a, transport_b = create_transport_pair()
        
        server = BidirectionalSession(transport_a, CallbackTarget())
        client_callback = ClientCallback()
        client = BidirectionalSession(transport_b, client_callback)
        
        server.start()
        client.start()
        
        metrics = StressMetrics()
        
        try:
            server_stub = client.get_main_stub()
            
            # Register 100 callbacks
            for i in range(100):
                callback_stub = RpcStub(client._exports[0].hook.dup())
                result_hook = server_stub.call(["register"], RpcPayload.owned([callback_stub]))
                await asyncio.wait_for(result_hook.pull(), timeout=5.0)
            
            metrics.start()
            
            # Trigger 10 times (100 callbacks each = 1000 total)
            for i in range(10):
                call_start = time.perf_counter()
                try:
                    result_hook = server_stub.call(["trigger"], RpcPayload.owned([]))
                    result = await asyncio.wait_for(result_hook.pull(), timeout=60.0)
                    latency_ms = (time.perf_counter() - call_start) * 1000
                    
                    if result.value == 100:  # All 100 callbacks called
                        metrics.record_success(latency_ms)
                    else:
                        metrics.record_success(latency_ms)
                        metrics.record_corruption()
                except Exception as e:
                    latency_ms = (time.perf_counter() - call_start) * 1000
                    metrics.record_failure(latency_ms, str(e))
            
            metrics.stop()
            
        finally:
            await server.stop()
            await client.stop()
        
        print(metrics.report())
        print(f"Total callback invocations: {client_callback.call_count}")
        
        assert metrics.error_rate_percent == 0
        assert client_callback.call_count == 1000
    
    async def test_50_callbacks_50_triggers(self) -> None:
        """50 callbacks, 50 triggers = 2,500 callback invocations."""
        transport_a, transport_b = create_transport_pair()
        
        server = BidirectionalSession(transport_a, CallbackTarget())
        client_callback = ClientCallback()
        client = BidirectionalSession(transport_b, client_callback)
        
        server.start()
        client.start()
        
        metrics = StressMetrics()
        
        try:
            server_stub = client.get_main_stub()
            
            # Register 50 callbacks
            for i in range(50):
                callback_stub = RpcStub(client._exports[0].hook.dup())
                result_hook = server_stub.call(["register"], RpcPayload.owned([callback_stub]))
                await asyncio.wait_for(result_hook.pull(), timeout=5.0)
            
            metrics.start()
            
            # Trigger 50 times
            for i in range(50):
                call_start = time.perf_counter()
                try:
                    result_hook = server_stub.call(["trigger"], RpcPayload.owned([]))
                    result = await asyncio.wait_for(result_hook.pull(), timeout=120.0)
                    latency_ms = (time.perf_counter() - call_start) * 1000
                    metrics.record_success(latency_ms)
                except Exception as e:
                    latency_ms = (time.perf_counter() - call_start) * 1000
                    metrics.record_failure(latency_ms, str(e))
            
            metrics.stop()
            
        finally:
            await server.stop()
            await client.stop()
        
        print(metrics.report())
        print(f"Total callback invocations: {client_callback.call_count}")
        
        assert metrics.error_rate_percent == 0
        assert client_callback.call_count == 2500
    
    async def test_100_callbacks_100_triggers(self) -> None:
        """100 callbacks, 100 triggers = 10,000 callback invocations."""
        transport_a, transport_b = create_transport_pair()
        
        server = BidirectionalSession(transport_a, CallbackTarget())
        client_callback = ClientCallback()
        client = BidirectionalSession(transport_b, client_callback)
        
        server.start()
        client.start()
        
        metrics = StressMetrics()
        
        try:
            server_stub = client.get_main_stub()
            
            # Register 100 callbacks
            for i in range(100):
                callback_stub = RpcStub(client._exports[0].hook.dup())
                result_hook = server_stub.call(["register"], RpcPayload.owned([callback_stub]))
                await asyncio.wait_for(result_hook.pull(), timeout=5.0)
            
            metrics.start()
            
            # Trigger 100 times
            for i in range(100):
                call_start = time.perf_counter()
                try:
                    result_hook = server_stub.call(["trigger"], RpcPayload.owned([]))
                    result = await asyncio.wait_for(result_hook.pull(), timeout=120.0)
                    latency_ms = (time.perf_counter() - call_start) * 1000
                    metrics.record_success(latency_ms)
                except Exception as e:
                    latency_ms = (time.perf_counter() - call_start) * 1000
                    metrics.record_failure(latency_ms, str(e))
            
            metrics.stop()
            
        finally:
            await server.stop()
            await client.stop()
        
        print(metrics.report())
        print(f"Total callback invocations: {client_callback.call_count}")
        
        assert metrics.error_rate_percent == 0
        assert client_callback.call_count == 10000


# =============================================================================
# Extreme Payload Tests
# =============================================================================

@pytest.mark.asyncio
class TestExtremePayload:
    """Extreme payload tests - large messages."""
    
    async def test_100kb_payloads_100_calls(self) -> None:
        """100 calls with 100KB payloads = 10MB total data."""
        transport_a, transport_b = create_transport_pair()
        target = EchoTarget()
        
        server = BidirectionalSession(transport_a, target)
        client = BidirectionalSession(transport_b, ClientCallback())
        
        server.start()
        client.start()
        
        metrics = StressMetrics()
        payload = "x" * 102400  # 100KB
        
        try:
            server_stub = client.get_main_stub()
            metrics.start()
            
            for i in range(100):
                call_start = time.perf_counter()
                try:
                    result_hook = server_stub.call(["echo"], RpcPayload.owned([payload]))
                    result = await asyncio.wait_for(result_hook.pull(), timeout=30.0)
                    latency_ms = (time.perf_counter() - call_start) * 1000
                    
                    if len(result.value) == 102400:
                        metrics.record_success(latency_ms)
                    else:
                        metrics.record_success(latency_ms)
                        metrics.record_corruption()
                except Exception as e:
                    latency_ms = (time.perf_counter() - call_start) * 1000
                    metrics.record_failure(latency_ms, str(e))
            
            metrics.stop()
            
        finally:
            await server.stop()
            await client.stop()
        
        print(metrics.report())
        print(f"Total data transferred: {100 * 100 / 1024:.1f} MB")
        
        assert metrics.error_rate_percent == 0
        assert metrics.corrupted_responses == 0
    
    async def test_1mb_payloads_10_calls(self) -> None:
        """10 calls with 1MB payloads = 10MB total data."""
        transport_a, transport_b = create_transport_pair()
        target = EchoTarget()
        
        server = BidirectionalSession(transport_a, target)
        client = BidirectionalSession(transport_b, ClientCallback())
        
        server.start()
        client.start()
        
        metrics = StressMetrics()
        payload = "x" * 1048576  # 1MB
        
        try:
            server_stub = client.get_main_stub()
            metrics.start()
            
            for i in range(10):
                call_start = time.perf_counter()
                try:
                    result_hook = server_stub.call(["echo"], RpcPayload.owned([payload]))
                    result = await asyncio.wait_for(result_hook.pull(), timeout=60.0)
                    latency_ms = (time.perf_counter() - call_start) * 1000
                    
                    if len(result.value) == 1048576:
                        metrics.record_success(latency_ms)
                    else:
                        metrics.record_success(latency_ms)
                        metrics.record_corruption()
                except Exception as e:
                    latency_ms = (time.perf_counter() - call_start) * 1000
                    metrics.record_failure(latency_ms, str(e))
            
            metrics.stop()
            
        finally:
            await server.stop()
            await client.stop()
        
        print(metrics.report())
        print(f"Total data transferred: 10 MB")
        
        assert metrics.error_rate_percent == 0
        assert metrics.corrupted_responses == 0


# =============================================================================
# Extreme Sustained Load Tests
# =============================================================================

@pytest.mark.asyncio
class TestExtremeSustained:
    """Extreme sustained load tests - long-running continuous load."""
    
    async def test_sustained_30_seconds(self) -> None:
        """30 seconds of continuous load."""
        transport_a, transport_b = create_transport_pair()
        target = EchoTarget()
        
        server = BidirectionalSession(transport_a, target)
        client = BidirectionalSession(transport_b, ClientCallback())
        
        server.start()
        client.start()
        
        metrics = StressMetrics()
        
        try:
            server_stub = client.get_main_stub()
            metrics.start()
            
            test_duration = 30.0
            start = time.perf_counter()
            call_num = 0
            
            while time.perf_counter() - start < test_duration:
                call_start = time.perf_counter()
                try:
                    result_hook = server_stub.call(["echo"], RpcPayload.owned([f"sustained-{call_num}"]))
                    result = await asyncio.wait_for(result_hook.pull(), timeout=5.0)
                    latency_ms = (time.perf_counter() - call_start) * 1000
                    
                    if result.value == f"sustained-{call_num}":
                        metrics.record_success(latency_ms)
                    else:
                        metrics.record_success(latency_ms)
                        metrics.record_corruption()
                except Exception as e:
                    latency_ms = (time.perf_counter() - call_start) * 1000
                    metrics.record_failure(latency_ms, str(e))
                
                call_num += 1
            
            metrics.stop()
            
        finally:
            await server.stop()
            await client.stop()
        
        print(metrics.report())
        
        # Should maintain at least 100 calls/sec for 30 seconds
        assert metrics.error_rate_percent <= 1.0  # Allow 1% errors under sustained load
        assert metrics.throughput_per_second >= 100
        assert metrics.total_calls >= 3000  # At least 100/s * 30s


# =============================================================================
# Breaking Point Finder
# =============================================================================

@pytest.mark.asyncio
class TestBreakingPoint:
    """Find the breaking point of the system."""
    
    async def test_find_concurrent_limit(self) -> None:
        """Increase concurrency until we find issues."""
        concurrency_levels = [100, 200, 500, 1000, 2000]
        results = []
        
        for level in concurrency_levels:
            transport_a, transport_b = create_transport_pair()
            target = EchoTarget()
            
            server = BidirectionalSession(transport_a, target)
            client = BidirectionalSession(transport_b, ClientCallback())
            
            server.start()
            client.start()
            
            metrics = StressMetrics()
            lock = asyncio.Lock()
            
            async def make_call(i: int) -> None:
                call_start = time.perf_counter()
                try:
                    result_hook = server_stub.call(["echo"], RpcPayload.owned([f"c-{i}"]))
                    result = await asyncio.wait_for(result_hook.pull(), timeout=120.0)
                    latency_ms = (time.perf_counter() - call_start) * 1000
                    async with lock:
                        metrics.record_success(latency_ms)
                except Exception as e:
                    latency_ms = (time.perf_counter() - call_start) * 1000
                    async with lock:
                        metrics.record_failure(latency_ms, str(e))
            
            try:
                server_stub = client.get_main_stub()
                metrics.start()
                
                tasks = [make_call(i) for i in range(level)]
                await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=300.0)
                
                metrics.stop()
                
            except Exception as e:
                metrics.stop()
                results.append((level, "TIMEOUT/ERROR", str(e)))
                await server.stop()
                await client.stop()
                continue
            finally:
                await server.stop()
                await client.stop()
            
            results.append((
                level,
                "PASS" if metrics.error_rate_percent == 0 else "FAIL",
                f"{metrics.throughput_per_second:.0f}/s, p99={metrics.p99_ms:.0f}ms, errors={metrics.error_rate_percent:.1f}%"
            ))
            
            print(f"Concurrency {level}: {results[-1][1]} - {results[-1][2]}")
        
        print("\n=== CONCURRENCY BREAKING POINT SUMMARY ===")
        for level, status, details in results:
            print(f"  {level:>5} concurrent: {status:>10} - {details}")
        
        # At minimum, 100 concurrent should work
        assert results[0][1] == "PASS", f"Failed at 100 concurrent: {results[0][2]}"
    
    async def test_find_throughput_limit(self) -> None:
        """Measure maximum sustainable throughput."""
        transport_a, transport_b = create_transport_pair()
        target = EchoTarget()
        
        server = BidirectionalSession(transport_a, target)
        client = BidirectionalSession(transport_b, ClientCallback())
        
        server.start()
        client.start()
        
        try:
            server_stub = client.get_main_stub()
            
            # Warm up
            for i in range(100):
                result_hook = server_stub.call(["echo"], RpcPayload.owned(["warmup"]))
                await result_hook.pull()
            
            # Measure throughput over 10 seconds
            metrics = StressMetrics()
            metrics.start()
            
            test_duration = 10.0
            start = time.perf_counter()
            call_num = 0
            
            while time.perf_counter() - start < test_duration:
                call_start = time.perf_counter()
                try:
                    result_hook = server_stub.call(["echo"], RpcPayload.owned([f"t-{call_num}"]))
                    await asyncio.wait_for(result_hook.pull(), timeout=5.0)
                    latency_ms = (time.perf_counter() - call_start) * 1000
                    metrics.record_success(latency_ms)
                except Exception as e:
                    latency_ms = (time.perf_counter() - call_start) * 1000
                    metrics.record_failure(latency_ms, str(e))
                call_num += 1
            
            metrics.stop()
            
        finally:
            await server.stop()
            await client.stop()
        
        print(metrics.report())
        print(f"\n=== MAXIMUM THROUGHPUT: {metrics.throughput_per_second:.0f} calls/second ===")
        
        # Should achieve at least 100 calls/second
        assert metrics.throughput_per_second >= 100


# =============================================================================
# Real WebSocket Extreme Tests
# =============================================================================

@pytest.mark.asyncio
class TestWebSocketExtreme:
    """Extreme tests over real WebSocket."""
    
    async def test_websocket_1000_calls(self) -> None:
        """1,000 calls over real WebSocket."""
        
        async def handle_websocket(request):
            ws = web.WebSocketResponse()
            await ws.prepare(request)
            
            transport = WebSocketServerTransport(ws)
            session = BidirectionalSession(transport, EchoTarget())
            session.start()
            
            try:
                async for msg in ws:
                    if msg.type == web.WSMsgType.TEXT:
                        transport.feed_message(msg.data)
                    elif msg.type == web.WSMsgType.ERROR:
                        transport.set_error(ws.exception())
                        break
            finally:
                transport.set_closed()
                await session.stop()
            
            return ws
        
        app = web.Application()
        app.router.add_get("/ws", handle_websocket)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        
        metrics = StressMetrics()
        
        try:
            transport = WebSocketClientTransport(f"ws://127.0.0.1:{port}/ws")
            await transport.connect()
            
            session = BidirectionalSession(transport, ClientCallback())
            session.start()
            
            try:
                server_stub = session.get_main_stub()
                metrics.start()
                
                for i in range(1000):
                    call_start = time.perf_counter()
                    try:
                        result_hook = server_stub.call(["echo"], RpcPayload.owned([f"ws-{i}"]))
                        result = await asyncio.wait_for(result_hook.pull(), timeout=10.0)
                        latency_ms = (time.perf_counter() - call_start) * 1000
                        
                        if result.value == f"ws-{i}":
                            metrics.record_success(latency_ms)
                        else:
                            metrics.record_success(latency_ms)
                            metrics.record_corruption()
                    except Exception as e:
                        latency_ms = (time.perf_counter() - call_start) * 1000
                        metrics.record_failure(latency_ms, str(e))
                
                metrics.stop()
                
            finally:
                await session.stop()
                await transport.close()
                
        finally:
            await runner.cleanup()
        
        print(metrics.report())
        
        assert metrics.error_rate_percent == 0
        assert metrics.corrupted_responses == 0
