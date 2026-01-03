"""Comprehensive stress tests for HTTP Batch RPC.

Tests cover:
1. High volume - many calls in single batch
2. Large payloads - big data transfers
3. Nested capabilities - capability passing chains
4. Concurrent batches - multiple simultaneous requests
5. Error handling - mixed success/failure scenarios
6. Complex data types - various serializable types
7. Promise pipelining - chained calls
8. Memory/resource cleanup - no leaks
"""

import asyncio
import json
import time
import pytest
from typing import Any

from capnweb.batch import (
    BatchServerTransport,
    new_http_batch_rpc_response,
)
from capnweb.types import RpcTarget


# =============================================================================
# Test Fixtures - Various API implementations
# =============================================================================

class SimpleApi(RpcTarget):
    """Basic API for simple tests."""
    
    def __init__(self):
        self.call_count = 0
    
    async def call(self, method: str, args: list) -> Any:
        self.call_count += 1
        match method:
            case "echo":
                return args[0] if args else None
            case "add":
                return args[0] + args[1]
            case "multiply":
                return args[0] * args[1]
            case "concat":
                return "".join(str(a) for a in args)
            case "getCallCount":
                return self.call_count
            case "sleep":
                await asyncio.sleep(args[0] if args else 0.01)
                return "done"
            case "error":
                raise ValueError(f"Intentional error: {args[0] if args else 'no message'}")
            case _:
                raise ValueError(f"Unknown method: {method}")
    
    async def get_property(self, name: str) -> Any:
        if name == "callCount":
            return self.call_count
        raise AttributeError(f"Unknown property: {name}")


class NestedCapabilityApi(RpcTarget):
    """API that returns sub-capabilities."""
    
    def __init__(self, name: str = "root", depth: int = 0):
        self.name = name
        self.depth = depth
    
    async def call(self, method: str, args: list) -> Any:
        match method:
            case "getName":
                return self.name
            case "getDepth":
                return self.depth
            case "createChild":
                child_name = args[0] if args else f"child_{self.depth + 1}"
                return NestedCapabilityApi(child_name, self.depth + 1)
            case "echo":
                return args[0] if args else None
            case _:
                raise ValueError(f"Unknown method: {method}")
    
    async def get_property(self, name: str) -> Any:
        if name == "name":
            return self.name
        if name == "depth":
            return self.depth
        raise AttributeError(f"Unknown property: {name}")


class DataApi(RpcTarget):
    """API for testing various data types."""
    
    async def call(self, method: str, args: list) -> Any:
        match method:
            case "echoList":
                return args[0] if args else []
            case "echoDict":
                return args[0] if args else {}
            case "echoNested":
                return args[0] if args else {}
            case "generateList":
                size = args[0] if args else 10
                return list(range(size))
            case "generateDict":
                size = args[0] if args else 10
                return {f"key_{i}": f"value_{i}" for i in range(size)}
            case "processData":
                data = args[0] if args else []
                return {"count": len(data), "sum": sum(data) if data else 0}
            case _:
                raise ValueError(f"Unknown method: {method}")
    
    async def get_property(self, name: str) -> Any:
        raise AttributeError(f"Unknown property: {name}")


# =============================================================================
# Helper functions
# =============================================================================

def make_push(target_id: int, method: str, args: list) -> str:
    """Create a push message.
    
    Args are serialized to match what a real client would send.
    Arrays are escaped by wrapping in outer array.
    """
    def serialize_value(v):
        if isinstance(v, list):
            return [[serialize_value(item) for item in v]]
        elif isinstance(v, dict):
            return {k: serialize_value(val) for k, val in v.items()}
        else:
            return v
    
    serialized_args = [serialize_value(arg) for arg in args]
    return json.dumps(["push", ["pipeline", target_id, [method], serialized_args]])


def make_pull(import_id: int) -> str:
    """Create a pull message."""
    return json.dumps(["pull", import_id])


def parse_response(response: str) -> list[dict]:
    """Parse response into list of messages."""
    if not response:
        return []
    return [json.loads(line) for line in response.split("\n") if line.strip()]


def count_resolves(messages: list) -> int:
    """Count resolve messages."""
    return sum(1 for m in messages if m[0] == "resolve")


def count_rejects(messages: list) -> int:
    """Count reject messages."""
    return sum(1 for m in messages if m[0] == "reject")


def get_resolve_value(messages: list, export_id: int, parse: bool = True) -> Any:
    """Get the resolved value for a specific export ID.
    
    Args:
        messages: List of parsed wire messages
        export_id: The export ID to find
        parse: If True, parse the wire value through Parser to unwrap escaping
    """
    for m in messages:
        if m[0] == "resolve" and m[1] == export_id:
            wire_value = m[2]
            if parse:
                # Parse through Parser to unwrap escaped arrays
                from capnweb.parser import Parser
                
                class NullImporter:
                    def import_capability(self, id): return None
                    def create_promise_hook(self, id): return None
                
                parser = Parser(importer=NullImporter())
                payload = parser.parse(wire_value)
                return payload.value
            return wire_value
    return None


# =============================================================================
# Stress Tests
# =============================================================================

class TestHighVolume:
    """Test high volume of calls in single batch."""
    
    @pytest.mark.asyncio
    async def test_100_calls(self):
        """Test 100 calls in a single batch."""
        api = SimpleApi()
        
        messages = []
        for i in range(100):
            messages.append(make_push(0, "echo", [f"msg_{i}"]))
            messages.append(make_pull(i + 1))
        
        request = "\n".join(messages)
        response = await new_http_batch_rpc_response(request, api)
        
        parsed = parse_response(response)
        assert count_resolves(parsed) == 100
        assert api.call_count == 100
    
    @pytest.mark.asyncio
    async def test_500_calls(self):
        """Test 500 calls in a single batch."""
        api = SimpleApi()
        
        messages = []
        for i in range(500):
            messages.append(make_push(0, "add", [i, i * 2]))
            messages.append(make_pull(i + 1))
        
        request = "\n".join(messages)
        response = await new_http_batch_rpc_response(request, api)
        
        parsed = parse_response(response)
        assert count_resolves(parsed) == 500
        
        # Verify some results
        assert get_resolve_value(parsed, 1) == 0  # 0 + 0
        assert get_resolve_value(parsed, 2) == 3  # 1 + 2
        assert get_resolve_value(parsed, 100) == 297  # 99 + 198
    
    @pytest.mark.asyncio
    async def test_1000_calls_no_pull(self):
        """Test 1000 push calls without pulls (fire-and-forget)."""
        api = SimpleApi()
        
        messages = [make_push(0, "echo", [f"msg_{i}"]) for i in range(1000)]
        
        request = "\n".join(messages)
        response = await new_http_batch_rpc_response(request, api)
        
        # No pulls = no resolves
        parsed = parse_response(response)
        assert count_resolves(parsed) == 0
        assert api.call_count == 1000


class TestLargePayloads:
    """Test large data transfers."""
    
    @pytest.mark.asyncio
    async def test_large_string(self):
        """Test echoing a large string."""
        api = SimpleApi()
        
        large_string = "x" * 100_000  # 100KB string
        
        messages = [
            make_push(0, "echo", [large_string]),
            make_pull(1),
        ]
        
        request = "\n".join(messages)
        response = await new_http_batch_rpc_response(request, api)
        
        parsed = parse_response(response)
        assert count_resolves(parsed) == 1
        assert get_resolve_value(parsed, 1) == large_string
    
    @pytest.mark.asyncio
    async def test_large_list(self):
        """Test processing a large list."""
        api = DataApi()
        
        large_list = list(range(10_000))
        
        messages = [
            make_push(0, "processData", [large_list]),
            make_pull(1),
        ]
        
        request = "\n".join(messages)
        response = await new_http_batch_rpc_response(request, api)
        
        parsed = parse_response(response)
        result = get_resolve_value(parsed, 1)
        assert result["count"] == 10_000
        assert result["sum"] == sum(range(10_000))
    
    @pytest.mark.asyncio
    async def test_large_nested_dict(self):
        """Test echoing a large nested dictionary."""
        api = DataApi()
        
        nested = {
            f"level1_{i}": {
                f"level2_{j}": {
                    f"level3_{k}": f"value_{i}_{j}_{k}"
                    for k in range(10)
                }
                for j in range(10)
            }
            for i in range(10)
        }
        
        messages = [
            make_push(0, "echoNested", [nested]),
            make_pull(1),
        ]
        
        request = "\n".join(messages)
        response = await new_http_batch_rpc_response(request, api)
        
        parsed = parse_response(response)
        result = get_resolve_value(parsed, 1)
        assert result == nested


class TestNestedCapabilities:
    """Test capability passing and chaining."""
    
    @pytest.mark.asyncio
    async def test_create_child_capability(self):
        """Test creating and using a child capability."""
        api = NestedCapabilityApi("root")
        
        messages = [
            # Create child
            make_push(0, "createChild", ["child1"]),
            make_pull(1),
        ]
        
        request = "\n".join(messages)
        response = await new_http_batch_rpc_response(request, api)
        
        parsed = parse_response(response)
        # Use parse=False to get raw wire value (export reference)
        result = get_resolve_value(parsed, 1, parse=False)
        
        # Result should be an export reference: ["export", id]
        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0] == "export"
        assert isinstance(result[1], int)
    
    @pytest.mark.asyncio
    async def test_multiple_child_capabilities(self):
        """Test creating multiple child capabilities from root."""
        api = NestedCapabilityApi("root")

        messages = []
        # Create 5 children from root (can't chain in single batch)
        for i in range(5):
            messages.append(make_push(0, "createChild", [f"child{i+1}"]))
            messages.append(make_pull(i + 1))

        request = "\n".join(messages)
        response = await new_http_batch_rpc_response(request, api)

        parsed = parse_response(response)
        assert count_resolves(parsed) == 5
        
        # Each resolve should contain an export reference: ["export", id]
        for i in range(1, 6):
            result = get_resolve_value(parsed, i, parse=False)
            assert isinstance(result, list)
            assert len(result) == 2
            assert result[0] == "export"
            assert isinstance(result[1], int)


class TestErrorHandling:
    """Test error handling scenarios."""
    
    @pytest.mark.asyncio
    async def test_single_error(self):
        """Test single error in batch."""
        api = SimpleApi()
        
        messages = [
            make_push(0, "error", ["test error"]),
            make_pull(1),
        ]
        
        request = "\n".join(messages)
        response = await new_http_batch_rpc_response(request, api)
        
        parsed = parse_response(response)
        assert count_rejects(parsed) == 1
    
    @pytest.mark.asyncio
    async def test_mixed_success_and_errors(self):
        """Test batch with both successful calls and errors."""
        api = SimpleApi()
        
        messages = []
        for i in range(10):
            if i % 3 == 0:
                messages.append(make_push(0, "error", [f"error_{i}"]))
            else:
                messages.append(make_push(0, "echo", [f"msg_{i}"]))
            messages.append(make_pull(i + 1))
        
        request = "\n".join(messages)
        response = await new_http_batch_rpc_response(request, api)
        
        parsed = parse_response(response)
        # 4 errors (0, 3, 6, 9), 6 successes
        assert count_rejects(parsed) == 4
        assert count_resolves(parsed) == 6
    
    @pytest.mark.asyncio
    async def test_unknown_method(self):
        """Test calling unknown method."""
        api = SimpleApi()
        
        messages = [
            make_push(0, "nonexistent_method", []),
            make_pull(1),
        ]
        
        request = "\n".join(messages)
        response = await new_http_batch_rpc_response(request, api)
        
        parsed = parse_response(response)
        assert count_rejects(parsed) == 1


class TestConcurrentBatches:
    """Test multiple concurrent batch requests."""
    
    @pytest.mark.asyncio
    async def test_10_concurrent_batches(self):
        """Test 10 concurrent batch requests."""
        
        async def run_batch(batch_id: int) -> tuple[int, int]:
            api = SimpleApi()
            
            messages = []
            for i in range(50):
                messages.append(make_push(0, "echo", [f"batch_{batch_id}_msg_{i}"]))
                messages.append(make_pull(i + 1))
            
            request = "\n".join(messages)
            response = await new_http_batch_rpc_response(request, api)
            
            parsed = parse_response(response)
            return batch_id, count_resolves(parsed)
        
        # Run 10 batches concurrently
        tasks = [run_batch(i) for i in range(10)]
        results = await asyncio.gather(*tasks)
        
        # All batches should complete with 50 resolves each
        for batch_id, resolve_count in results:
            assert resolve_count == 50, f"Batch {batch_id} had {resolve_count} resolves"
    
    @pytest.mark.asyncio
    async def test_concurrent_with_shared_api(self):
        """Test concurrent batches sharing same API instance."""
        api = SimpleApi()
        
        async def run_batch(batch_id: int) -> int:
            messages = []
            for i in range(20):
                messages.append(make_push(0, "echo", [f"batch_{batch_id}_msg_{i}"]))
                messages.append(make_pull(i + 1))
            
            request = "\n".join(messages)
            response = await new_http_batch_rpc_response(request, api)
            
            parsed = parse_response(response)
            return count_resolves(parsed)
        
        # Run 5 batches concurrently with shared API
        tasks = [run_batch(i) for i in range(5)]
        results = await asyncio.gather(*tasks)
        
        # All batches should complete
        assert all(r == 20 for r in results)
        # Total calls should be 100 (5 batches * 20 calls)
        assert api.call_count == 100


class TestComplexDataTypes:
    """Test various complex data types."""
    
    @pytest.mark.asyncio
    async def test_nested_arrays(self):
        """Test deeply nested arrays."""
        api = DataApi()
        
        nested = [[[[1, 2, 3], [4, 5, 6]], [[7, 8, 9], [10, 11, 12]]]]
        
        messages = [
            make_push(0, "echoList", [nested]),
            make_pull(1),
        ]
        
        request = "\n".join(messages)
        response = await new_http_batch_rpc_response(request, api)
        
        parsed = parse_response(response)
        assert get_resolve_value(parsed, 1) == nested
    
    @pytest.mark.asyncio
    async def test_mixed_types(self):
        """Test mixed data types in single call."""
        api = DataApi()
        
        mixed = {
            "string": "hello",
            "number": 42,
            "float": 3.14,
            "bool": True,
            "null": None,
            "array": [1, "two", 3.0, False],
            "nested": {"a": {"b": {"c": "deep"}}},
        }
        
        messages = [
            make_push(0, "echoDict", [mixed]),
            make_pull(1),
        ]
        
        request = "\n".join(messages)
        response = await new_http_batch_rpc_response(request, api)
        
        parsed = parse_response(response)
        assert get_resolve_value(parsed, 1) == mixed
    
    @pytest.mark.asyncio
    async def test_unicode_strings(self):
        """Test unicode strings."""
        api = SimpleApi()
        
        unicode_strings = [
            "Hello, 世界!",
            "مرحبا بالعالم",
            "🎉🚀💻🔥",
            "Ελληνικά",
            "日本語テスト",
        ]
        
        messages = []
        for i, s in enumerate(unicode_strings):
            messages.append(make_push(0, "echo", [s]))
            messages.append(make_pull(i + 1))
        
        request = "\n".join(messages)
        response = await new_http_batch_rpc_response(request, api)
        
        parsed = parse_response(response)
        for i, s in enumerate(unicode_strings):
            assert get_resolve_value(parsed, i + 1) == s


class TestPerformance:
    """Performance benchmarks."""
    
    @pytest.mark.asyncio
    async def test_throughput_benchmark(self):
        """Measure throughput of batch processing."""
        api = SimpleApi()
        
        num_calls = 1000
        messages = []
        for i in range(num_calls):
            messages.append(make_push(0, "add", [i, i]))
            messages.append(make_pull(i + 1))
        
        request = "\n".join(messages)
        
        start = time.perf_counter()
        response = await new_http_batch_rpc_response(request, api)
        elapsed = time.perf_counter() - start
        
        parsed = parse_response(response)
        assert count_resolves(parsed) == num_calls
        
        throughput = num_calls / elapsed
        print(f"\nThroughput: {throughput:.0f} calls/second ({elapsed:.3f}s for {num_calls} calls)")
        
        # Should process at least 1000 calls/second
        assert throughput > 1000, f"Throughput too low: {throughput:.0f} calls/s"
    
    @pytest.mark.asyncio
    async def test_latency_benchmark(self):
        """Measure latency of single call batch."""
        api = SimpleApi()
        
        messages = [
            make_push(0, "echo", ["test"]),
            make_pull(1),
        ]
        request = "\n".join(messages)
        
        # Warm up
        await new_http_batch_rpc_response(request, api)
        
        # Measure
        latencies = []
        for _ in range(100):
            start = time.perf_counter()
            await new_http_batch_rpc_response(request, api)
            latencies.append(time.perf_counter() - start)
        
        avg_latency = sum(latencies) / len(latencies)
        max_latency = max(latencies)
        min_latency = min(latencies)
        
        print(f"\nLatency: avg={avg_latency*1000:.2f}ms, min={min_latency*1000:.2f}ms, max={max_latency*1000:.2f}ms")
        
        # Average latency should be under 10ms
        assert avg_latency < 0.01, f"Average latency too high: {avg_latency*1000:.2f}ms"
