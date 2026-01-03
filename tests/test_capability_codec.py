"""Tests for CapabilityCodec - value encoding with capability table integration.

These tests verify:
1. Encoding of Python values including capabilities
2. Decoding of wire format including capability resolution
3. Round-trip consistency
4. Integration with mock Importer/Exporter
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import pytest

from capnweb.capability_codec import (
    CapabilityCodec,
    CapabilityCodecOptions,
    create_capability_codec,
)
from capnweb.error import RpcError
from capnweb.hooks import StubHook
from capnweb.payload import RpcPayload
from capnweb.stubs import RpcPromise, RpcStub
from capnweb.value_codec import ValueCodecOptions


# =============================================================================
# Mock Importer/Exporter for testing
# =============================================================================


@dataclass
class MockStubHook:
    """Mock StubHook for testing."""
    id: int
    kind: str  # "export" or "promise"
    
    def call(self, *args: Any, **kwargs: Any) -> Any:
        pass
    
    def add_release_callback(self, callback: Any) -> None:
        pass


class MockImporter:
    """Mock Importer that tracks imports."""
    
    def __init__(self) -> None:
        self.imports: dict[int, MockStubHook] = {}
        self.promises: dict[int, MockStubHook] = {}
        self.exports: dict[int, MockStubHook] = {}
    
    def import_exported_capability(self, export_id: int) -> StubHook:
        hook = MockStubHook(export_id, "export")
        self.imports[export_id] = hook
        return hook  # type: ignore
    
    def create_promise_hook(self, promise_id: int) -> StubHook:
        hook = MockStubHook(promise_id, "promise")
        self.promises[promise_id] = hook
        return hook  # type: ignore
    
    def get_export(self, export_id: int) -> StubHook | None:
        return self.exports.get(export_id)  # type: ignore


class MockExporter:
    """Mock Exporter that tracks exports."""
    
    def __init__(self) -> None:
        self.next_stub_id = 0
        self.next_promise_id = 0
        self.exported_stubs: dict[int, RpcStub] = {}
        self.exported_promises: dict[int, RpcPromise] = {}
    
    def export_stub(self, stub: RpcStub) -> int:
        export_id = self.next_stub_id
        self.next_stub_id += 1
        self.exported_stubs[export_id] = stub
        return export_id
    
    def export_promise(self, promise: RpcPromise) -> int:
        export_id = self.next_promise_id
        self.next_promise_id += 1
        self.exported_promises[export_id] = promise
        return export_id


# =============================================================================
# Tests
# =============================================================================


class TestCapabilityCodecEncode:
    """Test encoding Python values to wire format."""

    def test_encode_primitives(self) -> None:
        """Primitives encode correctly."""
        codec = CapabilityCodec()
        assert codec.encode(None) is None
        assert codec.encode(True) is True
        assert codec.encode(42) == 42
        assert codec.encode(3.14) == 3.14
        assert codec.encode("hello") == "hello"

    def test_encode_list_escaped(self) -> None:
        """Lists are escaped with [[...]]."""
        codec = CapabilityCodec()
        assert codec.encode([1, 2, 3]) == [[1, 2, 3]]
        assert codec.encode([]) == [[]]

    def test_encode_dict(self) -> None:
        """Dicts encode correctly."""
        codec = CapabilityCodec()
        assert codec.encode({"a": 1}) == {"a": 1}

    def test_encode_datetime(self) -> None:
        """Datetime converts to date tag."""
        codec = CapabilityCodec()
        dt = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        encoded = codec.encode(dt)
        assert encoded == ["date", dt.timestamp() * 1000]

    def test_encode_bytes(self) -> None:
        """Bytes convert to bytes tag."""
        codec = CapabilityCodec()
        encoded = codec.encode(b"hello")
        assert encoded[0] == "bytes"
        assert isinstance(encoded[1], str)  # base64

    def test_encode_stub_requires_exporter(self) -> None:
        """Encoding RpcStub without exporter raises error."""
        codec = CapabilityCodec()
        stub = RpcStub(MockStubHook(0, "export"))  # type: ignore
        with pytest.raises(RuntimeError, match="without exporter"):
            codec.encode(stub)

    def test_encode_stub_with_exporter(self) -> None:
        """Encoding RpcStub with exporter produces export tag."""
        exporter = MockExporter()
        codec = CapabilityCodec(exporter=exporter)
        stub = RpcStub(MockStubHook(0, "export"))  # type: ignore
        encoded = codec.encode(stub)
        assert encoded == ["export", 0]
        assert 0 in exporter.exported_stubs

    def test_encode_promise_with_exporter(self) -> None:
        """Encoding RpcPromise with exporter produces promise tag."""
        exporter = MockExporter()
        codec = CapabilityCodec(exporter=exporter)
        promise = RpcPromise(MockStubHook(0, "promise"))  # type: ignore
        encoded = codec.encode(promise)
        assert encoded == ["promise", 0]
        assert 0 in exporter.exported_promises

    def test_encode_error(self) -> None:
        """RpcError encodes to error tag (not double-wrapped)."""
        codec = CapabilityCodec()
        error = RpcError.bad_request("test error")
        encoded = codec.encode(error)
        # Should be ["error", type, message] - NOT [["error", ...]]
        assert isinstance(encoded, list)
        assert encoded[0] == "error"
        assert encoded[1] == "bad_request"
        assert encoded[2] == "test error"

    def test_encode_dict_with_non_string_key_rejected(self) -> None:
        """Dict with non-string key raises TypeError."""
        codec = CapabilityCodec()
        with pytest.raises(TypeError, match="must be strings"):
            codec.encode({1: "value"})  # type: ignore


class TestCapabilityCodecDecode:
    """Test decoding wire format to Python values."""

    def test_decode_primitives(self) -> None:
        """Primitives decode correctly."""
        codec = CapabilityCodec()
        assert codec.decode(None) is None
        assert codec.decode(True) is True
        assert codec.decode(42) == 42
        assert codec.decode(3.14) == 3.14
        assert codec.decode("hello") == "hello"

    def test_decode_escaped_list(self) -> None:
        """Escaped lists [[...]] decode correctly."""
        codec = CapabilityCodec()
        assert codec.decode([[1, 2, 3]]) == [1, 2, 3]
        assert codec.decode([[]]) == []

    def test_decode_dict(self) -> None:
        """Dicts decode correctly."""
        codec = CapabilityCodec()
        assert codec.decode({"a": 1}) == {"a": 1}

    def test_decode_date(self) -> None:
        """Date tag decodes to datetime."""
        codec = CapabilityCodec()
        ms = 1704067200000  # 2024-01-01 00:00:00 UTC
        decoded = codec.decode(["date", ms])
        assert isinstance(decoded, datetime)
        assert decoded.timestamp() == ms / 1000

    def test_decode_bytes(self) -> None:
        """Bytes tag decodes to bytes."""
        codec = CapabilityCodec()
        import base64
        b64 = base64.b64encode(b"hello").decode('ascii')
        decoded = codec.decode(["bytes", b64])
        assert decoded == b"hello"

    def test_decode_bigint(self) -> None:
        """BigInt tag decodes to int."""
        codec = CapabilityCodec()
        decoded = codec.decode(["bigint", "12345678901234567890"])
        assert decoded == 12345678901234567890

    def test_decode_undefined(self) -> None:
        """Undefined tag decodes to None."""
        codec = CapabilityCodec()
        decoded = codec.decode(["undefined"])
        assert decoded is None

    def test_decode_nan(self) -> None:
        """NaN tag decodes to float('nan')."""
        import math
        codec = CapabilityCodec()
        decoded = codec.decode(["nan"])
        assert math.isnan(decoded)

    def test_decode_inf(self) -> None:
        """Inf tags decode to float('inf')."""
        codec = CapabilityCodec()
        assert codec.decode(["inf"]) == float("inf")
        assert codec.decode(["-inf"]) == float("-inf")

    def test_decode_export_requires_importer(self) -> None:
        """Decoding export tag without importer raises error."""
        codec = CapabilityCodec()
        with pytest.raises(RuntimeError, match="without importer"):
            codec.decode(["export", 0])

    def test_decode_export_with_importer(self) -> None:
        """Decoding export tag with importer creates RpcStub."""
        importer = MockImporter()
        codec = CapabilityCodec(importer=importer)
        decoded = codec.decode(["export", 42])
        assert isinstance(decoded, RpcStub)
        assert 42 in importer.imports

    def test_decode_promise_with_importer(self) -> None:
        """Decoding promise tag with importer creates RpcPromise."""
        importer = MockImporter()
        codec = CapabilityCodec(importer=importer)
        decoded = codec.decode(["promise", 5])
        assert isinstance(decoded, RpcPromise)
        assert 5 in importer.promises

    def test_decode_to_payload(self) -> None:
        """decode_to_payload wraps result in RpcPayload."""
        codec = CapabilityCodec()
        payload = codec.decode_to_payload([[1, 2, 3]])
        assert isinstance(payload, RpcPayload)
        assert payload.value == [1, 2, 3]


class TestCapabilityCodecRoundtrip:
    """Test encode/decode round-trips."""

    def test_roundtrip_primitives(self) -> None:
        """Primitives round-trip correctly."""
        codec = CapabilityCodec()
        for value in [None, True, False, 42, 3.14, "hello"]:
            assert codec.decode(codec.encode(value)) == value

    def test_roundtrip_list(self) -> None:
        """Lists round-trip correctly."""
        codec = CapabilityCodec()
        original = [1, 2, [3, 4], {"a": 5}]
        assert codec.decode(codec.encode(original)) == original

    def test_roundtrip_dict(self) -> None:
        """Dicts round-trip correctly."""
        codec = CapabilityCodec()
        original = {"a": 1, "b": [2, 3], "c": {"d": 4}}
        assert codec.decode(codec.encode(original)) == original

    def test_roundtrip_datetime(self) -> None:
        """Datetime round-trips correctly."""
        codec = CapabilityCodec()
        original = datetime(2024, 1, 1, 12, 30, 45, tzinfo=timezone.utc)
        decoded = codec.decode(codec.encode(original))
        assert isinstance(decoded, datetime)
        # Compare timestamps (sub-second precision may vary)
        assert abs(decoded.timestamp() - original.timestamp()) < 0.001

    def test_roundtrip_bytes(self) -> None:
        """Bytes round-trip correctly."""
        codec = CapabilityCodec()
        original = b"hello world"
        assert codec.decode(codec.encode(original)) == original


class TestCapabilityCodecOptions:
    """Test codec options."""

    def test_filter_dangerous_keys(self) -> None:
        """Dangerous keys are filtered by default."""
        codec = CapabilityCodec()
        wire = {"__proto__": "bad", "safe": 1}
        decoded = codec.decode(wire)
        assert "__proto__" not in decoded
        assert decoded["safe"] == 1

    def test_dangerous_keys_not_filtered_when_disabled(self) -> None:
        """Dangerous keys pass through when filtering disabled."""
        opts = CapabilityCodecOptions(filter_dangerous_keys=False)
        codec = CapabilityCodec(opts)
        wire = {"__proto__": "bad", "safe": 1}
        decoded = codec.decode(wire)
        assert decoded["__proto__"] == "bad"


class TestErrorRoundtrip:
    """Test error encoding/decoding round-trip."""

    def test_error_not_double_wrapped(self) -> None:
        """Errors are NOT double-wrapped (critical correctness)."""
        codec = CapabilityCodec()
        error = RpcError.bad_request("test")
        encoded = codec.encode(error)
        # First element should be "error", not a list
        assert encoded[0] == "error"
        assert not isinstance(encoded[0], list)

    def test_error_decode_to_rpc_error(self) -> None:
        """Error decodes to RpcError (with error_mode='return')."""
        codec = CapabilityCodec()
        wire = ["error", "bad_request", "test message"]
        decoded = codec.decode(wire)
        assert isinstance(decoded, RpcError)
        assert decoded.message == "test message"


class TestPreservedTagsNotRecursed:
    """Test that preserved unknown tags are not recursed into."""

    def test_unknown_tag_preserved_opaque(self) -> None:
        """Unknown tags should NOT be recursed into."""
        codec = CapabilityCodec()
        # This contains an unknown tag with nested content
        wire = ["futureType", {"nested": [[1, 2, 3]]}]
        decoded = codec.decode(wire)
        # Should be exactly the same - no recursive resolution
        assert decoded == wire

    def test_known_values_still_resolved(self) -> None:
        """Known values (not in tagged arrays) should still resolve."""
        importer = MockImporter()
        codec = CapabilityCodec(importer=importer)
        wire = {"stub": ["export", 42], "data": [[1, 2]]}
        decoded = codec.decode(wire)
        assert isinstance(decoded["stub"], RpcStub)
        assert decoded["data"] == [1, 2]


class TestFactoryFunction:
    """Test create_capability_codec factory."""

    def test_create_with_importer_exporter(self) -> None:
        """Factory creates codec with importer/exporter."""
        importer = MockImporter()
        exporter = MockExporter()
        codec = create_capability_codec(importer=importer, exporter=exporter)
        
        # Test encoding with exporter
        stub = RpcStub(MockStubHook(0, "export"))  # type: ignore
        encoded = codec.encode(stub)
        assert encoded == ["export", 0]
        
        # Test decoding with importer
        decoded = codec.decode(["export", 5])
        assert isinstance(decoded, RpcStub)
