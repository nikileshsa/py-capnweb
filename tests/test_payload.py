"""Tests for RpcPayload - ownership semantics and resource tracking."""

import copy
from dataclasses import dataclass

from capnweb.hooks import PayloadStubHook
from capnweb.payload import PayloadSource, RpcPayload
from capnweb.rpc_session import BidirectionalSession
from capnweb.stubs import RpcPromise, RpcStub
from tests.conftest import create_transport_pair


class TestPayloadCreation:
    """Test different payload creation methods."""

    def test_from_app_params(self):
        """Test creating payload from application parameters."""
        value = {"key": "value"}
        payload = RpcPayload.from_app_params(value)

        assert payload.value is value
        assert payload.source == PayloadSource.PARAMS
        assert len(payload.stubs) == 0
        assert len(payload.promises) == 0

    def test_from_app_return(self):
        """Test creating payload from application return value."""
        value = [1, 2, 3]
        payload = RpcPayload.from_app_return(value)

        assert payload.value is value
        assert payload.source == PayloadSource.RETURN
        assert len(payload.stubs) == 0
        assert len(payload.promises) == 0

    def test_owned(self):
        """Test creating owned payload."""
        value = {"data": 42}
        payload = RpcPayload.owned(value)

        assert payload.value is value
        assert payload.source == PayloadSource.OWNED
        assert len(payload.stubs) == 0
        assert len(payload.promises) == 0


class TestPayloadEnsureDeepCopied:
    """Test ensure_deep_copied method."""

    def test_owned_payload_no_copy(self):
        """Test that owned payloads aren't copied."""
        original = {"data": "test"}
        payload = RpcPayload.owned(original)

        payload.ensure_deep_copied()

        # Should still be the same object
        assert payload.value is original
        assert payload.source == PayloadSource.OWNED

    def test_params_payload_deep_copied(self):
        """Test that PARAMS payloads are deep copied."""
        original = {"data": [1, 2, 3]}
        payload = RpcPayload.from_app_params(original)

        payload.ensure_deep_copied()

        # Should be a deep copy
        assert payload.value is not original
        assert payload.value == original
        assert payload.source == PayloadSource.OWNED

        # Modifications to copy shouldn't affect original
        payload.value["data"].append(4)
        assert original["data"] == [1, 2, 3]

    def test_return_payload_ownership_transfer(self):
        """Test that RETURN payloads are deep copied (matching TypeScript behavior)."""
        original = {"data": "test"}
        payload = RpcPayload.from_app_return(original)

        payload.ensure_deep_copied()

        # TypeScript always deep copies in ensureDeepCopied(), even for RETURN
        # The value is a new object with the same content
        assert payload.value == original
        assert payload.value is not original  # Deep copied
        assert payload.source == PayloadSource.OWNED

    def test_ensure_deep_copied_idempotent(self):
        """Test that ensure_deep_copied can be called multiple times."""
        original = {"data": "test"}
        payload = RpcPayload.from_app_params(original)

        payload.ensure_deep_copied()
        first_copy = payload.value

        payload.ensure_deep_copied()
        second_copy = payload.value

        # Should be the same copy
        assert first_copy is second_copy


class TestPayloadDeepCopyTracking:
    """Test deep copy with RPC reference tracking."""

    def test_deep_copy_with_stub(self):
        """Test deep copying payload containing RpcStub."""
        stub_hook = PayloadStubHook(RpcPayload.owned("inner"))
        stub = RpcStub(stub_hook)

        payload = RpcPayload.from_app_params({"stub": stub})
        payload.ensure_deep_copied()

        # Should have tracked the stub
        assert len(payload.stubs) == 1
        # The stub in the payload should be a duplicate
        assert payload.value["stub"] is not stub
        # But should share the same hook (via dup)

    def test_deep_copy_with_promise(self):
        """Test deep copying payload containing RpcPromise."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        hook = session.create_promise_hook(1)
        promise = RpcPromise(hook)

        payload = RpcPayload.from_app_params({"promise": promise})
        payload.ensure_deep_copied()

        # Promise should be duplicated
        assert payload.value["promise"] is not promise
        # Original promise hook should still exist

    def test_deep_copy_primitives(self):
        """Test deep copying primitives."""
        payload = RpcPayload.from_app_params({
            "none": None,
            "bool": True,
            "int": 42,
            "float": 3.14,
            "string": "hello",
            "bytes": b"data",
        })

        payload.ensure_deep_copied()

        assert payload.value["none"] is None
        assert payload.value["bool"] is True
        assert payload.value["int"] == 42
        assert payload.value["float"] == 3.14
        assert payload.value["string"] == "hello"
        assert payload.value["bytes"] == b"data"

    def test_deep_copy_nested_structures(self):
        """Test deep copying nested lists and dicts."""
        original = {
            "users": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
            "metadata": {"total": 2},
        }
        payload = RpcPayload.from_app_params(original)

        payload.ensure_deep_copied()

        # Should be a deep copy
        assert payload.value is not original
        assert payload.value["users"] is not original["users"]  # type: ignore[index]
        assert payload.value["users"][0] is not original["users"][0]  # type: ignore[index]

        # But values should be equal
        assert payload.value == original

    def test_deep_copy_custom_object_with_deepcopy(self):
        """Test deep copying custom objects that support deepcopy."""

        @dataclass
        class CustomClass:
            value: int

            def __deepcopy__(self, memo):
                return CustomClass(copy.deepcopy(self.value, memo))

        obj = CustomClass(42)
        payload = RpcPayload.from_app_params({"obj": obj})

        payload.ensure_deep_copied()

        # Should be a deep copy
        assert payload.value["obj"] is not obj
        assert payload.value["obj"] == obj

    def test_deep_copy_custom_object_fallback(self):
        """Test deep copying custom objects without deepcopy support."""

        class ImmutableClass:
            def __init__(self, value):
                self.value = value

            def __deepcopy__(self, memo):
                msg = "Can't deepcopy"
                raise TypeError(msg)

        obj = ImmutableClass(42)
        payload = RpcPayload.from_app_params({"obj": obj})

        payload.ensure_deep_copied()

        # Should return the same object (fallback behavior)
        assert payload.value["obj"] is obj


class TestPayloadTrackReferences:
    """Test tracking references in RETURN payloads."""

    def test_track_stub_in_return(self):
        """Test tracking stubs in RETURN payloads."""
        stub_hook = PayloadStubHook(RpcPayload.owned("data"))
        stub = RpcStub(stub_hook)

        payload = RpcPayload.from_app_return({"stub": stub})
        payload.ensure_deep_copied()

        # Should have tracked the stub
        assert len(payload.stubs) == 1
        # The tracked stub is the NEW stub in the deep-copied value
        # (not the original - deep copy creates new objects)
        assert payload.stubs[0] is payload.value["stub"]

    def test_track_promise_in_return(self):
        """Test tracking promises in RETURN payloads."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        hook = session.create_promise_hook(1)
        promise = RpcPromise(hook)

        payload = RpcPayload.from_app_return({"promise": promise})
        payload.ensure_deep_copied()

        # Should have tracked the promise
        assert len(payload.promises) == 1
        _parent, key, tracked_promise = payload.promises[0]
        # The tracked promise is the NEW promise in the deep-copied value
        # (not the original - deep copy creates new objects)
        assert tracked_promise is payload.value["promise"]
        assert key == "promise"

    def test_track_promise_in_list(self):
        """Test tracking promises in lists."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        hook = session.create_promise_hook(1)
        promise = RpcPromise(hook)

        payload = RpcPayload.from_app_return([promise, "data"])
        payload.ensure_deep_copied()

        # Should have tracked the promise
        assert len(payload.promises) == 1
        _parent, key, tracked_promise = payload.promises[0]
        # The tracked promise is the NEW promise in the deep-copied value
        assert tracked_promise is payload.value[0]
        assert key == 0  # Index in list

    def test_track_nested_stubs_and_promises(self):
        """Test tracking nested stubs and promises."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)

        stub_hook = PayloadStubHook(RpcPayload.owned("stub_data"))
        stub = RpcStub(stub_hook)

        promise_hook = session.create_promise_hook(1)
        promise = RpcPromise(promise_hook)

        payload = RpcPayload.from_app_return({
            "level1": {"stub": stub, "promise": promise, "list": [stub, promise]}
        })
        payload.ensure_deep_copied()

        # Should have tracked all stubs and promises
        assert len(payload.stubs) == 2  # stub appears twice
        assert len(payload.promises) == 2  # promise appears twice


class TestPayloadDispose:
    """Test payload disposal."""

    def test_dispose_payload_with_stubs(self):
        """Test that disposing payload disposes tracked stubs."""
        stub_hook = PayloadStubHook(RpcPayload.owned("data"))
        stub = RpcStub(stub_hook)

        payload = RpcPayload.from_app_params({"stub": stub})
        payload.ensure_deep_copied()

        assert len(payload.stubs) == 1

        payload.dispose()

        # Stubs should be cleared
        assert len(payload.stubs) == 0
        assert len(payload.promises) == 0

    def test_dispose_payload_with_promises(self):
        """Test that disposing payload disposes tracked promises."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        hook = session.create_promise_hook(1)
        promise = RpcPromise(hook)

        payload = RpcPayload.from_app_return({"promise": promise})
        payload.ensure_deep_copied()

        assert len(payload.promises) == 1

        payload.dispose()

        # Promises should be cleared
        assert len(payload.stubs) == 0
        assert len(payload.promises) == 0

    def test_dispose_idempotent(self):
        """Test that dispose can be called multiple times."""
        payload = RpcPayload.owned({"data": "test"})

        payload.dispose()
        payload.dispose()
        payload.dispose()

        assert len(payload.stubs) == 0
        assert len(payload.promises) == 0


class TestPayloadRepr:
    """Test payload __repr__ method."""

    def test_repr_owned(self):
        """Test repr for owned payload."""
        payload = RpcPayload.owned({"test": "data"})
        repr_str = repr(payload)

        assert "RpcPayload" in repr_str
        assert "OWNED" in repr_str
        assert "test" in repr_str

    def test_repr_params(self):
        """Test repr for params payload."""
        payload = RpcPayload.from_app_params([1, 2, 3])
        repr_str = repr(payload)

        assert "RpcPayload" in repr_str
        assert "PARAMS" in repr_str
        assert "[1, 2, 3]" in repr_str

    def test_repr_return(self):
        """Test repr for return payload."""
        payload = RpcPayload.from_app_return("value")
        repr_str = repr(payload)

        assert "RpcPayload" in repr_str
        assert "RETURN" in repr_str
        assert "value" in repr_str
