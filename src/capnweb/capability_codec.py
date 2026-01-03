"""CapabilityCodec - Value encoding with capability table integration.

Composes ValueCodec (stateless array-escape rules) with capability table
management (stateful export/import tracking).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Final, Protocol

from capnweb.error import RpcError
from capnweb.hooks import ErrorStubHook
from capnweb.payload import RpcPayload
from capnweb.stubs import RpcPromise, RpcStub

from capnweb.value_codec import (
    BigIntValue,
    BytesValue,
    DateValue,
    ErrorValue,
    ExportRef,
    ImportRef,
    PromiseRef,
    UndefinedValue,
    ValueCodec,
    ValueCodecOptions,
)

# Security: Maximum recursion depth for capability resolution
MAX_RESOLVE_DEPTH: Final[int] = 64

# Security: Dangerous keys that could lead to surprising behavior in some consumers
DANGEROUS_KEYS: Final[frozenset[str]] = frozenset({
    "__proto__", "__class__", "__dict__", "__slots__",
    "__getattr__", "__setattr__", "__delattr__",
    "__reduce__", "__reduce_ex__", "__getstate__", "__setstate__",
})


class Importer(Protocol):
    """Resolves inbound refs to local hook objects."""

    def import_exported_capability(self, export_id: int):
        """Peer sent us ["export", id] meaning 'capability you can call'."""
        ...

    def create_promise_hook(self, promise_id: int):
        """Peer sent us ["promise", id] meaning 'promise to a capability/value'."""
        ...

    def get_export(self, export_id: int):
        """Optional: remap support (peer sends back something we exported earlier)."""
        ...


class Exporter(Protocol):
    """Converts outbound stubs/promises to ids for the wire."""

    def export_stub(self, stub: RpcStub) -> int:
        ...

    def export_promise(self, promise: RpcPromise) -> int:
        ...


@dataclass(slots=True)
class CapabilityCodecOptions:
    value_codec_options: ValueCodecOptions = ValueCodecOptions(
        # Strongly recommended for Cap'n Web correctness:
        # arrays must be [[...]] for literal arrays, and ["tag", ...] for escapes.
        strict_escape_arrays=True
    )
    filter_dangerous_keys: bool = True

    # How to handle ErrorValue when decoding:
    # - "return": return RpcError object inside decoded structure
    # - "raise": raise RpcError immediately if encountered
    error_mode: str = "return"  # "return" | "raise"


DEFAULT_OPTIONS: Final[CapabilityCodecOptions] = CapabilityCodecOptions()


class CapabilityCodec:
    __slots__ = ("opts", "value_codec", "importer", "exporter")

    def __init__(
        self,
        opts: CapabilityCodecOptions = DEFAULT_OPTIONS,
        *,
        importer: Importer | None = None,
        exporter: Exporter | None = None,
    ) -> None:
        self.opts = opts
        self.value_codec = ValueCodec(opts.value_codec_options)
        self.importer = importer
        self.exporter = exporter

    # ==========================================================================
    # Encoding (Python -> Wire JSON)
    # ==========================================================================

    def encode(self, value: Any) -> Any:
        prepared = self._prepare_for_encode(value, depth=0)
        return self.value_codec.encode(prepared)

    def encode_payload(self, payload: RpcPayload) -> Any:
        payload.ensure_deep_copied()
        return self.encode(payload.value)

    def _prepare_for_encode(self, value: Any, *, depth: int) -> Any:
        if depth > MAX_RESOLVE_DEPTH:
            raise ValueError(f"Max encode depth exceeded ({MAX_RESOLVE_DEPTH})")

        if value is None or isinstance(value, (bool, int, float, str)):
            return value

        if isinstance(value, RpcError):
            # IMPORTANT: return ErrorValue, not a raw ["error", ...] list
            return ErrorValue(
                error_type=value.code.value,
                message=value.message,
                stack=None,
                data=value.data,
            )

        if isinstance(value, RpcStub):
            if self.exporter is None:
                raise RuntimeError("Cannot encode RpcStub without exporter")
            export_id = self.exporter.export_stub(value)
            return ExportRef(export_id)

        if isinstance(value, RpcPromise):
            if self.exporter is None:
                raise RuntimeError("Cannot encode RpcPromise without exporter")
            promise_id = self.exporter.export_promise(value)
            return PromiseRef(promise_id)

        if isinstance(value, datetime):
            # Avoid local-time surprises for naive datetimes
            if value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)
            ms = value.timestamp() * 1000.0
            return DateValue(ms)

        if isinstance(value, bytes):
            import base64
            return BytesValue(base64.b64encode(value).decode("ascii"))

        if isinstance(value, list):
            return [self._prepare_for_encode(v, depth=depth + 1) for v in value]

        if isinstance(value, dict):
            for k in value.keys():
                if not isinstance(k, str):
                    raise TypeError("JSON object keys must be strings")
            return {k: self._prepare_for_encode(v, depth=depth + 1) for k, v in value.items()}

        if isinstance(value, RpcPayload):
            value.ensure_deep_copied()
            return self._prepare_for_encode(value.value, depth=depth + 1)

        raise TypeError(f"Cannot encode type: {type(value).__name__}")

    # ==========================================================================
    # Decoding (Wire JSON -> Python)
    # ==========================================================================

    def decode(self, wire_value: Any) -> Any:
        decoded = self.value_codec.decode(wire_value)
        return self._resolve(decoded, depth=0)

    def decode_to_payload(self, wire_value: Any) -> RpcPayload:
        return RpcPayload.owned(self.decode(wire_value))

    def _resolve(self, value: Any, *, depth: int) -> Any:
        if depth > MAX_RESOLVE_DEPTH:
            raise ValueError(f"Max decode depth exceeded ({MAX_RESOLVE_DEPTH})")

        if value is None or isinstance(value, (bool, int, float, str)):
            return value

        # IMPORTANT: if ValueCodec preserved an unknown tagged array as raw list,
        # keep it opaque (do not traverse/mutate its internals).
        if isinstance(value, list) and value and isinstance(value[0], str):
            return value

        if isinstance(value, ErrorValue):
            err = RpcError.from_wire(
                code=value.error_type,
                message=value.message,
                data=value.data,
                stack=value.stack,
            )
            if self.opts.error_mode == "raise":
                raise err
            return err

        if isinstance(value, ExportRef):
            if self.importer is None:
                raise RuntimeError("Cannot decode ExportRef without importer")
            hook = self.importer.import_exported_capability(value.id)
            return RpcStub(hook)

        # ImportRef should generally not appear in user payloads;
        # treat it as an error stub so callers can't accidentally use it.
        if isinstance(value, ImportRef):
            err = RpcError.bad_request("ImportRef should not appear in received values")
            return RpcStub(ErrorStubHook(err))

        if isinstance(value, PromiseRef):
            if self.importer is None:
                raise RuntimeError("Cannot decode PromiseRef without importer")
            hook = self.importer.create_promise_hook(value.id)
            return RpcPromise(hook)

        if isinstance(value, DateValue):
            return datetime.fromtimestamp(value.ms / 1000.0, tz=timezone.utc)

        if isinstance(value, BigIntValue):
            return int(value.text)

        if isinstance(value, BytesValue):
            import base64
            b64 = value.b64
            # normalize padding
            pad = (-len(b64)) % 4
            if pad:
                b64 += "=" * pad
            return base64.b64decode(b64)

        if isinstance(value, UndefinedValue):
            return None

        if isinstance(value, list):
            return [self._resolve(v, depth=depth + 1) for v in value]

        if isinstance(value, dict):
            out: dict[str, Any] = {}
            for k, v in value.items():
                if self.opts.filter_dangerous_keys and k in DANGEROUS_KEYS:
                    # Drop key; do not traverse value to avoid side-effects.
                    continue
                out[k] = self._resolve(v, depth=depth + 1)
            return out

        # Unknown types pass through
        return value


def create_capability_codec(
    *,
    importer: Importer | None = None,
    exporter: Exporter | None = None,
    opts: CapabilityCodecOptions = DEFAULT_OPTIONS,
) -> CapabilityCodec:
    return CapabilityCodec(opts, importer=importer, exporter=exporter)
