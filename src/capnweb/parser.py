"""Parser (Evaluator) for converting wire format to Python objects.

This module implements the Evaluator from TypeScript capnweb. It takes
JSON-serializable wire expressions and, with the help of an Importer
(the RpcSession), converts them to Python objects wrapped in RpcPayload.

NOTE: This module now uses CapabilityCodec internally for value decoding.
The Parser class remains for backwards compatibility and additional features
like remap handling.

Key responsibilities:
- Decode wire format using CapabilityCodec
- Handle remap expressions (not handled by CapabilityCodec)
- Create RpcStub/RpcPromise for capability expressions
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Final, Protocol

from capnweb.capability_codec import (
    CapabilityCodec,
    CapabilityCodecOptions,
    DANGEROUS_KEYS,
    MAX_RESOLVE_DEPTH as MAX_PARSE_DEPTH,
)
from capnweb.error import ErrorCode, RpcError
from capnweb.hooks import ErrorStubHook
from capnweb.payload import RpcPayload
from capnweb.stubs import RpcPromise, RpcStub
from capnweb.value_codec import ValueCodecOptions
from capnweb.wire import WireError, WireExport, is_int_not_bool as _is_int_not_bool
from capnweb.wire import WirePromise as WirePromiseType

if TYPE_CHECKING:
    from capnweb.hooks import StubHook


class Importer(Protocol):
    """Protocol for objects that can import capabilities.

    This is typically implemented by RpcSession (Client/Server).
    """

    def import_capability(self, import_id: int) -> StubHook:
        """Import a capability and return its hook.

        Args:
            import_id: The import ID for this capability

        Returns:
            A StubHook representing the imported capability
        """
        ...

    def create_promise_hook(self, promise_id: int) -> StubHook:
        """Create a promise hook for a future value.

        Args:
            promise_id: The promise ID

        Returns:
            A PromiseStubHook that will resolve when the promise settles
        """
        ...

    def get_export(self, export_id: int) -> StubHook | None:
        """Get an export by ID (for remap - sender passing our object back).

        When we receive ["import", id] or ["remap", id, ...], the id refers to
        our export table (from the sender's perspective it's their import).

        Args:
            export_id: The export ID to look up

        Returns:
            The StubHook for this export, or None if not found
        """
        ...


class Parser:
    """Converts wire format to Python objects for RPC reception.

    This class (called Evaluator in TypeScript) is responsible for:
    1. Taking JSON-serializable wire expressions
    2. Finding ["export", id] and creating RpcStub with RpcImportHook
    3. Finding ["promise", id] and creating RpcPromise with PromiseStubHook
    4. Finding ["error", ...] and creating ErrorStubHook
    5. Returning the final result as RpcPayload.owned()

    The key difference from the old evaluator: this is a pure, stateless
    transformation. All state management happens in the RpcSession (Importer).
    """

    def __init__(self, importer: Importer) -> None:
        """Initialize with an importer.

        Args:
            importer: The RpcSession that manages import IDs
        """
        self.importer = importer

    def parse(self, wire_value: Any) -> RpcPayload:
        """Parse a wire expression into a Python value wrapped in RpcPayload.

        This is the main entry point. It recursively walks the wire structure
        and converts it to Python objects, creating stubs/promises as needed.

        Args:
            wire_value: The wire expression to parse (JSON-serializable)

        Returns:
            An RpcPayload.owned() containing the parsed value
            
        Raises:
            ValueError: If max depth exceeded or malformed expression
        """
        parsed = self._parse_value(wire_value, depth=0)
        return RpcPayload.owned(parsed)

    def _parse_value(self, value: Any, *, depth: int) -> Any:  # noqa: C901
        """Parse a single wire value recursively.
        
        This implements the same algorithm as TypeScript's Evaluator.evaluateImpl().
        
        Key insight: All application arrays are ESCAPED on the wire by wrapping them
        in an outer single-element array. So [[1,2,3]] on wire becomes [1,2,3] in app.
        Unescaped arrays starting with a string tag are special forms.

        Args:
            value: The wire value to parse
            depth: Current recursion depth (for security limiting)

        Returns:
            The parsed Python value
            
        Raises:
            ValueError: If max depth exceeded or malformed special form
        """
        # Security: Prevent stack overflow from deeply nested malicious payloads
        if depth > MAX_PARSE_DEPTH:
            raise ValueError(
                f"Expression exceeds maximum depth ({MAX_PARSE_DEPTH}). "
                "Possible malicious payload or circular reference."
            )
        
        # Handle None and primitives - pass through unchanged
        if value is None or isinstance(value, (bool, int, float, str)):
            return value

        # Handle arrays - could be escaped arrays or special forms
        if isinstance(value, list):
            # Check for escaped array: [[...]]
            # If array has exactly one element and that element is also an array,
            # it's an escaped literal array - unwrap and parse contents
            if len(value) == 1 and isinstance(value[0], list):
                inner = value[0]
                return [self._parse_value(item, depth=depth + 1) for item in inner]
            
            # Check for special forms (arrays starting with a string tag)
            if value and isinstance(value[0], str):
                tag = value[0]
                
                # Handle each special form with proper type validation
                # This matches TypeScript's switch statement with break on validation failure
                
                if tag == "bigint":
                    if len(value) == 2 and isinstance(value[1], str):
                        # Python handles big integers natively
                        return int(value[1])
                    # Fall through to error
                
                elif tag == "date":
                    if len(value) == 2 and isinstance(value[1], (int, float)):
                        # Return as timestamp - could convert to datetime if needed
                        from datetime import datetime, timezone
                        return datetime.fromtimestamp(value[1] / 1000, tz=timezone.utc)
                    # Fall through to error
                
                elif tag == "bytes":
                    if len(value) == 2 and isinstance(value[1], str):
                        import base64
                        # Handle base64 with or without padding
                        b64 = value[1]
                        padding = 4 - len(b64) % 4
                        if padding != 4:
                            b64 += "=" * padding
                        return base64.b64decode(b64)
                    # Fall through to error
                
                elif tag == "error":
                    if len(value) >= 3 and isinstance(value[1], str) and isinstance(value[2], str):
                        return self._parse_error(value)
                    # Fall through to error
                
                elif tag == "undefined":
                    if len(value) == 1:
                        return None  # Python's None is closest to undefined
                    # Fall through to error
                
                elif tag == "inf":
                    return float("inf")
                
                elif tag == "-inf":
                    return float("-inf")
                
                elif tag == "nan":
                    return float("nan")
                
                elif tag in ("import", "pipeline"):
                    # These reference OUR exports (sender is passing our object back)
                    if len(value) >= 2 and len(value) <= 4 and _is_int_not_bool(value[1]):
                        # For now, treat as error - session handles these in push messages
                        error = RpcError.bad_request(
                            f"{tag} expressions should not appear in parse input"
                        )
                        return RpcStub(ErrorStubHook(error))
                    # Fall through to error
                
                elif tag == "remap":
                    # ["remap", importId, propertyPath, captures, instructions]
                    # propertyPath can be null or list
                    if (len(value) == 5 and _is_int_not_bool(value[1]) and
                        (value[2] is None or isinstance(value[2], list)) and
                        isinstance(value[3], list) and isinstance(value[4], list)):
                        return self._parse_remap(value)
                    # Malformed remap - fall through to regular array handling
                
                elif tag in ("export", "promise"):
                    # These are capabilities being sent TO us
                    if len(value) == 2 and _is_int_not_bool(value[1]):
                        if tag == "promise":
                            return self._parse_promise(value)
                        else:
                            return self._parse_export(value)
                    # Malformed - fall through to regular array handling
                
                # Unknown tag or malformed special form
                # Treat as regular array (could be application data that starts with a string)
                # This is more lenient than TypeScript which throws, but safer for Python
                pass
            
            # Array starting with string but not a valid special form
            # Treat as regular array
            return [self._parse_value(item, depth=depth + 1) for item in value]

        # Handle dicts - parse each value with dangerous key filtering
        # (matches TypeScript's filtering of Object.prototype keys)
        if isinstance(value, dict):
            result = {}
            for key, val in value.items():
                if key in DANGEROUS_KEYS:
                    # Security: Skip dangerous keys that could cause mischief
                    # Still parse the value to properly release any stubs
                    self._parse_value(val, depth=depth + 1)
                    continue
                result[key] = self._parse_value(val, depth=depth + 1)
            return result

        # For other types, return as-is
        return value

    def _parse_export(self, wire_expr: list[Any]) -> Any:
        """Parse an export expression.

        When we receive ["export", id], it means the remote side is exporting
        a capability to us. We create an import for it.

        Args:
            wire_expr: ["export", export_id]

        Returns:
            An RpcStub wrapping an RpcImportHook
        """

        wire_export = WireExport.from_json(wire_expr)
        export_id = wire_export.export_id

        # The export ID becomes our import ID
        # (we're importing what they're exporting)
        import_hook = self.importer.import_capability(export_id)
        return RpcStub(import_hook)

    def _parse_import(self, wire_expr: list[Any]) -> Any:
        """Parse an import expression.

        When we receive ["import", id], it means the remote side is referencing
        a capability we exported to them. This shouldn't normally appear in
        parse input - it's typically used in serialization.

        Args:
            wire_expr: ["import", import_id]

        Returns:
            An error stub (imports shouldn't appear in received data)
        """

        error = RpcError.bad_request(
            "Import expressions should not appear in parse input"
        )
        return RpcStub(ErrorStubHook(error))

    def _parse_promise(self, wire_expr: list[Any]) -> Any:
        """Parse a promise expression.

        When we receive ["promise", id], it means the remote side is sending
        us a promise that will resolve later.

        Args:
            wire_expr: ["promise", promise_id]

        Returns:
            An RpcPromise wrapping a PromiseStubHook
        """

        wire_promise = WirePromiseType.from_json(wire_expr)
        promise_id = wire_promise.promise_id

        # Create a promise hook that will resolve when the promise settles
        promise_hook = self.importer.create_promise_hook(promise_id)
        return RpcPromise(promise_hook)

    def _parse_error(self, wire_expr: list[Any]) -> Any:
        """Parse an error expression.

        When we receive ["error", type, message, ...], we create an ErrorStubHook
        containing the RpcError.

        Args:
            wire_expr: ["error", type, message, stack?, data?]

        Returns:
            An RpcStub wrapping an ErrorStubHook
        """

        wire_error = WireError.from_json(wire_expr)

        # Convert string error type to ErrorCode enum
        try:
            error_code = ErrorCode(wire_error.error_type)
        except ValueError:
            # If unknown error type, default to internal
            error_code = ErrorCode.INTERNAL

        # Create RpcError from wire error
        error = RpcError(
            code=error_code,
            message=wire_error.message,
            data=wire_error.data,
        )

        # Wrap in ErrorStubHook and return as stub
        return RpcStub(ErrorStubHook(error))

    def _parse_remap(self, wire_expr: list[Any]) -> Any:
        """Parse a remap expression.

        Remap implements the .map() operation, allowing a function to be applied
        to array elements remotely without transferring all data back and forth.

        Format: ["remap", importId, propertyPath, captures, instructions]

        - importId: References our export table (sender passing our object back)
        - propertyPath: Path to the property to map over (can be null)
        - captures: Stubs captured by the mapper function
        - instructions: JSON instructions describing the mapper

        Args:
            wire_expr: ["remap", importId, propertyPath, captures, instructions]

        Returns:
            An RpcPromise wrapping the mapped result
        """
        import_id = wire_expr[1]
        property_path = wire_expr[2] or []  # Convert null to empty list
        captures_wire = wire_expr[3]
        instructions = wire_expr[4]

        # Get the target from our export table
        hook = self.importer.get_export(import_id)
        if hook is None:
            error = RpcError.bad_request(
                f"No such entry on exports table: {import_id}"
            )
            return RpcStub(ErrorStubHook(error))

        # Validate property path - must be strings or numbers
        if not all(isinstance(p, (str, int)) for p in property_path):
            error = RpcError.bad_request("Invalid property path in remap")
            return RpcStub(ErrorStubHook(error))

        # Process captures
        # Each capture is ["import", id] or ["export", id]
        # "export" means sender is exporting a new stub to us (we import it)
        # "import" means sender is passing back our export
        captures: list = []
        for cap in captures_wire:
            if (not isinstance(cap, list) or len(cap) != 2 or
                cap[0] not in ("import", "export") or
                not isinstance(cap[1], int)):
                error = RpcError.bad_request(
                    f"Invalid map capture: {cap}"
                )
                return RpcStub(ErrorStubHook(error))

            if cap[0] == "export":
                # Sender is exporting a new stub to us - import it
                cap_hook = self.importer.import_capability(cap[1])
                captures.append(cap_hook)
            else:  # "import"
                # Sender is passing back our export
                exp = self.importer.get_export(cap[1])
                if exp is None:
                    error = RpcError.bad_request(
                        f"No such entry on exports table: {cap[1]}"
                    )
                    return RpcStub(ErrorStubHook(error))
                captures.append(exp.dup())

        # Apply the map operation
        result_hook = hook.map(property_path, captures, instructions)

        # Return as promise (map results are always promises)
        return RpcPromise(result_hook)

    def parse_payload_value(self, wire_value: Any) -> RpcPayload:
        """Parse a wire value and return as owned payload.

        This is a convenience method that ensures the result is wrapped
        in an owned RpcPayload.

        Args:
            wire_value: The wire value to parse

        Returns:
            An RpcPayload.owned() containing the parsed value
        """
        return self.parse(wire_value)
