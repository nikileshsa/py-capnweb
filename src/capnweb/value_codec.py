"""ValueCodec - Explicit array-escape encoding for Cap'n Web.

This module implements the Cap'n Web "arrays are escape sequences" rule:
- JSON arrays are NOT literal arrays; they're escape sequences (e.g. ["date", ...])
- A literal array is encoded by double-wrapping: [[...]]

This makes the encoding rules explicit and keeps wire-message parsing simple.

Reference: https://blog.cloudflare.com/introducing-capnweb-high-performance-rpc-for-modern-web-apps/

Design goals:
1. Single responsibility: converts App values ↔ Wire JSON values
2. No partial parsing: unknown tagged arrays are preserved or rejected (configurable)
3. Safety knobs: depth/size limits, bool-vs-int hardening, allow/deny unknown tags
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Final, Literal, Union

# Type alias for JSON-serializable values
Json = Union[None, bool, int, float, str, list["Json"], dict[str, "Json"]]


# =============================================================================
# Special App-Level Value Wrappers
# =============================================================================


@dataclass(frozen=True, slots=True)
class DateValue:
    """Represents a date/timestamp value.
    
    Wire format: ["date", ms]
    """
    ms: float  # milliseconds since epoch


@dataclass(frozen=True, slots=True)
class UndefinedValue:
    """Represents JavaScript's undefined value.
    
    Wire format: ["undefined"]
    """
    pass


@dataclass(frozen=True, slots=True)
class BigIntValue:
    """Represents an arbitrary-precision integer.
    
    Wire format: ["bigint", "decimal_string"]
    """
    text: str  # decimal string representation


@dataclass(frozen=True, slots=True)
class BytesValue:
    """Represents binary data as base64.
    
    Wire format: ["bytes", "base64_string"]
    """
    b64: str  # base64-encoded string


@dataclass(frozen=True, slots=True)
class NaNValue:
    """Represents IEEE NaN.
    
    Wire format: ["nan"]
    """
    pass


@dataclass(frozen=True, slots=True)
class InfValue:
    """Represents IEEE positive infinity.
    
    Wire format: ["inf"]
    """
    pass


@dataclass(frozen=True, slots=True)
class NegInfValue:
    """Represents IEEE negative infinity.
    
    Wire format: ["-inf"]
    """
    pass


@dataclass(frozen=True, slots=True)
class ErrorValue:
    """Represents an error value.
    
    Wire format: ["error", error_type, message, stack?, data?]
    """
    error_type: str
    message: str
    stack: str | None = None
    data: dict[str, Any] | None = None


# =============================================================================
# Capability Reference Types (markers for higher-layer handling)
# =============================================================================


@dataclass(frozen=True, slots=True)
class ExportRef:
    """Reference to an exported capability.
    
    Wire format: ["export", id]
    """
    id: int


@dataclass(frozen=True, slots=True)
class ImportRef:
    """Reference to an imported capability.
    
    Wire format: ["import", id]
    """
    id: int


@dataclass(frozen=True, slots=True)
class PromiseRef:
    """Reference to a promise.
    
    Wire format: ["promise", id]
    """
    id: int


# Type alias for application values the codec understands
AppValue = Union[
    None, bool, int, float, str,
    list["AppValue"],
    dict[str, "AppValue"],
    DateValue, UndefinedValue, BigIntValue, BytesValue,
    NaNValue, InfValue, NegInfValue,
    ErrorValue,
    ExportRef, ImportRef, PromiseRef,
]


# =============================================================================
# Codec Configuration
# =============================================================================


UnknownTagMode = Literal["preserve", "error"]


@dataclass(frozen=True, slots=True)
class ValueCodecOptions:
    """Configuration options for ValueCodec."""
    
    # Safety limits
    max_depth: int = 200
    max_container_len: int = 100_000
    
    # Decoding behavior for unrecognized ["someTag", ...] arrays
    # "preserve" = keep as raw JSON (forward-compatible)
    # "error" = reject unknown tags
    unknown_tag: UnknownTagMode = "preserve"
    
    # If True (default), reject JSON arrays that are neither:
    #   - [["literal", "array"]] (escaped literal)
    #   - ["knownTag", ...] (recognized escape sequence)
    # Per Cap'n Web spec, bare arrays are INVALID - only [[...]] is a literal array.
    # Note: bare [] is ALWAYS rejected regardless of this setting.
    strict_escape_arrays: bool = True
    
    # Note: bool-as-int rejection for capability IDs is ALWAYS enforced
    # and is not configurable (it's a security-critical check).


# Default options instance
DEFAULT_OPTIONS: Final[ValueCodecOptions] = ValueCodecOptions()


# =============================================================================
# ValueCodec Implementation
# =============================================================================


class ValueCodec:
    """Translates between AppValue and wire JSON using Cap'n Web's array-escape convention.
    
    The core rule: JSON arrays are escape sequences, not literal arrays.
    - Literal arrays are double-wrapped: [1, 2, 3] → [[1, 2, 3]]
    - Special values use tags: Date → ["date", ms], etc.
    
    Example:
        >>> codec = ValueCodec()
        >>> codec.encode([1, 2, 3])
        [[1, 2, 3]]
        >>> codec.decode([[1, 2, 3]])
        [1, 2, 3]
        >>> codec.encode(DateValue(1704067200000))
        ["date", 1704067200000]
    """
    
    __slots__ = ('opts',)
    
    def __init__(self, opts: ValueCodecOptions = DEFAULT_OPTIONS) -> None:
        """Initialize the codec.
        
        Args:
            opts: Configuration options
        
        Note: Capability table integration (export/import resolution) should be
        handled by a higher layer that composes ValueCodec with capability tables.
        """
        self.opts = opts
    
    # ---------- Public API ----------
    
    def encode(self, v: AppValue) -> Json:
        """Encode an application value to wire JSON format.
        
        Args:
            v: Application value to encode
            
        Returns:
            JSON-serializable wire format
            
        Raises:
            TypeError: If value type is not supported
            ValueError: If depth/size limits exceeded
        """
        return self._enc(v, depth=0)
    
    def decode(self, j: Json) -> AppValue:
        """Decode wire JSON format to application value.
        
        Args:
            j: Wire JSON value to decode
            
        Returns:
            Application value
            
        Raises:
            ValueError: If wire format is invalid or limits exceeded
            TypeError: If JSON type is invalid
        """
        return self._dec(j, depth=0)
    
    # ---------- Encoding Internals ----------
    
    def _enc(self, v: AppValue, *, depth: int) -> Json:
        """Internal recursive encoder."""
        self._check_depth(depth)
        
        # Primitives pass through unchanged
        if v is None:
            return None
        if isinstance(v, bool):
            return v
        if isinstance(v, int):
            return v
        if isinstance(v, str):
            return v
        
        # Float: check for special values
        if isinstance(v, float):
            import math
            if math.isnan(v):
                return ["nan"]
            if math.isinf(v):
                return ["inf"] if v > 0 else ["-inf"]
            return v
        
        # Dict: recursively encode values (keys must be strings for valid JSON)
        if isinstance(v, dict):
            self._check_len(len(v))
            for k in v.keys():
                if not isinstance(k, str):
                    raise TypeError(f"JSON object keys must be strings, got {type(k).__name__}")
            return {k: self._enc(val, depth=depth + 1) for k, val in v.items()}
        
        # List: LITERAL ARRAY → [[...]] (double-wrap rule)
        if isinstance(v, list):
            self._check_len(len(v))
            inner = [self._enc(item, depth=depth + 1) for item in v]
            return [inner]  # Double-wrap for literal arrays
        
        # Special value wrappers
        if isinstance(v, DateValue):
            return ["date", v.ms]
        if isinstance(v, UndefinedValue):
            return ["undefined"]
        if isinstance(v, BigIntValue):
            return ["bigint", v.text]
        if isinstance(v, BytesValue):
            return ["bytes", v.b64]
        if isinstance(v, NaNValue):
            return ["nan"]
        if isinstance(v, InfValue):
            return ["inf"]
        if isinstance(v, NegInfValue):
            return ["-inf"]
        
        # Error value - NOT double-wrapped, it's a tagged escape sequence
        if isinstance(v, ErrorValue):
            arr: list[Any] = ["error", v.error_type, v.message]
            if v.stack is not None:
                arr.append(v.stack)
                if v.data is not None:
                    arr.append(v.data)
            elif v.data is not None:
                arr.extend([None, v.data])
            return arr
        
        # Capability references
        if isinstance(v, ExportRef):
            self._validate_int_id(v.id)
            return ["export", v.id]
        if isinstance(v, ImportRef):
            self._validate_int_id(v.id)
            return ["import", v.id]
        if isinstance(v, PromiseRef):
            self._validate_int_id(v.id)
            return ["promise", v.id]
        
        raise TypeError(f"Unsupported AppValue type: {type(v).__name__}")
    
    # ---------- Decoding Internals ----------
    
    def _dec(self, j: Json, *, depth: int) -> AppValue:
        """Internal recursive decoder."""
        self._check_depth(depth)
        
        # Primitives pass through unchanged
        if j is None or isinstance(j, (bool, int, float, str)):
            return j
        
        # Dict: recursively decode values
        if isinstance(j, dict):
            self._check_len(len(j))
            return {k: self._dec(v, depth=depth + 1) for k, v in j.items()}
        
        # Array: this is an escape sequence, NOT a literal array
        if isinstance(j, list):
            self._check_len(len(j))
            
            # Empty array - ALWAYS invalid per Cap'n Web spec
            # The literal empty list is [[]], not []
            if len(j) == 0:
                raise ValueError(
                    "Invalid wire value: bare [] is not allowed; "
                    "use [[]] for empty literal array"
                )
            
            # Literal array form: [[...]] (outer has 1 element which is an array)
            if len(j) == 1 and isinstance(j[0], list):
                inner = j[0]
                self._check_len(len(inner))
                return [self._dec(x, depth=depth + 1) for x in inner]
            
            # Tagged form: ["tag", ...]
            if isinstance(j[0], str):
                return self._decode_tagged(j, depth=depth)
            
            # Any other array shape
            if self.opts.strict_escape_arrays:
                raise ValueError(f"Invalid wire array escape shape: {j!r}")
            
            # Non-strict fallback: decode each element
            return [self._dec(x, depth=depth + 1) for x in j]
        
        raise TypeError(f"Invalid JSON type: {type(j).__name__}")
    
    def _decode_tagged(self, j: list[Any], *, depth: int) -> AppValue:
        """Decode a tagged array like ["date", ms]."""
        tag = j[0]
        
        # Date: ["date", ms]
        if tag == "date" and len(j) == 2 and isinstance(j[1], (int, float)):
            return DateValue(float(j[1]))
        
        # Undefined: ["undefined"]
        if tag == "undefined" and len(j) == 1:
            return UndefinedValue()
        
        # BigInt: ["bigint", "decimal_string"]
        if tag == "bigint" and len(j) == 2 and isinstance(j[1], str):
            return BigIntValue(j[1])
        
        # Bytes: ["bytes", "base64"]
        if tag == "bytes" and len(j) == 2 and isinstance(j[1], str):
            return BytesValue(j[1])
        
        # Special floats - decode to actual float values for round-trip consistency
        if tag == "nan" and len(j) == 1:
            return float("nan")
        if tag == "inf" and len(j) == 1:
            return float("inf")
        if tag == "-inf" and len(j) == 1:
            return float("-inf")
        
        # Error: ["error", type, message, stack?, data?]
        if tag == "error" and len(j) >= 3:
            error_type = j[1]
            message = j[2]
            if not isinstance(error_type, str) or not isinstance(message, str):
                raise ValueError("Error type and message must be strings")
            stack = j[3] if len(j) > 3 else None
            if stack is not None and not isinstance(stack, str):
                raise ValueError("Error stack must be string or null")
            data = j[4] if len(j) > 4 and isinstance(j[4], dict) else None
            return ErrorValue(error_type, message, stack, data)
        
        # Capability references: ["export"|"import"|"promise", id]
        # ALWAYS reject bool for IDs - this is not configurable (security critical)
        if tag in ("export", "import", "promise") and len(j) == 2:
            if isinstance(j[1], bool):
                raise ValueError(f"Invalid {tag} ID: bool not allowed as capability ID")
            if not isinstance(j[1], int):
                raise ValueError(f"Invalid {tag} ID: expected int, got {type(j[1]).__name__}")
            if tag == "export":
                return ExportRef(j[1])
            if tag == "import":
                return ImportRef(j[1])
            return PromiseRef(j[1])
        
        # Unknown tag handling
        if self.opts.unknown_tag == "preserve":
            # Preserve as raw JSON list (do NOT recursively decode)
            # This is the safe default for forward compatibility
            return j  # type: ignore[return-value]
        
        # unknown_tag == "error"
        raise ValueError(f"Unknown wire tag: {tag!r}")
    
    # ---------- Validation Helpers ----------
    
    def _check_depth(self, depth: int) -> None:
        """Check recursion depth limit."""
        if depth > self.opts.max_depth:
            raise ValueError(f"Max decode depth exceeded ({self.opts.max_depth})")
    
    def _check_len(self, n: int) -> None:
        """Check container length limit."""
        if n > self.opts.max_container_len:
            raise ValueError(f"Max container length exceeded ({self.opts.max_container_len})")
    
    def _validate_int_id(self, n: int) -> None:
        """Validate that n is an int, not a bool.
        
        Note: For capability IDs, bool rejection is always enforced
        regardless of any options. This method is for other int validations.
        """
        if isinstance(n, bool):
            raise ValueError("Invalid int ID (bool not allowed)")


# =============================================================================
# Convenience Functions
# =============================================================================


# Global default codec instance
_default_codec: ValueCodec | None = None


def get_default_codec() -> ValueCodec:
    """Get the global default ValueCodec instance."""
    global _default_codec
    if _default_codec is None:
        _default_codec = ValueCodec()
    return _default_codec


def encode_value(v: AppValue) -> Json:
    """Encode an application value using the default codec.
    
    Convenience function equivalent to:
        ValueCodec().encode(v)
    """
    return get_default_codec().encode(v)


def decode_value(j: Json) -> AppValue:
    """Decode a wire JSON value using the default codec.
    
    Convenience function equivalent to:
        ValueCodec().decode(j)
    """
    return get_default_codec().decode(j)
