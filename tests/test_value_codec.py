"""Tests for ValueCodec - the array-escape encoding layer.

These tests verify:
1. Round-trip encode/decode for all value types
2. Array escaping: [[...]] for literal arrays
3. Special value handling (date, bigint, bytes, nan, inf, undefined)
4. Capability references (export, import, promise)
5. Safety limits (depth, container length)
6. Unknown tag handling modes
7. Bool-vs-int validation
"""

import math
import pytest

from capnweb.value_codec import (
    AppValue,
    BigIntValue,
    BytesValue,
    DateValue,
    ExportRef,
    ImportRef,
    InfValue,
    NaNValue,
    NegInfValue,
    PromiseRef,
    UndefinedValue,
    ValueCodec,
    ValueCodecOptions,
    decode_value,
    encode_value,
)


class TestPrimitives:
    """Test primitive value encoding/decoding."""

    def test_none(self) -> None:
        codec = ValueCodec()
        assert codec.encode(None) is None
        assert codec.decode(None) is None

    def test_bool(self) -> None:
        codec = ValueCodec()
        assert codec.encode(True) is True
        assert codec.encode(False) is False
        assert codec.decode(True) is True
        assert codec.decode(False) is False

    def test_int(self) -> None:
        codec = ValueCodec()
        assert codec.encode(42) == 42
        assert codec.encode(-1) == -1
        assert codec.encode(0) == 0
        assert codec.decode(42) == 42

    def test_float(self) -> None:
        codec = ValueCodec()
        assert codec.encode(3.14) == 3.14
        assert codec.decode(3.14) == 3.14

    def test_string(self) -> None:
        codec = ValueCodec()
        assert codec.encode("hello") == "hello"
        assert codec.encode("") == ""
        assert codec.decode("hello") == "hello"


class TestArrayEscaping:
    """Test the core array-escaping rule: literal arrays become [[...]]."""

    def test_empty_list_encoded_as_nested(self) -> None:
        """Empty list becomes [[]]."""
        codec = ValueCodec()
        assert codec.encode([]) == [[]]

    def test_simple_list_encoded_as_nested(self) -> None:
        """[1, 2, 3] becomes [[1, 2, 3]]."""
        codec = ValueCodec()
        assert codec.encode([1, 2, 3]) == [[1, 2, 3]]

    def test_nested_list_encoded(self) -> None:
        """Nested lists are recursively wrapped."""
        codec = ValueCodec()
        # [1, [2, 3]] → [[1, [[2, 3]]]]
        result = codec.encode([1, [2, 3]])
        assert result == [[1, [[2, 3]]]]

    def test_decode_escaped_array(self) -> None:
        """[[1, 2, 3]] decodes to [1, 2, 3]."""
        codec = ValueCodec()
        assert codec.decode([[1, 2, 3]]) == [1, 2, 3]

    def test_decode_empty_escaped_array(self) -> None:
        """[[]] decodes to []."""
        codec = ValueCodec()
        assert codec.decode([[]]) == []

    def test_roundtrip_simple_list(self) -> None:
        """Round-trip: list → encode → decode → list."""
        codec = ValueCodec()
        original = [1, "two", 3.0, None, True]
        encoded = codec.encode(original)
        decoded = codec.decode(encoded)
        assert decoded == original

    def test_roundtrip_nested_list(self) -> None:
        """Round-trip with nested lists."""
        codec = ValueCodec()
        original = [[1, 2], [3, [4, 5]]]
        encoded = codec.encode(original)
        decoded = codec.decode(encoded)
        assert decoded == original


class TestDict:
    """Test dictionary encoding/decoding."""

    def test_empty_dict(self) -> None:
        codec = ValueCodec()
        assert codec.encode({}) == {}
        assert codec.decode({}) == {}

    def test_simple_dict(self) -> None:
        codec = ValueCodec()
        original = {"a": 1, "b": "two"}
        assert codec.encode(original) == original
        assert codec.decode(original) == original

    def test_dict_with_list_values(self) -> None:
        """Dict values that are lists get escaped."""
        codec = ValueCodec()
        original = {"items": [1, 2, 3]}
        encoded = codec.encode(original)
        assert encoded == {"items": [[1, 2, 3]]}
        assert codec.decode(encoded) == original

    def test_roundtrip_nested_dict(self) -> None:
        codec = ValueCodec()
        original = {"outer": {"inner": [1, 2, 3], "value": 42}}
        encoded = codec.encode(original)
        decoded = codec.decode(encoded)
        assert decoded == original


class TestSpecialValues:
    """Test special value types (date, bigint, bytes, undefined, nan, inf)."""

    def test_date_value(self) -> None:
        codec = ValueCodec()
        date = DateValue(1704067200000)
        encoded = codec.encode(date)
        assert encoded == ["date", 1704067200000]
        decoded = codec.decode(encoded)
        assert isinstance(decoded, DateValue)
        assert decoded.ms == 1704067200000

    def test_undefined_value(self) -> None:
        codec = ValueCodec()
        undef = UndefinedValue()
        encoded = codec.encode(undef)
        assert encoded == ["undefined"]
        decoded = codec.decode(encoded)
        assert isinstance(decoded, UndefinedValue)

    def test_bigint_value(self) -> None:
        codec = ValueCodec()
        bigint = BigIntValue("12345678901234567890")
        encoded = codec.encode(bigint)
        assert encoded == ["bigint", "12345678901234567890"]
        decoded = codec.decode(encoded)
        assert isinstance(decoded, BigIntValue)
        assert decoded.text == "12345678901234567890"

    def test_bytes_value(self) -> None:
        codec = ValueCodec()
        b = BytesValue("SGVsbG8gV29ybGQ=")  # "Hello World" in base64
        encoded = codec.encode(b)
        assert encoded == ["bytes", "SGVsbG8gV29ybGQ="]
        decoded = codec.decode(encoded)
        assert isinstance(decoded, BytesValue)
        assert decoded.b64 == "SGVsbG8gV29ybGQ="

    def test_nan_value_wrapper(self) -> None:
        """NaNValue wrapper encodes to ["nan"]."""
        codec = ValueCodec()
        nan = NaNValue()
        encoded = codec.encode(nan)
        assert encoded == ["nan"]

    def test_nan_roundtrip(self) -> None:
        """NaN round-trips correctly (decodes to float)."""
        codec = ValueCodec()
        encoded = codec.encode(float('nan'))
        assert encoded == ["nan"]
        decoded = codec.decode(encoded)
        assert math.isnan(decoded)

    def test_inf_value_wrapper(self) -> None:
        """InfValue wrapper encodes to ["inf"]."""
        codec = ValueCodec()
        inf = InfValue()
        encoded = codec.encode(inf)
        assert encoded == ["inf"]

    def test_inf_roundtrip(self) -> None:
        """Infinity round-trips correctly (decodes to float)."""
        codec = ValueCodec()
        encoded = codec.encode(float('inf'))
        assert encoded == ["inf"]
        decoded = codec.decode(encoded)
        assert decoded == float('inf')

    def test_neg_inf_value_wrapper(self) -> None:
        """NegInfValue wrapper encodes to ["-inf"]."""
        codec = ValueCodec()
        neg_inf = NegInfValue()
        encoded = codec.encode(neg_inf)
        assert encoded == ["-inf"]

    def test_neg_inf_roundtrip(self) -> None:
        """Negative infinity round-trips correctly (decodes to float)."""
        codec = ValueCodec()
        encoded = codec.encode(float('-inf'))
        assert encoded == ["-inf"]
        decoded = codec.decode(encoded)
        assert decoded == float('-inf')


class TestCapabilityRefs:
    """Test capability reference encoding/decoding."""

    def test_export_ref(self) -> None:
        codec = ValueCodec()
        ref = ExportRef(42)
        encoded = codec.encode(ref)
        assert encoded == ["export", 42]
        decoded = codec.decode(encoded)
        assert isinstance(decoded, ExportRef)
        assert decoded.id == 42

    def test_import_ref(self) -> None:
        codec = ValueCodec()
        ref = ImportRef(-1)
        encoded = codec.encode(ref)
        assert encoded == ["import", -1]
        decoded = codec.decode(encoded)
        assert isinstance(decoded, ImportRef)
        assert decoded.id == -1

    def test_promise_ref(self) -> None:
        codec = ValueCodec()
        ref = PromiseRef(0)
        encoded = codec.encode(ref)
        assert encoded == ["promise", 0]
        decoded = codec.decode(encoded)
        assert isinstance(decoded, PromiseRef)
        assert decoded.id == 0

    def test_export_ref_negative_id(self) -> None:
        """Negative export IDs are valid (our exports)."""
        codec = ValueCodec()
        ref = ExportRef(-5)
        encoded = codec.encode(ref)
        assert encoded == ["export", -5]


class TestBoolRejection:
    """Test that bool is ALWAYS rejected for capability IDs (not configurable)."""

    def test_bool_rejected_as_export_id_encode(self) -> None:
        """Bool should not be accepted as export ID on encode."""
        codec = ValueCodec()
        with pytest.raises(ValueError, match="bool not allowed"):
            codec.encode(ExportRef(True))  # type: ignore

    def test_bool_rejected_as_export_id_decode(self) -> None:
        """Bool should not be accepted as ID in wire format."""
        codec = ValueCodec()
        with pytest.raises(ValueError, match="bool not allowed"):
            codec.decode(["export", True])

    def test_bool_rejected_as_import_id(self) -> None:
        """Bool rejected for import IDs too."""
        codec = ValueCodec()
        with pytest.raises(ValueError, match="bool not allowed"):
            codec.decode(["import", False])

    def test_bool_rejected_as_promise_id(self) -> None:
        """Bool rejected for promise IDs too."""
        codec = ValueCodec()
        with pytest.raises(ValueError, match="bool not allowed"):
            codec.decode(["promise", True])


class TestUnknownTags:
    """Test unknown tag handling modes."""

    def test_preserve_unknown_tag(self) -> None:
        """Unknown tags are preserved as-is by default."""
        codec = ValueCodec(ValueCodecOptions(unknown_tag="preserve"))
        wire = ["futureType", "some", "data"]
        decoded = codec.decode(wire)
        assert decoded == wire  # Preserved unchanged, NOT recursively decoded

    def test_error_on_unknown_tag(self) -> None:
        """Unknown tags raise error when configured."""
        codec = ValueCodec(ValueCodecOptions(unknown_tag="error"))
        with pytest.raises(ValueError, match="Unknown wire tag"):
            codec.decode(["futureType", "data"])

    def test_preserve_is_default(self) -> None:
        """Preserve mode is the default for forward compatibility."""
        codec = ValueCodec()
        wire = ["newtype", {"nested": [[1, 2]]}]
        decoded = codec.decode(wire)
        # Should be preserved exactly, no recursive decoding
        assert decoded == wire


class TestSafetyLimits:
    """Test depth and container length limits."""

    def test_max_depth_exceeded(self) -> None:
        """Deep nesting should be rejected."""
        codec = ValueCodec(ValueCodecOptions(max_depth=2))
        # Depth 0: outer list, Depth 1: inner list, Depth 2: innermost
        # This should exceed max_depth=2
        deep = [[[[[[1]]]]]]
        with pytest.raises(ValueError, match="Max.*(depth|decode)"):
            codec.decode(deep)

    def test_max_depth_ok(self) -> None:
        """Nesting within limits should work."""
        codec = ValueCodec(ValueCodecOptions(max_depth=10))
        # [[1]] decodes to [1] (depth 2)
        shallow = [[1]]
        result = codec.decode(shallow)
        assert result == [1]

    def test_max_container_len_exceeded(self) -> None:
        """Large containers should be rejected."""
        codec = ValueCodec(ValueCodecOptions(max_container_len=10))
        large = [[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]]
        with pytest.raises(ValueError, match="Max container length"):
            codec.decode(large)

    def test_max_container_len_ok(self) -> None:
        """Containers within limits should work."""
        codec = ValueCodec(ValueCodecOptions(max_container_len=10))
        small = [[1, 2, 3, 4, 5]]
        result = codec.decode(small)
        assert result == [1, 2, 3, 4, 5]


class TestStrictEscapeArrays:
    """Test strict escape array mode (default)."""

    def test_empty_array_always_rejected(self) -> None:
        """Bare [] is ALWAYS rejected - use [[]] for empty list."""
        codec = ValueCodec()
        with pytest.raises(ValueError, match="bare \\[\\] is not allowed"):
            codec.decode([])

    def test_empty_array_rejected_even_in_non_strict(self) -> None:
        """Bare [] is rejected even with strict_escape_arrays=False."""
        codec = ValueCodec(ValueCodecOptions(strict_escape_arrays=False))
        with pytest.raises(ValueError, match="bare \\[\\] is not allowed"):
            codec.decode([])

    def test_empty_literal_array_works(self) -> None:
        """[[]] correctly decodes to empty list."""
        codec = ValueCodec()
        result = codec.decode([[]])
        assert result == []

    def test_invalid_array_shape_rejected_by_default(self) -> None:
        """Arrays not matching known patterns rejected by default."""
        codec = ValueCodec()  # strict_escape_arrays=True by default
        with pytest.raises(ValueError, match="Invalid wire array"):
            codec.decode([1, 2, 3])  # First element not string, not [[...]]

    def test_invalid_array_shape_allowed_in_non_strict(self) -> None:
        """Non-strict mode allows bare arrays (for debugging)."""
        codec = ValueCodec(ValueCodecOptions(strict_escape_arrays=False))
        result = codec.decode([1, 2, 3])
        assert result == [1, 2, 3]


class TestConvenienceFunctions:
    """Test module-level convenience functions."""

    def test_encode_value(self) -> None:
        result = encode_value([1, 2, 3])
        assert result == [[1, 2, 3]]

    def test_decode_value(self) -> None:
        result = decode_value([[1, 2, 3]])
        assert result == [1, 2, 3]


class TestComplexRoundTrips:
    """Test complex round-trip scenarios."""

    def test_complex_structure(self) -> None:
        """Complex nested structure with multiple value types."""
        codec = ValueCodec()
        original: AppValue = {
            "name": "test",
            "items": [1, 2, 3],
            "metadata": {
                "created": DateValue(1704067200000),
                "tags": ["a", "b"],
            },
            "count": 42,
            "active": True,
        }
        encoded = codec.encode(original)
        decoded = codec.decode(encoded)
        assert decoded == original

    def test_list_with_special_values(self) -> None:
        """List containing special values."""
        codec = ValueCodec()
        original: AppValue = [
            DateValue(1000),
            UndefinedValue(),
            BigIntValue("999"),
            ExportRef(5),
        ]
        encoded = codec.encode(original)
        decoded = codec.decode(encoded)
        assert decoded == original

    def test_dict_with_refs(self) -> None:
        """Dict with capability references."""
        codec = ValueCodec()
        original: AppValue = {
            "callback": ExportRef(-1),
            "target": ImportRef(0),
            "pending": PromiseRef(3),
        }
        encoded = codec.encode(original)
        decoded = codec.decode(encoded)
        assert decoded == original


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_unsupported_type_raises(self) -> None:
        """Unsupported types raise TypeError."""
        codec = ValueCodec()
        with pytest.raises(TypeError, match="Unsupported AppValue"):
            codec.encode(object())  # type: ignore

    def test_invalid_json_type_raises(self) -> None:
        """Invalid JSON types raise TypeError."""
        codec = ValueCodec()
        with pytest.raises(TypeError, match="Invalid JSON type"):
            codec.decode(object())  # type: ignore

    def test_deeply_nested_roundtrip(self) -> None:
        """Deeply nested but valid structure round-trips."""
        codec = ValueCodec()
        original: AppValue = {"a": {"b": {"c": {"d": [1, 2, 3]}}}}
        encoded = codec.encode(original)
        decoded = codec.decode(encoded)
        assert decoded == original

    def test_dict_with_non_string_key_rejected(self) -> None:
        """Dict keys must be strings (JSON requirement)."""
        codec = ValueCodec()
        with pytest.raises(TypeError, match="JSON object keys must be strings"):
            codec.encode({1: "value"})  # type: ignore


class TestConformanceGoldenVectors:
    """Conformance tests with golden vectors for Cap'n Web interop.
    
    These tests verify the exact wire format expected by the TypeScript
    implementation to ensure cross-language interoperability.
    """

    def test_bare_empty_array_must_fail(self) -> None:
        """[] must fail - literal empty list is [[]]."""
        codec = ValueCodec()
        with pytest.raises(ValueError):
            codec.decode([])

    def test_escaped_empty_array(self) -> None:
        """[[]] is the correct encoding for empty list."""
        codec = ValueCodec()
        assert codec.decode([[]]) == []
        assert codec.encode([]) == [[]]

    def test_literal_list_nesting(self) -> None:
        """Verify nesting round-trips correctly.
        
        The key property is that encode/decode are inverses.
        Nested arrays get exponentially wrapped on encode but decode correctly.
        """
        codec = ValueCodec()
        
        # Simple: [[1]] decodes to [1]
        assert codec.decode([[1]]) == [1]
        
        # Round-trip is the important property
        assert codec.decode(codec.encode([1])) == [1]
        assert codec.decode(codec.encode([[1]])) == [[1]]
        assert codec.decode(codec.encode([[[1]]])) == [[[1]]]
        
        # Verify encoding pattern
        assert codec.encode([1]) == [[1]]
        assert codec.encode([[1]]) == [[[[1]]]]  # Nested arrays get double-wrapped recursively

    def test_tagged_values_inside_literal_lists(self) -> None:
        """Tagged values inside literal lists: [[["date", 1000]]]."""
        codec = ValueCodec()
        # Encode a list containing a date
        original = [DateValue(1000)]
        encoded = codec.encode(original)
        # Should be [[["date", 1000]]] - list wrapped, date tagged
        assert encoded == [[["date", 1000]]]
        # Decode back
        decoded = codec.decode(encoded)
        assert decoded == [DateValue(1000)]

    def test_unknown_tags_preserve_mode(self) -> None:
        """Unknown tags in preserve mode."""
        codec = ValueCodec(ValueCodecOptions(unknown_tag="preserve"))
        # Unknown tag preserved exactly
        assert codec.decode(["newtype", 1, 2]) == ["newtype", 1, 2]
        # Unknown tag with nested content NOT recursively decoded
        wire = ["futureType", {"data": [[1, 2, 3]]}]
        assert codec.decode(wire) == wire  # Exact preservation

    def test_unknown_tags_error_mode(self) -> None:
        """Unknown tags in error mode."""
        codec = ValueCodec(ValueCodecOptions(unknown_tag="error"))
        with pytest.raises(ValueError, match="Unknown wire tag"):
            codec.decode(["newtype", 1, 2])

    def test_all_special_values_roundtrip(self) -> None:
        """All special values round-trip correctly."""
        codec = ValueCodec()
        
        # Date
        assert codec.decode(codec.encode(DateValue(1704067200000))) == DateValue(1704067200000)
        
        # Undefined
        assert codec.decode(codec.encode(UndefinedValue())) == UndefinedValue()
        
        # BigInt
        assert codec.decode(codec.encode(BigIntValue("999"))) == BigIntValue("999")
        
        # Bytes
        assert codec.decode(codec.encode(BytesValue("YWJj"))) == BytesValue("YWJj")
        
        # NaN (decodes to float)
        nan_encoded = codec.encode(float("nan"))
        nan_decoded = codec.decode(nan_encoded)
        assert math.isnan(nan_decoded)
        
        # Inf (decodes to float)
        assert codec.decode(codec.encode(float("inf"))) == float("inf")
        assert codec.decode(codec.encode(float("-inf"))) == float("-inf")

    def test_capability_refs_roundtrip(self) -> None:
        """Capability references round-trip correctly."""
        codec = ValueCodec()
        
        assert codec.decode(codec.encode(ExportRef(0))) == ExportRef(0)
        assert codec.decode(codec.encode(ExportRef(-1))) == ExportRef(-1)
        assert codec.decode(codec.encode(ImportRef(5))) == ImportRef(5)
        assert codec.decode(codec.encode(PromiseRef(10))) == PromiseRef(10)

    def test_complex_nested_structure_roundtrip(self) -> None:
        """Complex nested structure with all value types."""
        codec = ValueCodec()
        original: AppValue = {
            "users": [
                {"name": "Alice", "created": DateValue(1000)},
                {"name": "Bob", "created": DateValue(2000)},
            ],
            "count": 2,
            "active": True,
            "metadata": None,
        }
        encoded = codec.encode(original)
        decoded = codec.decode(encoded)
        assert decoded == original
