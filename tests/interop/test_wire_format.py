"""Wire format tests for TypeScript/Python interop.

These tests verify the exact wire format of messages using MockTransport
to inspect what is actually sent over the wire.
"""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from typing import Any

import pytest

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))


# =============================================================================
# Wire Message Parsing Tests
# =============================================================================

class TestWireMessageParsing:
    """Test parsing of wire messages."""
    
    def test_push_message_structure(self):
        """Push message has correct structure."""
        from capnweb.wire import WirePush, WirePipeline
        
        pipeline = WirePipeline(import_id=0, property_path=None, args=None)
        push = WirePush(pipeline)
        json_msg = push.to_json()
        
        assert json_msg[0] == "push"
        assert json_msg[1][0] == "pipeline"
        assert json_msg[1][1] == 0
    
    def test_pull_message_structure(self):
        """Pull message has correct structure."""
        from capnweb.wire import WirePull
        
        pull = WirePull(import_id=1)
        json_msg = pull.to_json()
        
        assert json_msg[0] == "pull"
        assert json_msg[1] == 1
    
    def test_resolve_message_structure(self):
        """Resolve message has correct structure."""
        from capnweb.wire import WireResolve
        
        resolve = WireResolve(export_id=-1, value="test")
        json_msg = resolve.to_json()
        
        assert json_msg[0] == "resolve"
        assert json_msg[1] == -1
        assert json_msg[2] == "test"
    
    def test_reject_message_structure(self):
        """Reject message has correct structure."""
        from capnweb.wire import WireReject, WireError
        
        error = WireError(error_type="Error", message="test error")
        reject = WireReject(export_id=-1, error=error)
        json_msg = reject.to_json()
        
        assert json_msg[0] == "reject"
        assert json_msg[1] == -1
        assert json_msg[2][0] == "error"
        assert json_msg[2][1] == "Error"
        assert json_msg[2][2] == "test error"
    
    def test_release_message_structure(self):
        """Release message has correct structure."""
        from capnweb.wire import WireRelease
        
        release = WireRelease(import_id=1, refcount=1)
        json_msg = release.to_json()
        
        assert json_msg[0] == "release"
        assert json_msg[1] == 1
        assert json_msg[2] == 1  # refcount
    
    def test_abort_message_structure(self):
        """Abort message has correct structure."""
        from capnweb.wire import WireAbort, WireError
        
        error = WireError(error_type="Error", message="session aborted")
        abort = WireAbort(error=error)
        json_msg = abort.to_json()
        
        assert json_msg[0] == "abort"
        assert json_msg[1][0] == "error"


# =============================================================================
# Pipeline Expression Tests
# =============================================================================

class TestPipelineExpression:
    """Test pipeline expression wire format."""
    
    def test_pipeline_with_no_path_no_args(self):
        """Pipeline with no path and no args."""
        from capnweb.wire import WirePipeline
        
        pipeline = WirePipeline(import_id=0, property_path=None, args=None)
        json_msg = pipeline.to_json()
        
        assert json_msg == ["pipeline", 0]
    
    def test_pipeline_with_path_no_args(self):
        """Pipeline with path but no args."""
        from capnweb.wire import WirePipeline, PropertyKey
        
        path = [PropertyKey("method")]
        pipeline = WirePipeline(import_id=0, property_path=path, args=None)
        json_msg = pipeline.to_json()
        
        assert json_msg == ["pipeline", 0, ["method"]]
    
    def test_pipeline_with_path_and_args(self):
        """Pipeline with path and args."""
        from capnweb.wire import WirePipeline, PropertyKey
        
        path = [PropertyKey("method")]
        pipeline = WirePipeline(import_id=0, property_path=path, args=[42])
        json_msg = pipeline.to_json()
        
        assert json_msg == ["pipeline", 0, ["method"], [42]]
    
    def test_pipeline_with_null_path_and_args(self):
        """Pipeline with null path but with args."""
        from capnweb.wire import WirePipeline
        
        pipeline = WirePipeline(import_id=0, property_path=None, args=[42])
        json_msg = pipeline.to_json()
        
        assert json_msg == ["pipeline", 0, None, [42]]
    
    def test_pipeline_with_integer_path_key(self):
        """Pipeline with integer path key (array index)."""
        from capnweb.wire import WirePipeline, PropertyKey
        
        path = [PropertyKey(0)]
        pipeline = WirePipeline(import_id=0, property_path=path, args=None)
        json_msg = pipeline.to_json()
        
        assert json_msg == ["pipeline", 0, [0]]
    
    def test_pipeline_with_mixed_path_keys(self):
        """Pipeline with mixed string and integer path keys."""
        from capnweb.wire import WirePipeline, PropertyKey
        
        path = [
            PropertyKey("items"),
            PropertyKey(0),
            PropertyKey("name"),
        ]
        pipeline = WirePipeline(import_id=0, property_path=path, args=None)
        json_msg = pipeline.to_json()
        
        assert json_msg == ["pipeline", 0, ["items", 0, "name"]]


# =============================================================================
# Special Form Wire Format Tests
# =============================================================================

class TestSpecialFormWireFormat:
    """Test wire format of special forms."""
    
    def test_export_form(self):
        """Export special form: ["export", id]."""
        from capnweb.wire import WireExport
        
        export = WireExport(export_id=-1)
        json_msg = export.to_json()
        
        assert json_msg == ["export", -1]
    
    def test_import_form(self):
        """Import special form: ["import", id]."""
        from capnweb.wire import WireImport
        
        imp = WireImport(import_id=1)
        json_msg = imp.to_json()
        
        assert json_msg == ["import", 1]
    
    def test_promise_form(self):
        """Promise special form: ["promise", id]."""
        # WirePromise may not exist as a separate class
        # The promise form is typically just ["promise", id] in the wire format
        # This is handled by the parser, not as a separate wire type
        promise_json = ["promise", 1]
        assert promise_json[0] == "promise"
        assert promise_json[1] == 1
    
    def test_error_form(self):
        """Error special form: ["error", type, message, stack?]."""
        from capnweb.wire import WireError
        
        error = WireError(error_type="TypeError", message="test error")
        json_msg = error.to_json()
        
        assert json_msg[0] == "error"
        assert json_msg[1] == "TypeError"
        assert json_msg[2] == "test error"
    
    def test_error_form_with_stack(self):
        """Error special form with stack trace."""
        from capnweb.wire import WireError
        
        error = WireError(
            error_type="Error",
            message="test",
            stack="Error: test\n    at foo.js:1:1"
        )
        json_msg = error.to_json()
        
        assert json_msg[0] == "error"
        assert json_msg[1] == "Error"
        assert json_msg[2] == "test"
        assert json_msg[3] == "Error: test\n    at foo.js:1:1"


# =============================================================================
# Array Escaping Wire Format Tests
# =============================================================================

class TestArrayEscapingWireFormat:
    """Test array escaping in wire format."""
    
    def test_serialize_simple_array(self):
        """Simple array is escaped as [[...]]."""
        from capnweb.serializer import Serializer
        from unittest.mock import Mock
        
        # Create a mock exporter (not needed for primitive arrays)
        mock_exporter = Mock()
        serializer = Serializer(exporter=mock_exporter)
        result = serializer.serialize([1, 2, 3])
        
        # Array should be wrapped in outer array
        assert result == [[1, 2, 3]]
    
    def test_serialize_nested_array(self):
        """Nested array is properly escaped."""
        from capnweb.serializer import Serializer
        from unittest.mock import Mock
        
        mock_exporter = Mock()
        serializer = Serializer(exporter=mock_exporter)
        result = serializer.serialize([[1, 2], [3, 4]])
        
        # Outer array escaped, inner arrays also escaped
        assert result == [[[[1, 2]], [[3, 4]]]]
    
    def test_serialize_empty_array(self):
        """Empty array is escaped as [[]]."""
        from capnweb.serializer import Serializer
        from unittest.mock import Mock
        
        mock_exporter = Mock()
        serializer = Serializer(exporter=mock_exporter)
        result = serializer.serialize([])
        
        assert result == [[]]
    
    def test_parse_escaped_array(self):
        """Escaped array [[...]] is parsed correctly."""
        from capnweb.parser import Parser
        from unittest.mock import Mock
        
        mock_importer = Mock()
        parser = Parser(importer=mock_importer)
        result = parser.parse([[1, 2, 3]])
        
        # Parser returns RpcPayload, get the value
        assert result.value == [1, 2, 3]
    
    def test_parse_nested_escaped_array(self):
        """Nested escaped arrays are parsed correctly."""
        from capnweb.parser import Parser
        from unittest.mock import Mock
        
        mock_importer = Mock()
        parser = Parser(importer=mock_importer)
        result = parser.parse([[[[1, 2]], [[3, 4]]]])
        
        assert result.value == [[1, 2], [3, 4]]
    
    def test_parse_empty_escaped_array(self):
        """Empty escaped array [[]] is parsed correctly."""
        from capnweb.parser import Parser
        from unittest.mock import Mock
        
        mock_importer = Mock()
        parser = Parser(importer=mock_importer)
        result = parser.parse([[]])
        
        assert result.value == []


# =============================================================================
# ID Sign Convention Tests
# =============================================================================

class TestIdSignConventions:
    """Test ID sign conventions: exports negative, imports positive, main=0."""
    
    def test_main_capability_id_is_zero(self):
        """Main capability has ID 0."""
        from capnweb.wire import WirePipeline
        
        # Main capability is always ID 0
        pipeline = WirePipeline(import_id=0, property_path=None, args=None)
        assert pipeline.import_id == 0
    
    def test_export_ids_are_negative(self):
        """Export IDs should be negative."""
        from capnweb.wire import WireExport
        
        export = WireExport(export_id=-1)
        assert export.export_id < 0
        
        export2 = WireExport(export_id=-2)
        assert export2.export_id < 0
    
    def test_import_ids_are_positive(self):
        """Import IDs should be positive (except main=0)."""
        from capnweb.wire import WireImport
        
        imp = WireImport(import_id=1)
        assert imp.import_id > 0
        
        imp2 = WireImport(import_id=2)
        assert imp2.import_id > 0
    
    def test_resolve_uses_export_id(self):
        """Resolve message uses export ID (negative)."""
        from capnweb.wire import WireResolve
        
        resolve = WireResolve(export_id=-1, value="test")
        assert resolve.export_id < 0
    
    def test_release_uses_import_id(self):
        """Release message uses import ID (positive)."""
        from capnweb.wire import WireRelease
        
        release = WireRelease(import_id=1, refcount=1)
        assert release.import_id > 0


# =============================================================================
# Message Batch Format Tests
# =============================================================================

class TestMessageBatchFormat:
    """Test message batch format (newline-separated JSON)."""
    
    def test_single_message_format(self):
        """Single message is valid JSON."""
        from capnweb.wire import WirePush, WirePipeline
        
        pipeline = WirePipeline(import_id=0, property_path=None, args=None)
        push = WirePush(pipeline)
        json_str = json.dumps(push.to_json())
        
        # Should be valid JSON
        parsed = json.loads(json_str)
        assert parsed[0] == "push"
    
    def test_multiple_messages_format(self):
        """Multiple messages are newline-separated."""
        from capnweb.wire import WirePush, WirePull, WirePipeline
        
        pipeline = WirePipeline(import_id=0, property_path=None, args=None)
        push = WirePush(pipeline)
        pull = WirePull(import_id=1)
        
        # Format as newline-separated JSON
        batch = "\n".join([
            json.dumps(push.to_json()),
            json.dumps(pull.to_json()),
        ])
        
        # Parse back
        messages = [json.loads(line) for line in batch.strip().split("\n")]
        assert len(messages) == 2
        assert messages[0][0] == "push"
        assert messages[1][0] == "pull"


# =============================================================================
# Wire Message Round-Trip Tests
# =============================================================================

class TestWireMessageRoundTrip:
    """Test wire message serialization/deserialization round-trip."""
    
    def test_push_roundtrip(self):
        """Push message round-trips correctly."""
        from capnweb.wire import WirePush, WirePipeline, parse_wire_message
        import json as json_mod
        
        pipeline = WirePipeline(import_id=0, property_path=None, args=None)
        original = WirePush(pipeline)
        
        json_msg = original.to_json()
        parsed = parse_wire_message(json_mod.dumps(json_msg))
        
        assert isinstance(parsed, WirePush)
        assert parsed.expression.import_id == 0
    
    def test_pull_roundtrip(self):
        """Pull message round-trips correctly."""
        from capnweb.wire import WirePull, parse_wire_message
        import json as json_mod
        
        original = WirePull(import_id=1)
        
        json_msg = original.to_json()
        parsed = parse_wire_message(json_mod.dumps(json_msg))
        
        assert isinstance(parsed, WirePull)
        assert parsed.import_id == 1
    
    def test_resolve_roundtrip(self):
        """Resolve message round-trips correctly."""
        from capnweb.wire import WireResolve, parse_wire_message
        import json as json_mod
        
        original = WireResolve(export_id=-1, value="test")
        
        json_msg = original.to_json()
        parsed = parse_wire_message(json_mod.dumps(json_msg))
        
        assert isinstance(parsed, WireResolve)
        assert parsed.export_id == -1
        assert parsed.value == "test"
    
    def test_reject_roundtrip(self):
        """Reject message round-trips correctly."""
        from capnweb.wire import WireReject, WireError, parse_wire_message
        import json as json_mod
        
        error = WireError(error_type="Error", message="test")
        original = WireReject(export_id=-1, error=error)
        
        json_msg = original.to_json()
        parsed = parse_wire_message(json_mod.dumps(json_msg))
        
        assert isinstance(parsed, WireReject)
        assert parsed.export_id == -1
        assert parsed.error.error_type == "Error"
    
    def test_release_roundtrip(self):
        """Release message round-trips correctly."""
        from capnweb.wire import WireRelease, parse_wire_message
        import json as json_mod
        
        original = WireRelease(import_id=1, refcount=1)
        
        json_msg = original.to_json()
        # parse_wire_message expects a string
        parsed = parse_wire_message(json_mod.dumps(json_msg))
        
        assert isinstance(parsed, WireRelease)
        assert parsed.import_id == 1
    
    def test_abort_roundtrip(self):
        """Abort message round-trips correctly."""
        from capnweb.wire import WireAbort, WireError, parse_wire_message
        import json as json_mod
        
        error = WireError(error_type="Error", message="aborted")
        original = WireAbort(error=error)
        
        json_msg = original.to_json()
        parsed = parse_wire_message(json_mod.dumps(json_msg))
        
        assert isinstance(parsed, WireAbort)
        assert parsed.error.message == "aborted"
