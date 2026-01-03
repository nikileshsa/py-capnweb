"""Map applicator for executing map operations locally.

This module implements the map() functionality that allows applying a function
to array elements without transferring data back and forth over the network.

The map operation works by:
1. Receiving instructions that describe a mapper function
2. Applying those instructions to each element of an array
3. Returning the mapped results

Instructions format:
- Each instruction is an expression to evaluate
- The last instruction is the return value
- Variables:
  - 0: The input element being mapped
  - Positive values (1-based): Results of previous instructions
  - Negative values (-1-based): Captures (external stubs)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from capnweb.error import RpcError
from capnweb.payload import RpcPayload

if TYPE_CHECKING:
    from capnweb.hooks import StubHook


class MapApplicator:
    """Applies map instructions to an input value.
    
    This class evaluates the instructions for a single element of the array
    being mapped over.
    """
    
    def __init__(self, captures: list["StubHook"], input_hook: "StubHook") -> None:
        """Initialize the applicator.
        
        Args:
            captures: External stubs used in the mapper function
            input_hook: The input element wrapped in a hook
        """
        self.captures = captures
        # Variables: index 0 is input, positive indices are instruction results
        self.variables: list["StubHook"] = [input_hook]
    
    def dispose(self) -> None:
        """Dispose all variables."""
        for var in self.variables:
            var.dispose()
    
    def apply(self, instructions: list[Any]) -> RpcPayload:
        """Apply the instructions and return the result.
        
        Args:
            instructions: List of expressions to evaluate
            
        Returns:
            The result payload
            
        Raises:
            RpcError: If the instructions are invalid
        """
        from capnweb.hooks import PayloadStubHook
        from capnweb.parser import Parser
        
        try:
            if not instructions:
                raise RpcError.bad_request("Invalid empty mapper function")
            
            # Evaluate all instructions except the last (which is the return value)
            for instruction in instructions[:-1]:
                payload = self._evaluate_instruction(instruction)
                
                # The payload usually contains a single stub - unwrap it
                from capnweb.stubs import RpcStub
                if isinstance(payload.value, RpcStub):
                    hook = payload.value._hook
                    self.variables.append(hook)
                else:
                    self.variables.append(PayloadStubHook(payload))
            
            # Evaluate the final instruction (return value)
            return self._evaluate_instruction(instructions[-1])
            
        finally:
            # Dispose all variables
            for var in self.variables:
                var.dispose()
    
    def _evaluate_instruction(self, instruction: Any) -> RpcPayload:
        """Evaluate a single instruction.
        
        Args:
            instruction: The instruction to evaluate
            
        Returns:
            The result payload
        """
        from capnweb.parser import Parser
        
        # Create a parser that uses our variable lookup
        parser = Parser(importer=self)
        return parser.parse(instruction)
    
    def import_capability(self, import_id: int) -> "StubHook":
        """Import a capability by ID (implements Importer protocol).
        
        For map applicator:
        - Negative IDs (-1-based) refer to captures
        - Non-negative IDs refer to variables (0 = input, 1+ = instruction results)
        """
        if import_id < 0:
            # Capture reference (-1 = captures[0], -2 = captures[1], etc.)
            capture_idx = -import_id - 1
            if capture_idx >= len(self.captures):
                raise RpcError.bad_request(f"Invalid capture index: {capture_idx}")
            return self.captures[capture_idx].dup()
        else:
            # Variable reference
            if import_id >= len(self.variables):
                raise RpcError.bad_request(f"Invalid variable index: {import_id}")
            return self.variables[import_id].dup()
    
    def create_promise_hook(self, promise_id: int) -> "StubHook":
        """Create a promise hook (not supported in map)."""
        raise RpcError.bad_request("Promises not supported in map functions")


def apply_map_to_element(
    input_value: Any,
    parent: object | None,
    owner: RpcPayload | None,
    captures: list["StubHook"],
    instructions: list[Any],
) -> RpcPayload:
    """Apply map instructions to a single element.
    
    Args:
        input_value: The element to map over
        parent: Parent object (for RpcTarget handling)
        owner: Owner payload (for RpcTarget handling)
        captures: External stubs used in the mapper
        instructions: The mapper instructions
        
    Returns:
        The mapped result payload
    """
    from capnweb.hooks import PayloadStubHook
    
    # Create a hook for the input
    input_payload = RpcPayload.deep_copy_from(input_value, parent, owner)
    input_hook = PayloadStubHook(input_payload)
    
    # Apply the instructions
    applicator = MapApplicator(captures, input_hook)
    try:
        return applicator.apply(instructions)
    finally:
        applicator.dispose()


def apply_map_locally(
    input_value: Any,
    owner: RpcPayload | None,
    captures: list["StubHook"],
    instructions: list[Any],
) -> "StubHook":
    """Apply a map operation locally.
    
    This is used when the target is a local payload (not remote).
    
    Args:
        input_value: The value to map over (usually an array)
        owner: The owner payload
        captures: External stubs used in the mapper
        instructions: The mapper instructions
        
    Returns:
        A StubHook containing the mapped results
    """
    from capnweb.hooks import PayloadStubHook, ErrorStubHook
    from capnweb.stubs import RpcStub, RpcPromise
    
    try:
        if isinstance(input_value, RpcPromise):
            raise RpcError.bad_request("Cannot map over an unresolved promise")
        
        if isinstance(input_value, list):
            # Map over array elements
            payloads: list[RpcPayload] = []
            try:
                for elem in input_value:
                    payload = apply_map_to_element(
                        elem, input_value, owner, captures, instructions
                    )
                    payloads.append(payload)
            except Exception:
                # Dispose already-created payloads on error
                for p in payloads:
                    p.dispose()
                raise
            
            # Combine into array payload
            result = RpcPayload.from_array(payloads)
        elif input_value is None:
            # null/None passes through
            result = RpcPayload.from_app_return(input_value)
        else:
            # Single value - apply map to it
            result = apply_map_to_element(
                input_value, None, owner, captures, instructions
            )
        
        return PayloadStubHook(result)
        
    finally:
        # Dispose captures
        for cap in captures:
            cap.dispose()
