"""MapBuilder for constructing complex map operations.

This module implements the TypeScript MapBuilder pattern for building
map instructions that can be sent to the server for remote execution.

The MapBuilder intercepts calls made within a map callback and converts
them into a series of instructions that can be serialized and sent to
the server. The server then executes these instructions using MapApplicator.

Example:
    ```python
    # Client-side: Build map instructions
    result = await stub.items.map(lambda x: x.process().get_value())
    
    # This generates instructions like:
    # [
    #   ["pipeline", 0, ["process"]],      # Call process() on input
    #   ["pipeline", 1, ["get_value"]],    # Call get_value() on result
    #   ["import", 2]                       # Return the final result
    # ]
    ```
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Callable, Protocol

from capnweb.error import RpcError
from capnweb.hooks import PayloadStubHook, StubHook
from capnweb.payload import RpcPayload


# Type alias for map instructions
MapInstruction = list[Any]


class Exporter(Protocol):
    """Protocol for exporting capabilities during map building."""
    
    def export_capability(self, stub: Any) -> int:
        """Export a capability and return its ID."""
        ...
    
    def get_import(self, hook: StubHook) -> int | None:
        """Get the import ID for a hook if it's already captured."""
        ...


# Global current map builder for intercepting calls
_current_map_builder: MapBuilder | None = None


@dataclass
class MapContext:
    """Context for map building - tracks captures and subject."""
    parent: MapBuilder | None
    captures: list[StubHook | int]  # StubHook for root, int for nested
    subject: StubHook | int  # StubHook for root, int for nested
    path: list[str | int]


class MapVariableHook(StubHook):
    """A hook representing a variable in a map function.
    
    This is a placeholder that tracks which instruction result it refers to.
    It cannot be pulled or used outside the map context.
    """
    
    def __init__(self, mapper: MapBuilder, idx: int) -> None:
        self.mapper = mapper
        self.idx = idx
    
    def dup(self) -> MapVariableHook:
        """Variables can be freely shared within a map."""
        return self
    
    def dispose(self) -> None:
        """Nothing to dispose for variables."""
        pass
    
    def get(self, path: list[str | int]) -> StubHook:
        """Get a property - generates a pipeline instruction."""
        if not path:
            return self
        
        global _current_map_builder
        if _current_map_builder:
            return _current_map_builder.push_get(self, path)
        
        raise RpcError.internal(
            "Attempted to use an abstract placeholder from a mapper function. "
            "Please make sure your map function has no side effects."
        )
    
    async def call(self, path: list[str | int], args: RpcPayload) -> StubHook:
        """Call should be intercepted by MapBuilder."""
        raise RpcError.internal(
            "Attempted to use an abstract placeholder from a mapper function. "
            "Please make sure your map function has no side effects."
        )
    
    def map(
        self,
        path: list[str | int],
        captures: list[StubHook],
        instructions: list[Any],
    ) -> StubHook:
        """Nested map - should be intercepted."""
        raise RpcError.internal(
            "Attempted to use an abstract placeholder from a mapper function. "
            "Please make sure your map function has no side effects."
        )
    
    async def pull(self) -> RpcPayload:
        """Map variables cannot be pulled - map functions cannot be async."""
        raise RpcError.internal("RPC map() callbacks cannot be async.")
    
    def ignore_unhandled_rejections(self) -> None:
        """Nothing to do."""
        pass
    
    def on_broken(self, callback: Any) -> None:
        """Variables don't track broken callbacks."""
        raise RpcError.internal(
            "Attempted to use an abstract placeholder from a mapper function."
        )


class MapBuilder:
    """Builder for constructing map instructions.
    
    The MapBuilder intercepts all RPC calls made within a map callback
    and converts them into serializable instructions.
    
    Usage:
        ```python
        builder = MapBuilder(subject_hook, path)
        try:
            input_var = builder.make_input()
            # ... execute map callback with input_var ...
            result_hook = builder.make_output(result_payload)
        finally:
            builder.unregister()
        ```
    """
    
    def __init__(self, subject: StubHook, path: list[str | int]) -> None:
        """Initialize the map builder.
        
        Args:
            subject: The hook to map over
            path: Property path to the array
        """
        global _current_map_builder
        
        if _current_map_builder:
            # Nested map - capture subject from parent
            self.context = MapContext(
                parent=_current_map_builder,
                captures=[],
                subject=_current_map_builder.capture(subject),
                path=path,
            )
        else:
            # Root map
            self.context = MapContext(
                parent=None,
                captures=[],
                subject=subject,
                path=path,
            )
        
        self._capture_map: dict[int, int] = {}  # hook id -> capture index
        self._instructions: list[MapInstruction] = []
        
        # Register as current builder
        _current_map_builder = self
    
    def unregister(self) -> None:
        """Unregister this builder from the global context."""
        global _current_map_builder
        _current_map_builder = self.context.parent
    
    def make_input(self) -> MapVariableHook:
        """Create a hook representing the map input (index 0)."""
        return MapVariableHook(self, 0)
    
    def make_output(self, result: RpcPayload) -> StubHook:
        """Finalize the map and return the result hook.
        
        Args:
            result: The result payload from the map callback
            
        Returns:
            A hook representing the mapped result
        """
        from capnweb.serializer import Serializer
        
        # Serialize the result as the final instruction
        serializer = Serializer(exporter=self)
        serialized = serializer.serialize_value(result.value)
        result.dispose()
        
        # Add the final instruction (the return value)
        self._instructions.append(serialized)
        
        if self.context.parent:
            # Nested map - add remap instruction to parent
            captures_wire = [["import", cap] for cap in self.context.captures]
            self.context.parent._instructions.append([
                "remap",
                self.context.subject,
                self.context.path,
                captures_wire,
                self._instructions,
            ])
            return MapVariableHook(
                self.context.parent,
                len(self.context.parent._instructions),
            )
        else:
            # Root map - call map on the subject
            return self.context.subject.map(
                self.context.path,
                self.context.captures,  # type: ignore
                self._instructions,
            )
    
    def push_call(
        self,
        hook: StubHook,
        path: list[str | int],
        params: RpcPayload,
    ) -> StubHook:
        """Push a call instruction and return a variable for the result.
        
        Args:
            hook: The hook to call on
            path: Property path + method name
            params: Arguments
            
        Returns:
            A MapVariableHook for the result
        """
        from capnweb.serializer import Serializer
        
        # Serialize arguments
        serializer = Serializer(exporter=self)
        serialized_args = serializer.serialize_value(params.value)
        
        # Capture the subject
        subject_idx = self.capture(hook.dup())
        
        # Add pipeline instruction with args
        self._instructions.append(["pipeline", subject_idx, path, serialized_args])
        
        return MapVariableHook(self, len(self._instructions))
    
    def push_get(self, hook: StubHook, path: list[str | int]) -> StubHook:
        """Push a property get instruction and return a variable for the result.
        
        Args:
            hook: The hook to get from
            path: Property path
            
        Returns:
            A MapVariableHook for the result
        """
        subject_idx = self.capture(hook.dup())
        
        # Add pipeline instruction without args (property get)
        self._instructions.append(["pipeline", subject_idx, path])
        
        return MapVariableHook(self, len(self._instructions))
    
    def capture(self, hook: StubHook) -> int:
        """Capture a hook and return its index.
        
        Captured hooks are external references used in the map function.
        Index 0 is the input, positive indices are instruction results,
        negative indices are captures.
        
        Args:
            hook: The hook to capture
            
        Returns:
            The capture index (negative for captures, positive for variables)
        """
        # Check if it's already one of our variables
        if isinstance(hook, MapVariableHook) and hook.mapper is self:
            return hook.idx
        
        # Check if already captured
        hook_id = id(hook)
        if hook_id in self._capture_map:
            return self._capture_map[hook_id]
        
        # Add new capture
        if self.context.parent:
            # Nested - capture from parent
            parent_idx = self.context.parent.capture(hook)
            self.context.captures.append(parent_idx)
        else:
            # Root - store the hook directly
            self.context.captures.append(hook)
        
        # Captures use negative indices: -1, -2, -3, ...
        result = -len(self.context.captures)
        self._capture_map[hook_id] = result
        return result
    
    # Exporter protocol implementation
    
    def export_capability(self, stub: Any) -> int:
        """Export a capability - not allowed in map functions."""
        raise RpcError.internal(
            "Can't construct an RpcTarget or RPC callback inside a mapper function. "
            "Try creating a new RpcStub outside the callback first, then using it "
            "inside the callback."
        )
    
    def export_promise(self, stub: Any) -> int:
        """Export a promise - not allowed in map functions."""
        return self.export_capability(stub)
    
    def get_import(self, hook: StubHook) -> int | None:
        """Get the import ID for a hook if captured."""
        return self.capture(hook)
    
    def unexport(self, ids: list[int]) -> None:
        """Unexport - no-op for map builder."""
        pass
    
    def on_send_error(self, error: Exception) -> Exception | None:
        """Handle send error - not used in map builder."""
        return None


def build_map(
    subject: StubHook,
    path: list[str | int],
    func: Callable[[Any], Any],
) -> StubHook:
    """Build a map operation from a callback function.
    
    This is the main entry point for creating map operations.
    
    Args:
        subject: The hook to map over
        path: Property path to the array
        func: The map callback function
        
    Returns:
        A hook representing the mapped result
        
    Raises:
        RpcError: If the callback is async or uses placeholders incorrectly
    """
    from capnweb.stubs import RpcPromise
    
    builder = MapBuilder(subject, path)
    try:
        # Create input placeholder
        input_hook = builder.make_input()
        input_promise = RpcPromise(input_hook)
        
        # Execute the callback with call interception
        # TODO: Implement proper call interception like TypeScript
        result = func(input_promise)
        
        # Check for async - not allowed
        if asyncio.iscoroutine(result):
            # Cancel the coroutine to avoid warnings
            result.close()
            raise RpcError.internal("RPC map() callbacks cannot be async.")
        
        # Convert result to payload
        result_payload = RpcPayload.from_app_return(result)
        
        return builder.make_output(result_payload)
    finally:
        builder.unregister()


# Call interceptor for map operations
def with_call_interceptor(
    interceptor: Callable[[StubHook, list[str | int], RpcPayload], StubHook],
    callback: Callable[[], Any],
) -> Any:
    """Execute a callback with a call interceptor.
    
    This allows intercepting all RPC calls made within the callback.
    Used by MapBuilder to capture calls as instructions.
    
    Args:
        interceptor: Function to intercept calls
        callback: The callback to execute
        
    Returns:
        The result of the callback
    """
    # TODO: Implement proper call interception
    # For now, just execute the callback directly
    return callback()
