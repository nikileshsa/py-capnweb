from __future__ import annotations

import copy
import logging
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from capnweb.hooks import StubHook
    from capnweb.stubs import RpcPromise, RpcStub
    from capnweb.types import RpcTarget

logger = logging.getLogger(__name__)


class PayloadSource(Enum):
    """Represents the provenance of payload data.

    This tells us where the data came from and how we can safely use it:
    - PARAMS: From application as call parameters. Must be deep-copied before use.
    - RETURN: From application as return value. We take ownership.
    - OWNED: Deserialized or already copied. We own it and can modify safely.
    """

    PARAMS = auto()  # From app as call parameters. Must be copied.
    RETURN = auto()  # From app as a return value. We take ownership.
    OWNED = auto()  # Deserialized or copied. We own it.


@dataclass
class RpcPayload:
    """Wraps data with explicit ownership semantics for RPC transmission.

    This class is central to preventing data corruption bugs. It explicitly
    tracks where data came from and ensures we never accidentally mutate
    application data or share mutable state across RPC boundaries.

    Key responsibilities:
    1. Track data provenance (PARAMS, RETURN, or OWNED)
    2. Deep-copy application data when needed
    3. Track all RPC stubs and promises within the payload
    4. Provide explicit disposal for resource cleanup

    Example:
        ```python
        # From application parameters - must copy
        payload = RpcPayload.from_app_params({"user": user_dict})

        # Ensure it's safe to use
        payload.ensure_deep_copied()

        # Now we can safely pass it to RPC without worrying about mutations
        await stub.call("method", payload)

        # Clean up resources when done
        payload.dispose()
        ```
    """

    value: Any
    source: PayloadSource
    # These are only populated when source is OWNED (after deep copy)
    # They track all RPC references within this payload for lifecycle management
    stubs: list[RpcStub] = field(
        default_factory=list
    )  # All RpcStub instances found in value
    promises: list[tuple[Any, str | int, RpcPromise]] = field(
        default_factory=list
    )  # (parent, property, promise)
    
    # For source=RETURN payloads, tracks StubHooks created around RpcTargets
    # found in the payload at serialization time. This ensures they aren't
    # disposed before the pipeline ends. Maps RpcTarget/Function -> StubHook.
    # Matches TypeScript's rpcTargets field.
    _rpc_targets: dict[RpcTarget | Callable[..., Any], StubHook] | None = field(
        default=None, repr=False
    )

    @classmethod
    def from_app_params(cls, value: Any) -> RpcPayload:
        """Create a payload from parameters provided by the application.

        This marks the data as PARAMS, meaning it must be deep-copied before
        use to prevent the RPC system from accidentally mutating application state.

        Args:
            value: The parameter value from the application

        Returns:
            A new RpcPayload with source=PARAMS
        """
        return cls(value, PayloadSource.PARAMS)

    @classmethod
    def from_app_return(cls, value: Any) -> RpcPayload:
        """Create a payload from a return value provided by the application.

        This marks the data as RETURN, meaning the application is transferring
        ownership to the RPC system. We can take ownership without copying.

        Args:
            value: The return value from the application

        Returns:
            A new RpcPayload with source=RETURN
        """
        return cls(value, PayloadSource.RETURN)

    @classmethod
    def owned(cls, value: Any) -> RpcPayload:
        """Create a payload that is already owned by the RPC system.

        This is used for deserialized data or data that has been deep-copied.

        Args:
            value: The owned value

        Returns:
            A new RpcPayload with source=OWNED
        """
        return cls(value, PayloadSource.OWNED)

    @classmethod
    def from_array(cls, payloads: list[RpcPayload]) -> RpcPayload:
        """Combine an array of payloads into a single payload.

        Ownership of all stubs is transferred from the inputs to the output.
        If the output is disposed, the inputs should not be.

        Args:
            payloads: List of payloads to combine

        Returns:
            A new RpcPayload containing an array of the payload values
        """
        stubs: list[RpcStub] = []
        promises: list[tuple[Any, str | int, RpcPromise]] = []
        result_array: list[Any] = []

        for payload in payloads:
            payload.ensure_deep_copied()
            stubs.extend(payload.stubs)
            promises.extend(payload.promises)
            result_array.append(payload.value)

        result = cls(result_array, PayloadSource.OWNED)
        result.stubs = stubs
        result.promises = promises
        return result

    @classmethod
    def deep_copy_from(
        cls,
        value: Any,
        old_parent: object | None = None,
        owner: RpcPayload | None = None,
    ) -> RpcPayload:
        """Deep-copy a value, including dup()ing all stubs.

        Args:
            value: The value to copy
            old_parent: Parent object (for RpcTarget handling)
            owner: Owner payload (for RpcTarget handling - used to deduplicate
                   RpcTarget->StubHook mappings across multiple deep copies)

        Returns:
            A new RpcPayload with a deep copy of the value
        """
        result = cls(None, PayloadSource.OWNED)
        result.value = result._deep_copy_value(
            value, old_parent, dup_stubs=True, owner=owner
        )
        return result

    def get_hook_for_rpc_target(
        self,
        target: RpcTarget | Callable[..., Any],
        parent: object | None,
        dup_stubs: bool = True,
    ) -> StubHook:
        """Get or create a StubHook for an RpcTarget found in this payload.
        
        This method handles the complex ownership semantics for RpcTargets:
        - For PARAMS: Creates a new TargetStubHook (or calls target.dup() if available)
        - For RETURN: Deduplicates hooks via _rpc_targets map, handles dup vs take-ownership
        - For OWNED: Should not contain raw RpcTargets (raises error)
        
        Args:
            target: The RpcTarget or callable to wrap
            parent: The parent object containing this target
            dup_stubs: If True, duplicate stubs; if False, take ownership
            
        Returns:
            A StubHook wrapping the target
            
        Raises:
            RuntimeError: If called on an OWNED payload
        """
        from capnweb.hooks import TargetStubHook
        from capnweb.types import RpcTarget
        
        if self.source == PayloadSource.PARAMS:
            if dup_stubs:
                # For params, we're supposed to dup stubs, but RpcTarget isn't a stub.
                # If the RpcTarget has a dup() method, call it (like workerd-native stubs).
                # Otherwise, just wrap it - the caller probably wants us to take ownership.
                if hasattr(target, 'dup') and callable(getattr(target, 'dup')):
                    target = target.dup()  # type: ignore[union-attr]
            return TargetStubHook(target)
            
        elif self.source == PayloadSource.RETURN:
            # For return values, we need to deduplicate RpcTarget->StubHook mappings.
            # This ensures the same RpcTarget always maps to the same hook.
            # Use id(target) as key since RpcTarget objects may be unhashable (e.g., mutable dataclasses)
            if self._rpc_targets is None:
                self._rpc_targets = {}
            
            target_id = id(target)
            hook = self._rpc_targets.get(target_id)
            if hook:
                if dup_stubs:
                    return hook.dup()
                else:
                    # Take ownership - remove from map and return
                    del self._rpc_targets[target_id]
                    return hook
            else:
                hook = TargetStubHook(target)
                if dup_stubs:
                    self._rpc_targets[target_id] = hook
                    return hook.dup()
                else:
                    return hook
        else:
            raise RuntimeError("OWNED payload shouldn't contain raw RpcTargets")

    def ensure_deep_copied(self) -> None:
        """Ensure this payload owns its data through deep copying if needed.

        This is the most critical method for correctness. It:
        1. Deep-copies the value if source is PARAMS (to prevent mutation bugs)
        2. Takes ownership if source is RETURN (no copy needed, but must track refs)
        3. Finds and tracks all RpcStub/RpcPromise instances
        4. Transitions source to OWNED

        After calling this, the payload is safe to use and modify within the
        RPC system without worrying about corrupting application state.
        """
        match self.source:
            case PayloadSource.OWNED:
                # Already owned, nothing to do
                return
            case PayloadSource.PARAMS:
                # Must deep-copy to prevent mutating application data
                # dup_stubs=True means we duplicate any stubs we find
                self.value = self._deep_copy_value(
                    self.value, None, dup_stubs=True, owner=self,
                    parent=self, property_key="value"
                )
            case PayloadSource.RETURN:
                # Application gave us ownership - we take ownership of stubs (no dup)
                # and need to track all references
                self.value = self._deep_copy_value(
                    self.value, None, dup_stubs=False, owner=self,
                    parent=self, property_key="value"
                )

        # Now we own this data
        self.source = PayloadSource.OWNED
        
        # _rpc_targets should be empty after deep copy (all targets accounted for)
        if self._rpc_targets and len(self._rpc_targets) > 0:
            logger.warning("Not all rpcTargets were accounted for in deep-copy")
        self._rpc_targets = None

    def _deep_copy_value(
        self,
        obj: Any,
        old_parent: object | None,
        dup_stubs: bool,
        owner: RpcPayload | None,
        parent: Any = None,
        property_key: str | int | None = None,
    ) -> Any:
        """Deep copy an object while tracking all RPC references.
        
        This matches TypeScript's deepCopy() method in core.ts.

        Args:
            obj: The object to copy
            old_parent: The parent object in the original structure
            dup_stubs: If True, duplicate stubs; if False, take ownership
            owner: The owner payload for RpcTarget deduplication

        Returns:
            A deep copy with all RPC references tracked
        """
        from capnweb.hooks import TargetStubHook
        from capnweb.stubs import RpcPromise, RpcStub
        from capnweb.types import RpcTarget

        # Handle primitives - immutable, no need to copy
        if obj is None or isinstance(obj, (bool, int, float, str, bytes)):
            return obj

        # Handle RpcStub
        if isinstance(obj, RpcStub):
            if dup_stubs:
                hook = obj._hook.dup()
            else:
                # Take ownership - get hook without incrementing refcount
                hook = obj._hook
                # Prevent the original stub from disposing the hook
                obj._hook = None  # type: ignore[assignment]
            new_stub = RpcStub(hook)
            self.stubs.append(new_stub)
            return new_stub

        # Handle RpcPromise
        if isinstance(obj, RpcPromise):
            if dup_stubs:
                hook = obj._hook.dup()
            else:
                hook = obj._hook
                obj._hook = None  # type: ignore[assignment]
            new_promise = RpcPromise(hook)
            # Track promise location for later substitution (matches TypeScript)
            self.promises.append((parent, property_key, new_promise))
            return new_promise

        # Handle lists
        if isinstance(obj, list):
            result: list[Any] = []
            for i, item in enumerate(obj):
                result.append(
                    self._deep_copy_value(item, obj, dup_stubs, owner, result, i)
                )
            return result

        # Handle dicts
        if isinstance(obj, dict):
            result_dict: dict[str, Any] = {}
            for key, value in obj.items():
                result_dict[key] = self._deep_copy_value(value, obj, dup_stubs, owner, result_dict, key)
            return result_dict

        # Handle RpcTarget - wrap in a stub
        if isinstance(obj, RpcTarget):
            if owner:
                hook = owner.get_hook_for_rpc_target(obj, old_parent, dup_stubs)
            else:
                hook = TargetStubHook(obj)
            new_stub = RpcStub(hook)
            self.stubs.append(new_stub)
            return new_stub

        # Handle callable (functions) - wrap in a stub like RpcTarget
        if callable(obj) and not isinstance(obj, type):
            if owner:
                hook = owner.get_hook_for_rpc_target(obj, old_parent, dup_stubs)
            else:
                hook = TargetStubHook(obj)
            new_stub = RpcStub(hook)
            self.stubs.append(new_stub)
            return new_stub

        # For other types, try to copy using copy module
        try:
            return copy.deepcopy(obj)
        except (TypeError, AttributeError, RecursionError) as e:
            logger.debug(f"deepcopy failed for {type(obj).__name__}: {e}")
            return obj

    def _track_references(
        self, obj: Any, parent: Any = None, key: str | int | None = None
    ) -> None:
        """Track all RPC references in an object without copying.

        Args:
            obj: The object to scan
            parent: The parent container (for promise tracking)
            key: The key/index in parent (for promise tracking)
        """
        from capnweb.stubs import RpcPromise, RpcStub

        match obj:
            case RpcStub():
                self.stubs.append(obj)
            case RpcPromise():
                if parent is not None and key is not None:
                    self.promises.append((parent, key, obj))
            case list():
                # Recursively track in lists
                for i, item in enumerate(obj):
                    self._track_references(item, obj, i)
            case dict():
                # Recursively track in dicts
                for k, v in obj.items():
                    self._track_references(v, obj, k)

    def dispose(self) -> None:
        """Recursively dispose all RPC stubs and promises in this payload.

        This ensures proper resource cleanup by calling dispose() on all
        tracked RPC references. After calling this, the payload should not
        be used anymore.

        This is critical for preventing resource leaks, especially with
        remote capabilities that need to send "release" messages.
        """
        # Dispose all tracked stubs
        for stub in self.stubs:
            stub.dispose()

        # Dispose all tracked promises
        for _parent, _key, promise in self.promises:
            promise.dispose()

        # Clear tracking lists
        self.stubs.clear()
        self.promises.clear()

    def __repr__(self) -> str:
        """Return a readable representation for debugging."""
        return f"RpcPayload(source={self.source.name}, value={self.value!r})"
