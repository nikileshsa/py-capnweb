"""Error types for Cap'n Web protocol."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any


class ErrorCode(Enum):
    """Standard RPC error codes."""

    BAD_REQUEST = "bad_request"
    NOT_FOUND = "not_found"
    CAP_REVOKED = "cap_revoked"
    PERMISSION_DENIED = "permission_denied"
    CANCELED = "canceled"
    INTERNAL = "internal"

    def __str__(self) -> str:
        return self.value


@dataclass
class RpcError(Exception):
    """RPC error with code, message, and optional data."""

    code: ErrorCode
    message: str
    data: Any | None = None

    def __str__(self) -> str:
        return f"{self.code}: {self.message}"

    @staticmethod
    def bad_request(message: str, data: Any | None = None) -> RpcError:
        """Create a BAD_REQUEST error."""
        return RpcError(ErrorCode.BAD_REQUEST, message, data)

    @staticmethod
    def not_found(message: str, data: Any | None = None) -> RpcError:
        """Create a NOT_FOUND error."""
        return RpcError(ErrorCode.NOT_FOUND, message, data)

    @staticmethod
    def cap_revoked(message: str, data: Any | None = None) -> RpcError:
        """Create a CAP_REVOKED error."""
        return RpcError(ErrorCode.CAP_REVOKED, message, data)

    @staticmethod
    def permission_denied(message: str, data: Any | None = None) -> RpcError:
        """Create a PERMISSION_DENIED error."""
        return RpcError(ErrorCode.PERMISSION_DENIED, message, data)

    @staticmethod
    def canceled(message: str, data: Any | None = None) -> RpcError:
        """Create a CANCELED error."""
        return RpcError(ErrorCode.CANCELED, message, data)

    @staticmethod
    def internal(message: str, data: Any | None = None) -> RpcError:
        """Create an INTERNAL error."""
        return RpcError(ErrorCode.INTERNAL, message, data)

    @staticmethod
    def from_wire(
        code: str,
        message: str,
        data: Any | None = None,
        stack: str | None = None,
    ) -> RpcError:
        """Create an RpcError from wire format values.
        
        Args:
            code: Error code string (e.g., "bad_request")
            message: Error message
            data: Optional error data
            stack: Optional stack trace (ignored for security)
        """
        try:
            error_code = ErrorCode(code)
        except ValueError:
            error_code = ErrorCode.INTERNAL
        return RpcError(error_code, message, data)
