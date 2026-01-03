"""Pydantic configuration models for Cap'n Web.

This module contains Pydantic models for user-facing configuration classes.
These are NOT used in hot paths (wire parsing, message handling) to maintain
performance - only for startup/initialization configuration.

Wire format classes remain as @dataclass(frozen=True, slots=True) for performance.
"""

from __future__ import annotations

from typing import Any, Callable

from pydantic import BaseModel, ConfigDict, Field, field_validator


class RpcSessionConfig(BaseModel):
    """Configuration options for RPC sessions.
    
    This replaces the manual RpcSessionOptions class with Pydantic validation.
    
    Attributes:
        on_send_error: Optional callback to transform errors before sending.
            Useful for redacting sensitive information from stack traces.
    """
    
    model_config = ConfigDict(
        frozen=False,  # Allow mutation for callback assignment
        arbitrary_types_allowed=True,  # Allow Callable types
    )
    
    on_send_error: Callable[[Exception], Exception | None] | None = None


class ClientConfig(BaseModel):
    """Configuration for the unified RPC client.
    
    This replaces UnifiedClientConfig with Pydantic validation.
    
    Attributes:
        url: The RPC endpoint URL (ws://, wss://, http://, https://)
        timeout: Request timeout in seconds (must be positive)
        local_main: Optional capability to expose to the server
        session_options: Optional session configuration
    """
    
    model_config = ConfigDict(
        frozen=False,
        arbitrary_types_allowed=True,
    )
    
    url: str = Field(..., description="RPC endpoint URL")
    timeout: float = Field(
        default=30.0,
        gt=0,
        description="Request timeout in seconds"
    )
    local_main: Any | None = Field(
        default=None,
        description="Optional capability to expose to server"
    )
    options: RpcSessionConfig | None = Field(
        default=None,
        description="Optional session configuration"
    )
    
    @field_validator("url")
    @classmethod
    def validate_url(cls, v: str) -> str:
        """Validate URL format."""
        if not v:
            raise ValueError("URL cannot be empty")
        
        valid_schemes = ("ws://", "wss://", "http://", "https://")
        if not any(v.startswith(scheme) for scheme in valid_schemes):
            raise ValueError(
                f"URL must start with one of: {', '.join(valid_schemes)}"
            )
        return v


class WebSocketServerConfig(BaseModel):
    """Configuration for WebSocket RPC server.
    
    Attributes:
        host: Host to bind to
        port: Port to bind to
        path: WebSocket endpoint path
        local_main_factory: Factory function to create per-connection capability
        session_options: Optional session configuration
    """
    
    model_config = ConfigDict(
        frozen=False,
        arbitrary_types_allowed=True,
    )
    
    host: str = Field(default="0.0.0.0", description="Host to bind to")
    port: int = Field(default=8080, gt=0, le=65535, description="Port to bind to")
    path: str = Field(default="/rpc", description="WebSocket endpoint path")
    local_main_factory: Callable[[], Any] | None = Field(
        default=None,
        description="Factory to create per-connection capability"
    )
    options: RpcSessionConfig | None = None


class BatchRpcConfig(BaseModel):
    """Configuration for HTTP batch RPC.
    
    Attributes:
        url: The batch RPC endpoint URL
        timeout: Request timeout in seconds
        local_main: Optional capability to expose to server
        session_options: Optional session configuration
    """
    
    model_config = ConfigDict(
        frozen=False,
        arbitrary_types_allowed=True,
    )
    
    url: str = Field(..., description="Batch RPC endpoint URL")
    timeout: float = Field(default=30.0, gt=0, description="Request timeout")
    local_main: Any | None = None
    options: RpcSessionConfig | None = None
    
    @field_validator("url")
    @classmethod
    def validate_url(cls, v: str) -> str:
        """Validate URL format for batch RPC (HTTP only)."""
        if not v:
            raise ValueError("URL cannot be empty")
        
        valid_schemes = ("http://", "https://")
        if not any(v.startswith(scheme) for scheme in valid_schemes):
            raise ValueError(
                f"Batch RPC URL must start with one of: {', '.join(valid_schemes)}"
            )
        return v


# Backwards compatibility aliases
UnifiedClientConfig = ClientConfig
RpcSessionOptions = RpcSessionConfig
