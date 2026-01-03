"""Tests for Pydantic configuration models.

These tests verify:
1. URL validation for ClientConfig and BatchRpcConfig
2. Timeout validation (must be positive)
3. Port validation for WebSocketServerConfig
4. Backwards compatibility aliases
5. Model serialization/deserialization
"""

import pytest
from pydantic import ValidationError

from capnweb.config import (
    BatchRpcConfig,
    ClientConfig,
    RpcSessionConfig,
    WebSocketServerConfig,
)
# Test backwards compatibility aliases
from capnweb import (
    UnifiedClientConfig,
    RpcSessionOptions,
)


class TestRpcSessionConfig:
    """Tests for RpcSessionConfig."""

    def test_default_values(self) -> None:
        """Test default configuration."""
        config = RpcSessionConfig()
        assert config.on_send_error is None

    def test_with_callback(self) -> None:
        """Test with error callback."""
        def redact_error(e: Exception) -> Exception:
            return Exception("redacted")
        
        config = RpcSessionConfig(on_send_error=redact_error)
        assert config.on_send_error is not None
        result = config.on_send_error(ValueError("secret"))
        assert str(result) == "redacted"

    def test_backwards_compat_alias(self) -> None:
        """Test RpcSessionOptions is same as RpcSessionConfig."""
        assert RpcSessionOptions is RpcSessionConfig


class TestClientConfig:
    """Tests for ClientConfig (unified client configuration)."""

    def test_valid_websocket_url(self) -> None:
        """Test valid WebSocket URLs."""
        config = ClientConfig(url="ws://localhost:8080/rpc")
        assert config.url == "ws://localhost:8080/rpc"
        assert config.timeout == 30.0  # default
        
        config_secure = ClientConfig(url="wss://example.com/rpc")
        assert config_secure.url == "wss://example.com/rpc"

    def test_valid_http_url(self) -> None:
        """Test valid HTTP URLs."""
        config = ClientConfig(url="http://localhost:8080/batch")
        assert config.url == "http://localhost:8080/batch"
        
        config_secure = ClientConfig(url="https://api.example.com/rpc")
        assert config_secure.url == "https://api.example.com/rpc"

    def test_invalid_url_scheme(self) -> None:
        """Test invalid URL schemes are rejected."""
        with pytest.raises(ValidationError) as exc_info:
            ClientConfig(url="ftp://example.com")
        assert "URL must start with" in str(exc_info.value)

    def test_empty_url_rejected(self) -> None:
        """Test empty URL is rejected."""
        with pytest.raises(ValidationError) as exc_info:
            ClientConfig(url="")
        assert "URL cannot be empty" in str(exc_info.value)

    def test_invalid_url_no_scheme(self) -> None:
        """Test URL without scheme is rejected."""
        with pytest.raises(ValidationError) as exc_info:
            ClientConfig(url="localhost:8080/rpc")
        assert "URL must start with" in str(exc_info.value)

    def test_custom_timeout(self) -> None:
        """Test custom timeout."""
        config = ClientConfig(url="ws://localhost:8080", timeout=60.0)
        assert config.timeout == 60.0

    def test_invalid_timeout_zero(self) -> None:
        """Test timeout must be positive."""
        with pytest.raises(ValidationError) as exc_info:
            ClientConfig(url="ws://localhost:8080", timeout=0)
        assert "greater than 0" in str(exc_info.value)

    def test_invalid_timeout_negative(self) -> None:
        """Test negative timeout is rejected."""
        with pytest.raises(ValidationError) as exc_info:
            ClientConfig(url="ws://localhost:8080", timeout=-1.0)
        assert "greater than 0" in str(exc_info.value)

    def test_with_local_main(self) -> None:
        """Test with local_main capability."""
        class MyCapability:
            pass
        
        cap = MyCapability()
        config = ClientConfig(url="ws://localhost:8080", local_main=cap)
        assert config.local_main is cap

    def test_with_session_options(self) -> None:
        """Test with nested session options."""
        session_config = RpcSessionConfig()
        config = ClientConfig(
            url="ws://localhost:8080",
            options=session_config
        )
        assert config.options is session_config

    def test_backwards_compat_alias(self) -> None:
        """Test UnifiedClientConfig is same as ClientConfig."""
        assert UnifiedClientConfig is ClientConfig


class TestWebSocketServerConfig:
    """Tests for WebSocketServerConfig."""

    def test_default_values(self) -> None:
        """Test default server configuration."""
        config = WebSocketServerConfig()
        assert config.host == "0.0.0.0"
        assert config.port == 8080
        assert config.path == "/rpc"
        assert config.local_main_factory is None

    def test_custom_values(self) -> None:
        """Test custom server configuration."""
        config = WebSocketServerConfig(
            host="127.0.0.1",
            port=9000,
            path="/api/rpc"
        )
        assert config.host == "127.0.0.1"
        assert config.port == 9000
        assert config.path == "/api/rpc"

    def test_invalid_port_zero(self) -> None:
        """Test port must be positive."""
        with pytest.raises(ValidationError) as exc_info:
            WebSocketServerConfig(port=0)
        assert "greater than 0" in str(exc_info.value)

    def test_invalid_port_negative(self) -> None:
        """Test negative port is rejected."""
        with pytest.raises(ValidationError) as exc_info:
            WebSocketServerConfig(port=-1)
        assert "greater than 0" in str(exc_info.value)

    def test_invalid_port_too_high(self) -> None:
        """Test port above 65535 is rejected."""
        with pytest.raises(ValidationError) as exc_info:
            WebSocketServerConfig(port=70000)
        assert "less than or equal to 65535" in str(exc_info.value)

    def test_with_factory(self) -> None:
        """Test with local_main_factory."""
        def create_capability():
            return {"test": True}
        
        config = WebSocketServerConfig(local_main_factory=create_capability)
        assert config.local_main_factory is create_capability
        assert config.local_main_factory() == {"test": True}


class TestBatchRpcConfig:
    """Tests for BatchRpcConfig (HTTP batch RPC configuration)."""

    def test_valid_http_url(self) -> None:
        """Test valid HTTP URLs for batch RPC."""
        config = BatchRpcConfig(url="http://localhost:8080/batch")
        assert config.url == "http://localhost:8080/batch"
        
        config_secure = BatchRpcConfig(url="https://api.example.com/rpc")
        assert config_secure.url == "https://api.example.com/rpc"

    def test_invalid_websocket_url(self) -> None:
        """Test WebSocket URLs are rejected for batch RPC."""
        with pytest.raises(ValidationError) as exc_info:
            BatchRpcConfig(url="ws://localhost:8080")
        assert "Batch RPC URL must start with" in str(exc_info.value)

    def test_invalid_wss_url(self) -> None:
        """Test secure WebSocket URLs are rejected."""
        with pytest.raises(ValidationError) as exc_info:
            BatchRpcConfig(url="wss://localhost:8080")
        assert "Batch RPC URL must start with" in str(exc_info.value)

    def test_default_timeout(self) -> None:
        """Test default timeout."""
        config = BatchRpcConfig(url="http://localhost:8080")
        assert config.timeout == 30.0

    def test_custom_timeout(self) -> None:
        """Test custom timeout."""
        config = BatchRpcConfig(url="http://localhost:8080", timeout=120.0)
        assert config.timeout == 120.0


class TestConfigSerialization:
    """Tests for config serialization/deserialization."""

    def test_client_config_to_dict(self) -> None:
        """Test ClientConfig can be converted to dict."""
        config = ClientConfig(
            url="ws://localhost:8080",
            timeout=45.0
        )
        data = config.model_dump()
        assert data["url"] == "ws://localhost:8080"
        assert data["timeout"] == 45.0
        assert data["local_main"] is None
        assert data["options"] is None

    def test_client_config_from_dict(self) -> None:
        """Test ClientConfig can be created from dict."""
        data = {
            "url": "wss://example.com/rpc",
            "timeout": 60.0
        }
        config = ClientConfig(**data)
        assert config.url == "wss://example.com/rpc"
        assert config.timeout == 60.0

    def test_server_config_to_json(self) -> None:
        """Test WebSocketServerConfig JSON serialization."""
        config = WebSocketServerConfig(port=9000)
        json_str = config.model_dump_json()
        assert "9000" in json_str
        assert "0.0.0.0" in json_str
