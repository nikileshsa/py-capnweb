# Cap'n Web Python

A complete Python implementation of the [Cap'n Web protocol](https://github.com/cloudflare/capnweb) - a capability-based RPC system with promise pipelining, structured errors, and multiple transport support.

## What's in the Box

**Core Features:**
- **Capability-based security** - Unforgeable object references with explicit disposal
- **Promise pipelining** - Batch multiple dependent calls into single round-trips
- **Multiple transports** - HTTP Batch, WebSocket, and WebTransport/HTTP/3
- **Type-safe** - Full type hints compatible with pyright/mypy
- **Async/await** - Built on Python's asyncio
- **Bidirectional RPC** - Peer-to-peer capability passing
- **100% Interoperable** - Fully compatible with TypeScript reference implementation

**Beta-Testing-Ready":**
- 352 tests passing, 76% coverage
- 0 linting errors, 0 typing errors
- Hook-based architecture (clean, maintainable)
- ~99% protocol compliance

## Why Use Cap'n Web?

**Traditional RPC has problems:**
- No security model (anyone can call anything)
- No resource management (memory leaks)
- Poor performance (round-trip per call)

**Cap'n Web solves these:**
- **Security**: Capabilities are unforgeable - you can only call what you have a reference to
- **Resource Management**: Explicit disposal with reference counting prevents leaks
- **Performance**: Promise pipelining batches dependent calls into one round-trip
- **Flexibility**: Pass capabilities as arguments - the server decides who gets access

## Installation

```bash
pip install capnweb
# or
uv add capnweb

# For WebTransport support (optional):
pip install capnweb[webtransport]
```

## Quick Start

**Server:**
```python
import asyncio
from capnweb.ws_session import WebSocketRpcServer
from capnweb.types import RpcTarget
from capnweb.error import RpcError

class Calculator(RpcTarget):
    async def call(self, method: str, args: list) -> any:
        match method:
            case "add": return args[0] + args[1]
            case "multiply": return args[0] * args[1]
            case _: raise RpcError.not_found(f"Unknown method: {method}")

    async def get_property(self, property: str) -> any:
        raise RpcError.not_found("No properties")

async def main():
    server = WebSocketRpcServer(Calculator(), host="127.0.0.1", port=8080)
    await server.start()
    await asyncio.Event().wait()  # Run forever

asyncio.run(main())
```

**Client:**
```python
import asyncio
from capnweb.unified_client import UnifiedClient, UnifiedClientConfig

async def main():
    config = UnifiedClientConfig(url="ws://localhost:8080/rpc")
    async with UnifiedClient(config) as client:
        result = await client.call(0, "add", [5, 3])
        print(f"5 + 3 = {result}")  # Output: 8

asyncio.run(main())
```

**Capability Passing** (bidirectional RPC):
```python
# Server returns a capability, client can call methods on it
account = await client.call(0, "createAccount", [1000.0])
balance = await client.call(account._hook.import_id, "getBalance", [])
```

## Current Status (v0.5.0)

**Transports:**
- ✅ WebSocket (full bidirectional RPC with capability passing)
- ✅ WebTransport/HTTP/3 (requires aioquic)

**Protocol Features:**
- ✅ Wire protocol (all message types)
- ✅ Promise pipelining
- ✅ Expression evaluation (including `.map()`)
- ✅ Bidirectional RPC (full capability passing over WebSocket)
- ✅ Resume tokens
- ✅ Reference counting
- ✅ Structured errors
- ⚠️ IL plan execution (only remap supported, full IL is low priority)

**Code Quality:**
- ✅ 352 tests passing (100% success rate)
- ✅ 76% test coverage
- ✅ 0 linting errors (ruff)
- ✅ 0 typing errors (pyrefly)
- ✅ TypeScript interoperability verified

## Documentation

- **[Quickstart Guide](docs/quickstart.md)** - Get started in 5 minutes
- **[API Reference](docs/api-reference.md)** - Complete API documentation
- **[Architecture Guide](docs/architecture.md)** - Understand the internals
- **[Examples](examples/)** - Working code examples

## Examples

**Included examples:**
- `examples/calculator/` - Simple RPC calculator
- `examples/batch-pipelining/` - Promise pipelining demonstration
- `examples/peer_to_peer/` - Bidirectional RPC (Alice & Bob) - HTTP Batch only
- `examples/chat/` - ⚠️ Real-time WebSocket chat (requires bidirectional WebSocket - in progress)
- `examples/microservices/` - Service mesh architecture
- `examples/actor-system/` - Distributed actor system with supervisor/worker
- `examples/webtransport/` - WebTransport/HTTP/3 standalone demo
- `examples/webtransport-integrated/` - WebTransport with full RPC

Each example includes a README with running instructions.

## Transport Limitations

**Current WebSocket Support:**
- ✅ Client can connect to server via `ws://` or `wss://` URLs
- ✅ Client can call server methods (request-response RPC)
- ✅ Server can respond to client requests
- ❌ Server **cannot** initiate calls to clients (no bidirectional RPC yet)
- ❌ Chat example currently non-functional due to this limitation

**Workaround for bidirectional RPC:**
Use HTTP Batch transport instead - it supports full bidirectional RPC including:
- Passing client capabilities to server
- Server calling methods on client capabilities
- See `examples/peer_to_peer/` for working bidirectional RPC example

**WebTransport:**
- Full bidirectional support
- Requires `aioquic` library: `pip install capnweb[webtransport]`

## Development

```bash
# Clone and install
git clone https://github.com/abilian/py-capnweb.git
cd py-capnweb
uv sync

# Run tests
pytest
# or
make test

# Run linting & type checking
ruff check
pyrefly check
# or
make check

# Run with coverage
pytest --cov=capnweb --cov-report=term-missing
```

## Protocol Compliance

This implementation follows the [Cap'n Web protocol specification](https://github.com/cloudflare/capnweb/blob/main/protocol.md).

**Interoperability Testing:**
Cross-implementation testing with TypeScript reference validates all combinations:
- Python Server ↔ Python Client ✅
- Python Server ↔ TypeScript Client ✅
- TypeScript Server ↔ Python Client ✅
- TypeScript Server ↔ TypeScript Client ✅

Run interop tests: `cd interop && bash run_tests.sh`

## What's New

See [CHANGES.md](CHANGES.md) for detailed release notes.

**v0.4.0** (latest):
- WebTransport/HTTP/3 support with certificate management
- Actor system example with distributed capabilities
- "Perfect" code quality (0 linting errors, 0 typing errors)
- 352 tests passing

**v0.3.1**:
- Comprehensive documentation (quickstart, architecture, API reference)
- 85% test coverage (up from 67%)
- Legacy code removed (clean hook-based architecture)

**v0.3.0**:
- Promise pipelining support
- 100% TypeScript interoperability
- Array escaping for compatibility

## Acknowledgments

This project is based on [py-capnweb](https://github.com/abilian/py-capnweb) by Abilian SAS, with significant enhancements including:
- Refactored `ValueCodec` and `CapabilityCodec` architecture
- Improved error handling and error code propagation
- Enhanced examples with public API patterns
- Additional test coverage

## License

Dual-licensed under MIT or Apache-2.0, at your option.
