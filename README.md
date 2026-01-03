# Cap'n Web Python

A complete Python implementation of the [Cap'n Web protocol](https://github.com/cloudflare/capnweb) - a capability-based RPC system with promise pipelining, structured errors, and multiple transport support.

## What's in the Box

**Core Features:**
- **Capability-based security** - Unforgeable object references with explicit disposal
- **Promise pipelining** - Batch multiple dependent calls into single round-trips
- **Multiple transports** - HTTP Batch, WebSocket, and WebTransport/HTTP/3
- **Type-safe** - Full type hints compatible with pyright/mypy
- **Async/await** - Built on Python's asyncio
- **Bidirectional RPC** - Full peer-to-peer capability passing
- **100% Interoperable** - Fully compatible with TypeScript reference implementation

**Production-Ready:**
- 744 tests passing, 70% coverage
- 0 linting errors, 0 typing errors
- Clean hook-based architecture
- Full protocol compliance

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

**Server (HTTP Batch):**
```python
import asyncio
from aiohttp import web
from capnweb import RpcTarget, RpcError, aiohttp_batch_rpc_handler

class Calculator(RpcTarget):
    async def call(self, method: str, args: list) -> any:
        match method:
            case "add": return args[0] + args[1]
            case "multiply": return args[0] * args[1]
            case _: raise RpcError.not_found(f"Unknown method: {method}")

    async def get_property(self, name: str) -> any:
        raise RpcError.not_found(f"Property '{name}' not found")

async def main():
    calc = Calculator()
    app = web.Application()
    app.router.add_post("/rpc/batch", lambda req: aiohttp_batch_rpc_handler(req, calc))
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "127.0.0.1", 8080).start()
    await asyncio.Event().wait()

asyncio.run(main())
```

**Client:**
```python
import asyncio
from capnweb import UnifiedClient, UnifiedClientConfig

async def main():
    config = UnifiedClientConfig(url="http://localhost:8080/rpc/batch")
    async with UnifiedClient(config) as client:
        calc = client.get_main_stub()
        result = await calc.add(5, 3)
        print(f"5 + 3 = {result}")  # Output: 8

asyncio.run(main())
```

**Capability Passing** (bidirectional RPC):
```python
# Server returns a capability, client calls methods on it directly
account = await main_stub.createAccount(1000.0)
balance = await account.getBalance()  # Pythonic API!
await account.deposit(500.0)
```

## Current Status (v0.6.0)

**Transports:**
- ✅ HTTP Batch (stateless, pipelining)
- ✅ WebSocket (full bidirectional RPC with capability passing)
- ✅ WebTransport/HTTP/3 (requires aioquic)

**Protocol Features:**
- ✅ Wire protocol (all message types)
- ✅ Promise pipelining
- ✅ Expression evaluation (including `.map()`)
- ✅ Bidirectional RPC (full capability passing)
- ✅ Reference counting & disposal
- ✅ Structured errors with proper code propagation
- ✅ ValueCodec & CapabilityCodec architecture

**Code Quality:**
- ✅ 744 tests passing (100% success rate)
- ✅ 70% test coverage
- ✅ 0 linting errors (ruff)
- ✅ 0 typing errors (pyright)
- ✅ TypeScript interoperability verified

## Documentation

- **[Quickstart Guide](docs/quickstart.md)** - Get started in 5 minutes
- **[API Reference](docs/api-reference.md)** - Complete API documentation
- **[Architecture Guide](docs/architecture.md)** - Understand the internals
- **[Examples](examples/)** - Working code examples

## Examples

**All examples tested and working:**

| Example | Transport | Description |
|---------|-----------|-------------|
| `calculator/` | HTTP Batch | Simple RPC calculator with error handling |
| `batch-pipelining/` | HTTP Batch | Promise pipelining demonstration |
| `peer-to-peer/` | HTTP Batch | Bidirectional RPC (Alice & Bob) |
| `chat/` | WebSocket | Real-time chat with callbacks |
| `task-queue/` | WebSocket | Distributed task queue with progress callbacks |
| `collab-docs/` | WebSocket | Collaborative document editor |
| `capability-security/` | WebSocket | Bank account with capability attenuation |
| `microservices/` | WebSocket | Service mesh with capability passing |
| `actor-system/` | WebSocket | Supervisor/worker pattern |
| `webtransport/` | HTTP/3 | WebTransport/QUIC demo |

Each example includes a README with running instructions.

## Transport Features

| Feature | HTTP Batch | WebSocket | WebTransport |
|---------|------------|-----------|---------------|
| Request/Response | ✅ | ✅ | ✅ |
| Bidirectional RPC | ✅ | ✅ | ✅ |
| Capability Passing | ✅ | ✅ | ✅ |
| Server Callbacks | ✅ | ✅ | ✅ |
| Persistent Connection | ❌ | ✅ | ✅ |
| Multiplexing | Manual | Auto | Native |

**WebTransport:**
- Requires `aioquic` library: `pip install capnweb[webtransport]`
- Best for high-performance, low-latency applications

## Development

```bash
# Clone and install
git clone https://github.com/nikileshsa/capnweb-python.git
cd capnweb-python
uv sync

# Run tests
uv run pytest

# Run linting & type checking
uv run ruff check
uv run pyright

# Run with coverage
uv run pytest --cov=capnweb --cov-report=term-missing
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

**v0.6.0** (current):
- Refactored `ValueCodec` and `CapabilityCodec` architecture
- Fixed `RpcError` code propagation (errors now preserve original codes)
- Added `create_stub()` public API for creating stubs from RpcTarget
- All examples refactored to use public API patterns
- 744 tests passing, 70% coverage
- Full bidirectional WebSocket RPC support

**v0.5.0**:
- WebTransport/HTTP/3 support
- Actor system example

**v0.4.0**:
- WebSocket bidirectional RPC
- Comprehensive documentation

## Acknowledgments

This project is based on [py-capnweb](https://github.com/abilian/py-capnweb) by Abilian SAS, with significant enhancements including:
- Refactored `ValueCodec` and `CapabilityCodec` architecture
- Improved error handling and error code propagation
- Enhanced examples with public API patterns
- Additional test coverage

## License

Dual-licensed under MIT or Apache-2.0, at your option.
