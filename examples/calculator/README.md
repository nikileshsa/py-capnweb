# Calculator Example

A simple RPC calculator demonstrating basic Cap'n Web usage with HTTP batch transport.

## Features

- Basic arithmetic operations (add, subtract, multiply, divide)
- HTTP batch RPC transport
- Simple server/client architecture

## Running

### Terminal 1 - Start the server

```bash
cd py-capnweb
uv run python examples/calculator/server.py
```

### Terminal 2 - Run the client

```bash
cd py-capnweb
uv run python examples/calculator/client.py
```

## Architecture

```
┌─────────────────┐     HTTP POST      ┌─────────────────┐
│     Client      │ ──────────────────▶│     Server      │
│                 │                    │                 │
│  call("add",    │◀────────────────── │  Calculator     │
│       [5, 3])   │     Response       │  RpcTarget      │
└─────────────────┘                    └─────────────────┘
```

## Code Overview

### Server (`server.py`)

Defines a `Calculator` class that implements `RpcTarget`:
- `add(a, b)` - Returns a + b
- `subtract(a, b)` - Returns a - b
- `multiply(a, b)` - Returns a * b
- `divide(a, b)` - Returns a / b

### Client (`client.py`)

Makes RPC calls to the calculator server using HTTP batch transport.
