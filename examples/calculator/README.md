# Calculator Example

A simple RPC calculator demonstrating basic Cap'n Web usage with HTTP batch transport.

## Features

- Basic arithmetic operations (add, subtract, multiply, divide)
- HTTP batch RPC transport
- Error handling (division by zero)

## Running

### Step 1: Start the server

```bash
cd capnweb-python
uv run python examples/calculator/server.py
```

**Expected output:**
```
🧮 Calculator server running on http://127.0.0.1:8080
   Endpoint: http://127.0.0.1:8080/rpc/batch

Run client with: uv run python examples/calculator/client.py
Press Ctrl+C to stop
```

### Step 2: Run the client (new terminal)

```bash
cd capnweb-python
uv run python examples/calculator/client.py
```

**Expected output:**
```
🧮 Calculator Client
========================================

Testing add(5, 3)...
  5 + 3 = 8

Testing subtract(10, 4)...
  10 - 4 = 6

Testing multiply(7, 6)...
  7 × 6 = 42

Testing divide(20, 4)...
  20 ÷ 4 = 5.0

Testing divide(10, 0) - should fail...
  Expected error: bad_request: Division by zero

========================================
✅ All tests completed!
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
