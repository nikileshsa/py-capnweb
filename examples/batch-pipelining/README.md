# Batch Pipelining Example

Demonstrates HTTP batch RPC with multiple calls in a single request.

## Features

- HTTP batch transport for efficient RPC
- Multiple calls in a single HTTP request
- Simulated network latency to show batching benefits
- Comparison of batched vs sequential calls

## Running

### Step 1: Start the server

```bash
cd capnweb-python
uv run python examples/batch-pipelining/server.py
```

### Step 2: Run the client (new terminal)

```bash
cd capnweb-python
uv run python examples/batch-pipelining/client.py
```

**Expected output:**
```
============================================================
HTTP Batch RPC - Pipelining Demo
============================================================

--- Running sequential calls ---
HTTP requests: 3
Time: 329.54 ms
Authenticated user: {'id': 'u_1', 'name': 'Ada Lovelace'}
Profile: {'id': 'u_1', 'bio': 'Mathematician & first programmer'}
Notifications: ['Welcome to capnweb!', 'You have 2 new followers']

--- Running batched calls ---
HTTP requests: 3
Time: 325.73 ms
...
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                         Client                               │
│                                                              │
│  1. authenticate(token) ─────┐                               │
│  2. getUserProfile(userId) ──┼── Single HTTP POST ──────────▶│
│  3. getNotifications(userId)─┘                               │
│                                                              │
└─────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────┐
│                         Server                               │
│                                                              │
│  API:                                                        │
│  - authenticate(token) → {id, name}                          │
│  - getUserProfile(userId) → {id, bio}                        │
│  - getNotifications(userId) → [...]                          │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

## Batching Benefits

Without batching (3 sequential HTTP requests):
```
Request 1: authenticate()     → 80ms server + RTT
Request 2: getUserProfile()   → 120ms server + RTT  
Request 3: getNotifications() → 120ms server + RTT
Total: ~320ms server + 3×RTT
```

With batching (1 HTTP request with 3 calls):
```
Request 1: [authenticate, getUserProfile, getNotifications]
           → 320ms server (parallel) + 1×RTT
Total: ~320ms server + 1×RTT
```

The batching approach saves 2 round trips!
