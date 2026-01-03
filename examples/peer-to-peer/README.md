# Peer-to-Peer Example

Demonstrates peer-to-peer communication where both parties can act as client and server.

## Features

- Each peer runs both a server and client
- Bidirectional communication between peers
- Direct RPC calls between peers
- No central server required

## Running

### Terminal 1 - Start Alice

```bash
cd py-capnweb
uv run python examples/peer-to-peer/alice.py
```

### Terminal 2 - Start Bob

```bash
cd py-capnweb
uv run python examples/peer-to-peer/bob.py
```

## Architecture

```
┌─────────────────────────────────────┐
│              Alice                   │
│                                      │
│  Server (port 8080)                  │
│  - greet()                           │
│  - chat(message)                     │
│  - get_stats()                       │
│                                      │
│  Client ──────────────────────────┐  │
└─────────────────────────────────────┘
                                    │
                                    │ HTTP
                                    │
                                    ▼
┌─────────────────────────────────────┐
│               Bob                    │
│                                      │
│  Server (port 8081)                  │
│  - greet()                           │
│  - chat(message)                     │
│  - get_stats()                       │
│                                      │
│  Client ──────────────────────────┐  │
└─────────────────────────────────────┘
                                    │
                                    │ HTTP
                                    │
                                    ▼
                              (to Alice)
```

## How It Works

1. Alice starts her server on port 8080
2. Bob starts his server on port 8081
3. Alice connects to Bob and calls his methods
4. Bob connects to Alice and calls her methods
5. Both peers can communicate bidirectionally

This demonstrates the symmetric nature of Cap'n Web - the same code
can act as both client and server.
