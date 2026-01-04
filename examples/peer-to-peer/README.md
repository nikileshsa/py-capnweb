# Peer-to-Peer Example

Demonstrates peer-to-peer communication where both parties can act as client and server.

## Features

- Each peer runs both a server and client
- Bidirectional communication between peers
- Direct RPC calls between peers
- No central server required

## Running

### Step 1: Start Alice (Terminal 1)

```bash
cd capnweb-python
uv run python examples/peer-to-peer/alice.py
```

**Expected output:**
```
🚀 Starting Alice on port 8080...
✅ Alice is running!
   - Alice exports her capabilities at http://127.0.0.1:8080/rpc/batch
   - Alice can receive calls from Bob

🔗 Connecting to Bob at http://127.0.0.1:8081...
📞 Alice calls Bob.greet()...
❌ Could not connect to Bob: ...
   Make sure bob.py is running!

⏳ Alice is waiting for calls from Bob...
```

> Note: Alice tries to connect to Bob on startup. If Bob isn't running yet, this is expected to fail. Alice will continue running and accept connections.

### Step 2: Start Bob (Terminal 2)

```bash
cd capnweb-python
uv run python examples/peer-to-peer/bob.py
```

**Expected output:**
```
🚀 Starting Bob on port 8081...
✅ Bob is running!
   - Bob exports his capabilities at http://127.0.0.1:8081/rpc/batch
   - Bob can receive calls from Alice

🔗 Connecting to Alice at http://127.0.0.1:8080...
📞 Bob calls Alice.greet()...
   ← Hello! I'm Alice.
📞 Bob calls Alice.chat('Hi Alice!')...
   ← Alice says: Thanks for the message #1!
📞 Bob calls Alice.get_stats()...
   ← {'name': 'Alice', 'messages_received': 1}

⏳ Bob is waiting for calls from Alice...
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
