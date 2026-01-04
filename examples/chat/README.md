# Chat Example

A real-time chat application demonstrating WebSocket transport and bidirectional RPC.

## Features

- WebSocket transport for persistent connections
- Bidirectional RPC (server calls client methods)
- Broadcasting messages to all clients
- Client callback capability management

## Running

### Step 1: Start the server

```bash
cd capnweb-python
uv run python examples/chat/server.py
```

**Expected output:**
```
💬 Chat Server running on ws://127.0.0.1:8080/rpc/ws

Methods:
  - join(username, callback) → Welcome message
  - sendMessage(username, text) → Broadcasts to all
  - listUsers() → List of connected users
  - leave(username) → Goodbye message

Run client: uv run python examples/chat/client.py
Press Ctrl+C to stop
```

### Step 2: Run the client (new terminal)

```bash
cd capnweb-python
uv run python examples/chat/client.py
```

**Expected output:**
```
Enter your username: Alice
Joined chat as Alice
Users online: ['Alice']

Type a message and press Enter (or /quit to leave):
> Hello everyone!
[Alice]: Hello everyone!
```

Run multiple clients in different terminals to chat between them.

## Architecture

```
┌─────────────────┐                    ┌─────────────────┐
│   Client 1      │◀───── WebSocket ──▶│                 │
│   (Alice)       │                    │   Chat Server   │
└─────────────────┘                    │                 │
                                       │   - join()      │
┌─────────────────┐                    │   - leave()     │
│   Client 2      │◀───── WebSocket ──▶│   - send()      │
│   (Bob)         │                    │   - broadcast() │
└─────────────────┘                    └─────────────────┘
```

## Bidirectional RPC

The key feature demonstrated here is **bidirectional RPC**:

1. **Client → Server**: Client calls `join()`, `send()`, `leave()`
2. **Server → Client**: Server calls `onMessage()` on each client

When a client joins, they pass their callback capability to the server.
The server stores this capability and uses it to push messages to clients.

```python
# Client registers callback with server
await server.join(username, client_callback)

# Server broadcasts by calling each client's onMessage
for client in clients:
    await client.onMessage({"user": "Alice", "text": "Hello!"})
```

## Commands

In the chat client:
- Type a message and press Enter to send
- `/users` - List connected users
- `/quit` - Leave the chat
- `/help` - Show available commands
