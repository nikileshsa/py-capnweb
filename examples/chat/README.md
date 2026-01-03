# Chat Example

A real-time chat application demonstrating WebSocket transport and bidirectional RPC.

## Features

- WebSocket transport for persistent connections
- Bidirectional RPC (server can call client methods)
- Multiple connected clients
- Broadcasting messages to all clients
- Client capability management

## Running

### Terminal 1 - Start the server

```bash
cd py-capnweb
uv run python examples/chat/server.py
```

### Terminal 2+ - Run clients

```bash
cd py-capnweb
uv run python examples/chat/client.py
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
