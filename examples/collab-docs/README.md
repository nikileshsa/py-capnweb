# Collaborative Document Editor

A real-world application demonstrating all Cap'n Web features:

## Features Demonstrated

1. **Capability-Based Security**
   - Documents are capabilities, not just IDs
   - Share access by passing document capabilities
   - Revoke access by disposing capabilities
   - Fine-grained permissions (read, write, admin)

2. **Bidirectional RPC (WebSocket)**
   - Real-time document updates pushed to all editors
   - Cursor position sharing
   - Presence indicators (who's online)

3. **Promise Pipelining**
   - Create document → get editor → start editing (no round-trips)
   - Chain operations without waiting for responses

4. **HTTP Batch RPC**
   - Document listing and search (stateless)
   - User authentication

5. **Three-Party Capability Passing**
   - User A shares document with User B
   - User B can edit using the shared capability
   - Server mediates but doesn't need to trust either party

## Architecture

```
┌─────────────┐     WebSocket      ┌─────────────────┐
│   Client    │◄──────────────────►│     Server      │
│  (Editor)   │                    │                 │
└─────────────┘                    │  ┌───────────┐  │
                                   │  │ Documents │  │
┌─────────────┐     WebSocket      │  └───────────┘  │
│   Client    │◄──────────────────►│                 │
│  (Viewer)   │                    │  ┌───────────┐  │
└─────────────┘                    │  │   Users   │  │
                                   │  └───────────┘  │
┌─────────────┐     HTTP Batch     │                 │
│   Client    │◄──────────────────►│                 │
│  (Search)   │                    └─────────────────┘
└─────────────┘
```

## Running

1. Start the server:
   ```bash
   uv run python examples/collab-docs/server.py
   ```

2. Run the interactive client:
   ```bash
   uv run python examples/collab-docs/client.py
   ```

## Capabilities

### DocumentCapability
- `read()` - Read document content
- `write(content)` - Write content (requires write permission)
- `getHistory()` - Get edit history
- `share(userId, permission)` - Share with another user (requires admin)
- `onUpdate(callback)` - Subscribe to real-time updates

### EditorCapability
- `insert(position, text)` - Insert text
- `delete(position, length)` - Delete text
- `setCursor(position)` - Set cursor position
- `getCollaborators()` - Get list of active editors

### UserCapability
- `getProfile()` - Get user profile
- `listDocuments()` - List user's documents
- `createDocument(title)` - Create new document
