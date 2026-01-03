"""Collaborative Document Editor Server.

Demonstrates all Cap'n Web features:
- Capability-based security (documents as capabilities)
- Bidirectional RPC (real-time updates via WebSocket)
- Promise pipelining (chained operations)
- Three-party capability passing (sharing documents)

Run:
    uv run python examples/collab-docs/server.py
"""

import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from aiohttp import web

from capnweb.error import RpcError
from capnweb.rpc_session import BidirectionalSession
from capnweb.stubs import RpcStub
from capnweb.types import RpcTarget
from capnweb.ws_transport import WebSocketServerTransport

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# =============================================================================
# Data Models
# =============================================================================

@dataclass
class Edit:
    """A single edit operation."""
    user_id: str
    timestamp: datetime
    operation: str  # "insert" or "delete"
    position: int
    text: str = ""
    length: int = 0


@dataclass
class Document:
    """Document data."""
    id: str
    title: str
    content: str
    owner_id: str
    created_at: datetime
    history: list[Edit] = field(default_factory=list)
    collaborators: dict[str, str] = field(default_factory=dict)  # user_id -> permission


@dataclass
class User:
    """User data."""
    id: str
    username: str
    email: str


# =============================================================================
# In-Memory Storage
# =============================================================================

users_db: dict[str, User] = {
    "user1": User("user1", "alice", "alice@example.com"),
    "user2": User("user2", "bob", "bob@example.com"),
    "user3": User("user3", "charlie", "charlie@example.com"),
}

documents_db: dict[str, Document] = {}
sessions_db: dict[str, str] = {}  # token -> user_id

# Active editors per document (for real-time updates)
active_editors: dict[str, dict[str, "EditorCapability"]] = {}  # doc_id -> {user_id -> editor}


# =============================================================================
# Capabilities
# =============================================================================

@dataclass
class DocumentCapability(RpcTarget):
    """Capability to access a document with specific permissions.
    
    This is the core of capability-based security:
    - Holding this capability grants access to the document
    - Permission level determines what operations are allowed
    - Can be shared with others (if you have admin permission)
    - Revoked when disposed
    """
    
    doc_id: str
    user_id: str
    permission: str  # "read", "write", "admin"
    
    async def call(self, method: str, args: list[Any]) -> Any:
        doc = documents_db.get(self.doc_id)
        if not doc:
            raise RpcError.not_found(f"Document '{self.doc_id}' not found")
        
        match method:
            case "read":
                return self._read(doc)
            case "write":
                return await self._write(doc, args[0])
            case "getHistory":
                return self._get_history(doc)
            case "getInfo":
                return self._get_info(doc)
            case "share":
                return self._share(doc, args[0], args[1])
            case "openEditor":
                return self._open_editor(doc)
            case _:
                raise RpcError.not_found(f"Method '{method}' not found")
    
    async def get_property(self, name: str) -> Any:
        doc = documents_db.get(self.doc_id)
        if not doc:
            raise RpcError.not_found(f"Document '{self.doc_id}' not found")
        
        match name:
            case "title": return doc.title
            case "content": return doc.content
            case "permission": return self.permission
            case _: raise RpcError.not_found(f"Property '{name}' not found")
    
    def _read(self, doc: Document) -> dict[str, Any]:
        """Read document content."""
        return {
            "id": doc.id,
            "title": doc.title,
            "content": doc.content,
            "owner": doc.owner_id,
            "permission": self.permission,
        }
    
    async def _write(self, doc: Document, content: str) -> dict[str, Any]:
        """Write document content (requires write or admin permission)."""
        if self.permission == "read":
            raise RpcError.permission_denied("Read-only access")
        
        old_content = doc.content
        doc.content = content
        
        # Record edit
        edit = Edit(
            user_id=self.user_id,
            timestamp=datetime.now(),
            operation="replace",
            position=0,
            text=content,
        )
        doc.history.append(edit)
        
        # Notify all active editors
        await self._notify_editors(doc, {
            "type": "content_changed",
            "userId": self.user_id,
            "content": content,
        })
        
        return {"success": True, "length": len(content)}
    
    def _get_history(self, doc: Document) -> list[dict[str, Any]]:
        """Get edit history."""
        return [
            {
                "userId": e.user_id,
                "timestamp": e.timestamp.isoformat(),
                "operation": e.operation,
                "position": e.position,
                "text": e.text[:50] + "..." if len(e.text) > 50 else e.text,
            }
            for e in doc.history[-20:]  # Last 20 edits
        ]
    
    def _get_info(self, doc: Document) -> dict[str, Any]:
        """Get document info."""
        return {
            "id": doc.id,
            "title": doc.title,
            "owner": doc.owner_id,
            "created": doc.created_at.isoformat(),
            "collaborators": list(doc.collaborators.keys()),
            "editCount": len(doc.history),
            "permission": self.permission,
        }
    
    def _share(self, doc: Document, target_user_id: str, permission: str) -> "DocumentCapability":
        """Share document with another user (requires admin permission).
        
        This demonstrates THREE-PARTY CAPABILITY PASSING:
        - User A (admin) creates a capability for User B
        - The capability is returned to User A
        - User A can pass it to User B
        - User B can now access the document
        """
        if self.permission != "admin":
            raise RpcError.permission_denied("Only admins can share documents")
        
        if target_user_id not in users_db:
            raise RpcError.not_found(f"User '{target_user_id}' not found")
        
        if permission not in ("read", "write", "admin"):
            raise RpcError.bad_request(f"Invalid permission: {permission}")
        
        # Record the share
        doc.collaborators[target_user_id] = permission
        
        # Return a NEW capability for the target user
        return DocumentCapability(
            doc_id=self.doc_id,
            user_id=target_user_id,
            permission=permission,
        )
    
    def _open_editor(self, doc: Document) -> "EditorCapability":
        """Open a real-time editor for this document.
        
        This demonstrates BIDIRECTIONAL RPC:
        - Client gets an EditorCapability
        - Editor can push updates to the client
        - Multiple editors can collaborate in real-time
        """
        if self.permission == "read":
            raise RpcError.permission_denied("Read-only access - cannot edit")
        
        editor = EditorCapability(
            doc_id=self.doc_id,
            user_id=self.user_id,
            permission=self.permission,
        )
        
        # Register as active editor
        if self.doc_id not in active_editors:
            active_editors[self.doc_id] = {}
        active_editors[self.doc_id][self.user_id] = editor
        
        return editor
    
    async def _notify_editors(self, doc: Document, update: dict[str, Any]) -> None:
        """Notify all active editors of an update."""
        editors = active_editors.get(doc.id, {})
        for user_id, editor in editors.items():
            if user_id != self.user_id:  # Don't notify self
                try:
                    await editor._receive_update(update)
                except Exception as e:
                    logger.warning(f"Failed to notify editor {user_id}: {e}")


@dataclass
class EditorCapability(RpcTarget):
    """Real-time editor capability.
    
    Demonstrates BIDIRECTIONAL RPC:
    - Client can call methods (insert, delete, setCursor)
    - Server can push updates via the callback
    """
    
    doc_id: str
    user_id: str
    permission: str
    update_callback: RpcStub | None = None
    cursor_position: int = 0
    
    async def call(self, method: str, args: list[Any]) -> Any:
        doc = documents_db.get(self.doc_id)
        if not doc:
            raise RpcError.not_found(f"Document '{self.doc_id}' not found")
        
        match method:
            case "insert":
                return await self._insert(doc, args[0], args[1])
            case "delete":
                return await self._delete(doc, args[0], args[1])
            case "setCursor":
                return await self._set_cursor(doc, args[0])
            case "getCollaborators":
                return self._get_collaborators()
            case "setUpdateCallback":
                return self._set_update_callback(args[0])
            case "close":
                return self._close()
            case _:
                raise RpcError.not_found(f"Method '{method}' not found")
    
    async def get_property(self, name: str) -> Any:
        match name:
            case "cursor": return self.cursor_position
            case "docId": return self.doc_id
            case _: raise RpcError.not_found(f"Property '{name}' not found")
    
    async def _insert(self, doc: Document, position: int, text: str) -> dict[str, Any]:
        """Insert text at position."""
        if position < 0 or position > len(doc.content):
            raise RpcError.bad_request(f"Invalid position: {position}")
        
        doc.content = doc.content[:position] + text + doc.content[position:]
        
        edit = Edit(
            user_id=self.user_id,
            timestamp=datetime.now(),
            operation="insert",
            position=position,
            text=text,
        )
        doc.history.append(edit)
        
        # Notify other editors
        await self._broadcast({
            "type": "insert",
            "userId": self.user_id,
            "position": position,
            "text": text,
        })
        
        return {"success": True, "newLength": len(doc.content)}
    
    async def _delete(self, doc: Document, position: int, length: int) -> dict[str, Any]:
        """Delete text at position."""
        if position < 0 or position + length > len(doc.content):
            raise RpcError.bad_request(f"Invalid range: {position}:{position + length}")
        
        deleted = doc.content[position:position + length]
        doc.content = doc.content[:position] + doc.content[position + length:]
        
        edit = Edit(
            user_id=self.user_id,
            timestamp=datetime.now(),
            operation="delete",
            position=position,
            length=length,
            text=deleted,
        )
        doc.history.append(edit)
        
        # Notify other editors
        await self._broadcast({
            "type": "delete",
            "userId": self.user_id,
            "position": position,
            "length": length,
        })
        
        return {"success": True, "deleted": deleted, "newLength": len(doc.content)}
    
    async def _set_cursor(self, doc: Document, position: int) -> dict[str, Any]:
        """Set cursor position and broadcast to collaborators."""
        self.cursor_position = position
        
        await self._broadcast({
            "type": "cursor",
            "userId": self.user_id,
            "position": position,
        })
        
        return {"success": True}
    
    def _get_collaborators(self) -> list[dict[str, Any]]:
        """Get list of active collaborators."""
        editors = active_editors.get(self.doc_id, {})
        return [
            {
                "userId": uid,
                "username": users_db.get(uid, User(uid, "unknown", "")).username,
                "cursor": editor.cursor_position,
            }
            for uid, editor in editors.items()
        ]
    
    def _set_update_callback(self, callback: RpcStub) -> dict[str, Any]:
        """Set callback for receiving real-time updates.
        
        This is the BIDIRECTIONAL part - server can call this callback
        to push updates to the client.
        """
        self.update_callback = callback
        return {"success": True}
    
    def _close(self) -> dict[str, Any]:
        """Close the editor and unregister."""
        editors = active_editors.get(self.doc_id, {})
        if self.user_id in editors:
            del editors[self.user_id]
        return {"success": True}
    
    async def _broadcast(self, update: dict[str, Any]) -> None:
        """Broadcast update to all other editors."""
        editors = active_editors.get(self.doc_id, {})
        for user_id, editor in editors.items():
            if user_id != self.user_id and editor.update_callback:
                try:
                    await editor.update_callback.onUpdate(update)
                except Exception as e:
                    logger.warning(f"Failed to broadcast to {user_id}: {e}")
    
    async def _receive_update(self, update: dict[str, Any]) -> None:
        """Receive an update from another editor."""
        if self.update_callback:
            try:
                await self.update_callback.onUpdate(update)
            except Exception as e:
                logger.warning(f"Failed to deliver update: {e}")


@dataclass
class UserCapability(RpcTarget):
    """User capability - represents a logged-in user.
    
    Demonstrates CAPABILITY-BASED SECURITY:
    - Holding this capability proves authentication
    - Can create documents, list own documents
    - Can receive shared documents from others
    """
    
    user_id: str
    
    async def call(self, method: str, args: list[Any]) -> Any:
        user = users_db.get(self.user_id)
        if not user:
            raise RpcError.not_found(f"User '{self.user_id}' not found")
        
        match method:
            case "getProfile":
                return self._get_profile(user)
            case "listDocuments":
                return self._list_documents()
            case "createDocument":
                return self._create_document(args[0])
            case "getDocument":
                return self._get_document(args[0])
            case _:
                raise RpcError.not_found(f"Method '{method}' not found")
    
    async def get_property(self, name: str) -> Any:
        user = users_db.get(self.user_id)
        if not user:
            raise RpcError.not_found(f"User '{self.user_id}' not found")
        
        match name:
            case "id": return user.id
            case "username": return user.username
            case "email": return user.email
            case _: raise RpcError.not_found(f"Property '{name}' not found")
    
    def _get_profile(self, user: User) -> dict[str, Any]:
        """Get user profile."""
        return {
            "id": user.id,
            "username": user.username,
            "email": user.email,
        }
    
    def _list_documents(self) -> list[dict[str, Any]]:
        """List documents owned by or shared with this user."""
        result = []
        for doc in documents_db.values():
            if doc.owner_id == self.user_id:
                result.append({
                    "id": doc.id,
                    "title": doc.title,
                    "permission": "admin",
                    "owner": True,
                })
            elif self.user_id in doc.collaborators:
                result.append({
                    "id": doc.id,
                    "title": doc.title,
                    "permission": doc.collaborators[self.user_id],
                    "owner": False,
                })
        return result
    
    def _create_document(self, title: str) -> DocumentCapability:
        """Create a new document.
        
        This demonstrates PROMISE PIPELINING:
        - Returns a DocumentCapability immediately
        - Client can chain operations without waiting
        - e.g., user.createDocument("My Doc").openEditor().insert(0, "Hello")
        """
        doc_id = str(uuid.uuid4())[:8]
        doc = Document(
            id=doc_id,
            title=title,
            content="",
            owner_id=self.user_id,
            created_at=datetime.now(),
        )
        documents_db[doc_id] = doc
        
        # Return capability with admin permission (owner)
        return DocumentCapability(
            doc_id=doc_id,
            user_id=self.user_id,
            permission="admin",
        )
    
    def _get_document(self, doc_id: str) -> DocumentCapability:
        """Get a document capability by ID."""
        doc = documents_db.get(doc_id)
        if not doc:
            raise RpcError.not_found(f"Document '{doc_id}' not found")
        
        # Check access
        if doc.owner_id == self.user_id:
            permission = "admin"
        elif self.user_id in doc.collaborators:
            permission = doc.collaborators[self.user_id]
        else:
            raise RpcError.permission_denied("No access to this document")
        
        return DocumentCapability(
            doc_id=doc_id,
            user_id=self.user_id,
            permission=permission,
        )


# =============================================================================
# Server Gateway
# =============================================================================

class CollabDocsGateway(RpcTarget):
    """Main gateway for the collaborative docs service."""
    
    async def call(self, method: str, args: list[Any]) -> Any:
        match method:
            case "login":
                return self._login(args[0], args[1])
            case "register":
                return self._register(args[0], args[1], args[2])
            case "listPublicDocuments":
                return self._list_public_documents()
            case _:
                raise RpcError.not_found(f"Method '{method}' not found")
    
    async def get_property(self, name: str) -> Any:
        raise RpcError.not_found(f"Property '{name}' not found")
    
    def _login(self, username: str, password: str) -> dict[str, Any]:
        """Login and get a UserCapability.
        
        Returns both a token (for stateless HTTP batch) and a capability
        (for stateful WebSocket operations).
        """
        # Find user by username
        user = None
        for u in users_db.values():
            if u.username == username:
                user = u
                break
        
        if not user:
            raise RpcError.bad_request("Invalid username or password")
        
        # Create session token
        token = str(uuid.uuid4())
        sessions_db[token] = user.id
        
        # Return both token and capability
        return {
            "token": token,
            "userId": user.id,
            "username": user.username,
            "user": UserCapability(user_id=user.id),  # The CAPABILITY
        }
    
    def _register(self, username: str, email: str, password: str) -> dict[str, Any]:
        """Register a new user."""
        # Check if username exists
        for u in users_db.values():
            if u.username == username:
                raise RpcError.bad_request("Username already taken")
        
        user_id = f"user{len(users_db) + 1}"
        user = User(id=user_id, username=username, email=email)
        users_db[user_id] = user
        
        return {"success": True, "userId": user_id}
    
    def _list_public_documents(self) -> list[dict[str, str]]:
        """List public documents (for discovery)."""
        # In a real app, you'd have a "public" flag
        return [
            {"id": doc.id, "title": doc.title, "owner": doc.owner_id}
            for doc in list(documents_db.values())[:10]
        ]


# =============================================================================
# WebSocket Handler
# =============================================================================

gateway = CollabDocsGateway()


async def handle_websocket(request: web.Request) -> web.WebSocketResponse:
    """Handle WebSocket connections."""
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    
    transport = WebSocketServerTransport(ws)
    session = BidirectionalSession(transport, gateway)
    session.start()
    
    logger.info("Client connected")
    
    try:
        async for msg in ws:
            if msg.type == web.WSMsgType.TEXT:
                transport.feed_message(msg.data)
            elif msg.type == web.WSMsgType.BINARY:
                transport.feed_message(msg.data.decode("utf-8"))
            elif msg.type == web.WSMsgType.ERROR:
                transport.set_error(ws.exception() or Exception("WebSocket error"))
                break
    except Exception as e:
        logger.error("WebSocket error: %s", e)
        transport.set_error(e)
    finally:
        transport.set_closed()
        logger.info("Client disconnected")
    
    return ws


async def main() -> None:
    """Run the collaborative docs server."""
    app = web.Application()
    app.router.add_get("/rpc/ws", handle_websocket)
    
    runner = web.AppRunner(app)
    await runner.setup()
    
    site = web.TCPSite(runner, "127.0.0.1", 8080)
    await site.start()
    
    print("📝 Collaborative Docs Server running on http://127.0.0.1:8080")
    print("   WebSocket endpoint: ws://127.0.0.1:8080/rpc/ws")
    print()
    print("Available users (password = username):")
    for user in users_db.values():
        print(f"  - {user.username}")
    print()
    print("Run client with: uv run python examples/collab-docs/client.py")
    print("Press Ctrl+C to stop")
    
    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
