"""Collaborative Document Editor Client.

Interactive client demonstrating all Cap'n Web features:
- Capability-based security
- Bidirectional RPC (real-time updates)
- Promise pipelining
- Three-party capability passing

Run (after starting server.py):
    uv run python examples/collab-docs/client.py
"""

import asyncio
import sys
from dataclasses import dataclass
from typing import Any

from capnweb.error import RpcError
from capnweb.rpc_session import BidirectionalSession
from capnweb.stubs import RpcStub
from capnweb.types import RpcTarget
from capnweb.ws_transport import WebSocketClientTransport

SERVER_URL = "ws://127.0.0.1:8080/rpc/ws"


# =============================================================================
# Client-side callback for receiving real-time updates
# =============================================================================

@dataclass
class UpdateHandler(RpcTarget):
    """Handles real-time updates from the server.
    
    This demonstrates BIDIRECTIONAL RPC:
    - Server calls onUpdate() to push changes
    - Client receives and displays them
    """
    
    username: str
    
    async def call(self, method: str, args: list[Any]) -> Any:
        match method:
            case "onUpdate":
                return self._on_update(args[0])
            case _:
                raise RpcError.not_found(f"Method '{method}' not found")
    
    async def get_property(self, name: str) -> Any:
        raise RpcError.not_found(f"Property '{name}' not found")
    
    def _on_update(self, update: dict[str, Any]) -> None:
        """Handle an update from the server."""
        update_type = update.get("type", "unknown")
        user_id = update.get("userId", "unknown")
        
        if update_type == "insert":
            print(f"\n  📝 [{user_id}] inserted '{update.get('text', '')}' at position {update.get('position', 0)}")
        elif update_type == "delete":
            print(f"\n  🗑️  [{user_id}] deleted {update.get('length', 0)} chars at position {update.get('position', 0)}")
        elif update_type == "cursor":
            print(f"\n  👆 [{user_id}] moved cursor to position {update.get('position', 0)}")
        elif update_type == "content_changed":
            print(f"\n  📄 [{user_id}] replaced content")
        else:
            print(f"\n  ❓ Unknown update: {update}")


class DummyTarget(RpcTarget):
    """Dummy target for client side."""
    async def call(self, method: str, args: list[Any]) -> Any:
        raise RpcError.not_found(f"Method '{method}' not found")
    async def get_property(self, name: str) -> Any:
        raise RpcError.not_found(f"Property '{name}' not found")


# =============================================================================
# Interactive Client
# =============================================================================

async def read_input(prompt: str = "") -> str:
    """Read input asynchronously."""
    if prompt:
        print(prompt, end="", flush=True)
    loop = asyncio.get_event_loop()
    return (await loop.run_in_executor(None, sys.stdin.readline)).strip()


async def main() -> None:
    """Run the interactive collaborative docs client."""
    print("=" * 60)
    print("  📝 Collaborative Document Editor")
    print("  Demonstrating Cap'n Web Features")
    print("=" * 60)
    print()
    
    # Connect via WebSocket
    transport = WebSocketClientTransport(SERVER_URL)
    await transport.connect()
    
    session = BidirectionalSession(transport, DummyTarget())
    session.start()
    
    gateway = RpcStub(session.get_main_stub())
    
    # State
    user_cap = None
    current_doc = None
    current_editor = None
    update_handler = None
    
    try:
        while True:
            print()
            if not user_cap:
                print("Commands: login, register, quit")
            elif not current_doc:
                print("Commands: list, create, open <id>, share, logout, quit")
            elif not current_editor:
                print("Commands: read, write, info, history, edit, share, close, quit")
            else:
                print("Commands: insert <pos> <text>, delete <pos> <len>, cursor <pos>, content, collaborators, stop, quit")
            
            cmd = await read_input("> ")
            parts = cmd.split(maxsplit=2)
            if not parts:
                continue
            
            action = parts[0].lower()
            
            try:
                # === Authentication ===
                if action == "login":
                    username = await read_input("Username: ")
                    password = await read_input("Password: ")
                    
                    result = await gateway.login(username, password)
                    user_cap = result["user"]  # This is a CAPABILITY!
                    
                    print(f"\n✅ Logged in as {result['username']}")
                    print(f"   User ID: {result['userId']}")
                    print(f"   Token: {result['token'][:20]}...")
                    print("\n   You now have a UserCapability - this proves your identity!")
                
                elif action == "register":
                    username = await read_input("Username: ")
                    email = await read_input("Email: ")
                    password = await read_input("Password: ")
                    
                    result = await gateway.register(username, email, password)
                    print(f"\n✅ Registered! User ID: {result['userId']}")
                    print("   Now login to get your UserCapability")
                
                elif action == "logout":
                    user_cap = None
                    current_doc = None
                    current_editor = None
                    print("\n✅ Logged out")
                
                # === Document Management ===
                elif action == "list":
                    if not user_cap:
                        print("❌ Please login first")
                        continue
                    
                    docs = await user_cap.listDocuments()
                    print(f"\n📚 Your Documents ({len(docs)}):")
                    for doc in docs:
                        owner_mark = "👑" if doc.get("owner") else "📤"
                        print(f"   {owner_mark} [{doc['id']}] {doc['title']} ({doc['permission']})")
                    
                    if not docs:
                        print("   (none - create one with 'create')")
                
                elif action == "create":
                    if not user_cap:
                        print("❌ Please login first")
                        continue
                    
                    title = await read_input("Document title: ")
                    
                    # PROMISE PIPELINING: createDocument returns a DocumentCapability
                    # We can immediately use it without waiting!
                    current_doc = await user_cap.createDocument(title)
                    
                    info = await current_doc.getInfo()
                    print(f"\n✅ Created document: {info['title']}")
                    print(f"   ID: {info['id']}")
                    print(f"   Permission: {info['permission']} (you're the owner)")
                    print("\n   You now have a DocumentCapability with admin access!")
                
                elif action == "open":
                    if not user_cap:
                        print("❌ Please login first")
                        continue
                    
                    doc_id = parts[1] if len(parts) > 1 else await read_input("Document ID: ")
                    
                    # Get a DocumentCapability for this document
                    current_doc = await user_cap.getDocument(doc_id)
                    
                    info = await current_doc.getInfo()
                    print(f"\n✅ Opened: {info['title']}")
                    print(f"   Permission: {info['permission']}")
                    print(f"   Edits: {info['editCount']}")
                    print("\n   You now have a DocumentCapability!")
                
                elif action == "close":
                    current_doc = None
                    current_editor = None
                    print("\n✅ Closed document")
                
                # === Document Operations ===
                elif action == "read":
                    if not current_doc:
                        print("❌ No document open")
                        continue
                    
                    data = await current_doc.read()
                    print(f"\n📄 {data['title']}")
                    print("-" * 40)
                    print(data['content'] or "(empty)")
                    print("-" * 40)
                
                elif action == "write":
                    if not current_doc:
                        print("❌ No document open")
                        continue
                    
                    print("Enter content (end with empty line):")
                    lines = []
                    while True:
                        line = await read_input()
                        if not line:
                            break
                        lines.append(line)
                    content = "\n".join(lines)
                    
                    result = await current_doc.write(content)
                    print(f"\n✅ Written {result['length']} characters")
                
                elif action == "info":
                    if not current_doc:
                        print("❌ No document open")
                        continue
                    
                    info = await current_doc.getInfo()
                    print(f"\n📋 Document Info:")
                    print(f"   ID: {info['id']}")
                    print(f"   Title: {info['title']}")
                    print(f"   Owner: {info['owner']}")
                    print(f"   Created: {info['created']}")
                    print(f"   Edits: {info['editCount']}")
                    print(f"   Your permission: {info['permission']}")
                    print(f"   Collaborators: {info['collaborators'] or '(none)'}")
                
                elif action == "history":
                    if not current_doc:
                        print("❌ No document open")
                        continue
                    
                    history = await current_doc.getHistory()
                    print(f"\n📜 Edit History ({len(history)} recent):")
                    for edit in history:
                        print(f"   [{edit['timestamp']}] {edit['userId']}: {edit['operation']} at {edit['position']}")
                
                # === Sharing (Three-Party Capability Passing) ===
                elif action == "share":
                    if not current_doc:
                        print("❌ No document open")
                        continue
                    
                    target_user = await read_input("Share with user ID (e.g., user2): ")
                    print("Permission levels: read, write, admin")
                    permission = await read_input("Permission: ")
                    
                    # THREE-PARTY CAPABILITY PASSING:
                    # We create a new capability for the target user
                    # They can use it to access the document
                    shared_cap = await current_doc.share(target_user, permission)
                    
                    print(f"\n✅ Shared document with {target_user} ({permission})")
                    print("   A new DocumentCapability was created for them!")
                    print("   In a real app, you'd send this capability to them.")
                    print(f"   Capability type: {type(shared_cap).__name__}")
                
                # === Real-Time Editing ===
                elif action == "edit":
                    if not current_doc:
                        print("❌ No document open")
                        continue
                    
                    # Open a real-time editor
                    current_editor = await current_doc.openEditor()
                    
                    # Set up update callback for BIDIRECTIONAL RPC (public API)
                    from capnweb import create_stub
                    update_handler = UpdateHandler(username="me")
                    callback_stub = create_stub(update_handler)
                    await current_editor.setUpdateCallback(callback_stub)
                    
                    print("\n✅ Opened real-time editor!")
                    print("   You'll see updates from other collaborators.")
                    print("   Commands: insert, delete, cursor, content, collaborators, stop")
                
                elif action == "stop":
                    if current_editor:
                        await current_editor.close()
                        current_editor = None
                        update_handler = None
                        print("\n✅ Closed editor")
                    else:
                        print("❌ No editor open")
                
                elif action == "insert":
                    if not current_editor:
                        print("❌ No editor open (use 'edit' first)")
                        continue
                    
                    if len(parts) < 3:
                        pos = int(await read_input("Position: "))
                        text = await read_input("Text: ")
                    else:
                        pos = int(parts[1])
                        text = parts[2]
                    
                    result = await current_editor.insert(pos, text)
                    print(f"✅ Inserted. New length: {result['newLength']}")
                
                elif action == "delete":
                    if not current_editor:
                        print("❌ No editor open (use 'edit' first)")
                        continue
                    
                    if len(parts) < 3:
                        pos = int(await read_input("Position: "))
                        length = int(await read_input("Length: "))
                    else:
                        pos = int(parts[1])
                        length = int(parts[2])
                    
                    result = await current_editor.delete(pos, length)
                    print(f"✅ Deleted '{result['deleted']}'. New length: {result['newLength']}")
                
                elif action == "cursor":
                    if not current_editor:
                        print("❌ No editor open (use 'edit' first)")
                        continue
                    
                    pos = int(parts[1]) if len(parts) > 1 else int(await read_input("Position: "))
                    await current_editor.setCursor(pos)
                    print(f"✅ Cursor at position {pos}")
                
                elif action == "content":
                    if not current_doc:
                        print("❌ No document open")
                        continue
                    
                    data = await current_doc.read()
                    print(f"\n📄 Current content:")
                    print("-" * 40)
                    content = data['content']
                    # Show with position markers
                    for i, char in enumerate(content):
                        if char == '\n':
                            print(f"[{i}]↵")
                        else:
                            print(f"[{i}]{char}", end="")
                    print(f"[{len(content)}]")
                    print("-" * 40)
                
                elif action == "collaborators":
                    if not current_editor:
                        print("❌ No editor open")
                        continue
                    
                    collabs = await current_editor.getCollaborators()
                    print(f"\n👥 Active Collaborators ({len(collabs)}):")
                    for c in collabs:
                        print(f"   - {c['username']} (cursor at {c['cursor']})")
                
                elif action == "quit":
                    print("\n👋 Goodbye!")
                    break
                
                else:
                    print(f"❌ Unknown command: {action}")
            
            except RpcError as e:
                print(f"\n❌ Error: {e.message}")
            except Exception as e:
                print(f"\n❌ Error: {e}")
    
    finally:
        if current_editor:
            try:
                await current_editor.close()
            except Exception:
                pass
        await session.stop()
        await transport.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n👋 Goodbye!")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("\nMake sure the server is running:")
        print("  uv run python examples/collab-docs/server.py")
