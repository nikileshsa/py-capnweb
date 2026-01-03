"""Interactive chat client using WebSocket transport.

This example demonstrates:
- WebSocket transport for persistent connections
- Bidirectional RPC (client exports capabilities to server)
- Async input handling
- Real-time message updates

Run:
    uv run python examples/chat/client.py
"""

import asyncio
import sys
from contextlib import suppress
from dataclasses import dataclass
from typing import Any

from capnweb import RpcError, RpcTarget, RpcStub, BidirectionalSession, create_stub
from capnweb.ws_transport import WebSocketClientTransport


@dataclass
class ChatClient(RpcTarget):
    """Client-side chat capability that receives messages from the server."""

    username: str

    async def call(self, method: str, args: list[Any]) -> Any:
        """Handle RPC method calls from the server."""
        match method:
            case "onMessage":
                return await self._on_message(args[0])
            case _:
                raise RpcError.not_found(f"Method '{method}' not found")

    async def get_property(self, name: str) -> Any:
        """Handle property access."""
        raise RpcError.not_found(f"Property '{name}' not found")

    async def _on_message(self, message: dict[str, str]) -> None:
        """Receive a message from the server."""
        msg_type = message.get("type", "chat")
        username = message.get("username", "Unknown")
        text = message.get("text", "")

        if msg_type == "system":
            # System messages in yellow
            print(f"\033[93m*** {text} ***\033[0m")
        elif username == self.username:
            # Own messages in gray
            print(f"\033[90m[You] {text}\033[0m")
        else:
            # Other users' messages in default color
            print(f"[{username}] {text}")


async def read_input() -> str:
    """Read a line of input asynchronously."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, sys.stdin.readline)


async def main() -> None:
    """Run the interactive chat client."""
    print("=== Cap'n Web Chat Client ===")
    print()

    # Get username
    print("Enter your username: ", end="", flush=True)
    username = (await read_input()).strip()

    if not username:
        print("Username cannot be empty!")
        return

    print(f"\nConnecting to chat server as '{username}'...")

    try:
        # Connect via WebSocket using WebSocketClientTransport
        transport = WebSocketClientTransport("ws://127.0.0.1:8080/rpc/ws")
        await transport.connect()
        
        client_callback = ChatClient(username)
        session = BidirectionalSession(transport, client_callback)
        session.start()

        try:
            # Get server stub (wrap ImportHook in RpcStub for ergonomic API)
            server = RpcStub(session.get_main_stub())

            # Create a stub for our callback capability (public API)
            callback_stub = create_stub(client_callback)

            # Join the chat room
            try:
                welcome_data = await asyncio.wait_for(
                    server.join(username, callback_stub), timeout=5.0
                )

                print(f"\n\033[92m{welcome_data['message']}\033[0m")
                print(f"Connected users ({welcome_data['userCount']}): {', '.join(welcome_data['users'])}")
                print("\nType messages and press Enter to send.")
                print("Type '/quit' to exit, '/users' to list users, '/help' for commands")
                print()

            except RpcError as e:
                print(f"\n\033[91mError joining chat: {e.message}\033[0m")
                return

            # Main chat loop
            try:
                while True:
                    # Read user input
                    message = (await read_input()).strip()

                    if not message:
                        continue

                    # Handle commands
                    if message == "/quit":
                        print("\nLeaving chat...")
                        await server.leave(username)
                        break

                    if message == "/users":
                        users = await server.listUsers()
                        print(f"\n\033[96mConnected users: {', '.join(users)}\033[0m\n")

                    elif message == "/help":
                        print("\n\033[96mCommands:")
                        print("  /users  - List connected users")
                        print("  /quit   - Leave the chat")
                        print("  /help   - Show this help\033[0m\n")

                    elif message.startswith("/"):
                        print(f"\n\033[91mUnknown command: {message}\033[0m")
                        print("Type /help for available commands\n")

                    else:
                        # Send regular message
                        try:
                            await server.sendMessage(username, message)
                        except RpcError as e:
                            print(f"\n\033[91mError sending message: {e.message}\033[0m\n")

            except KeyboardInterrupt:
                print("\n\nLeaving chat...")
                with suppress(Exception):
                    await server.leave(username)

        finally:
            await session.stop()
            await transport.close()

    except Exception as e:
        print(f"\n\033[91mConnection error: {e}\033[0m")
        print("Make sure the server is running: uv run python examples/chat/server.py")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nGoodbye!")
