#!/usr/bin/env python3
"""
L-Game WebSocket Relay Server
- Players create or join rooms with a 4-letter code
- All messages are forwarded to the other player in the room
- Deploy on Railway, Fly.io, or any VPS with Python 3.10+
- Install: pip install websockets
- Run:     python relay_server.py
"""

import asyncio
import json
import random
import string
import websockets
from websockets.server import WebSocketServerProtocol

rooms: dict[str, list[WebSocketServerProtocol]] = {}

def make_code() -> str:
    while True:
        code = ''.join(random.choices(string.ascii_uppercase, k=4))
        if code not in rooms:
            return code

async def handler(ws: WebSocketServerProtocol):
    code = None
    role = None  # "host" or "guest"

    try:
        # First message must be {"action": "host"} or {"action": "join", "code": "XXXX"}
        raw = await asyncio.wait_for(ws.recv(), timeout=15)
        msg = json.loads(raw)

        if msg.get("action") == "host":
            code = make_code()
            rooms[code] = [ws]
            role = "host"
            await ws.send(json.dumps({"type": "room_created", "code": code}))
            print(f"[+] Room {code} created")

        elif msg.get("action") == "join":
            code = msg.get("code", "").upper().strip()
            if code not in rooms or len(rooms[code]) != 1:
                await ws.send(json.dumps({"type": "error", "msg": "Room not found or full"}))
                return
            rooms[code].append(ws)
            role = "guest"
            host_ws = rooms[code][0]
            await ws.send(json.dumps({"type": "joined", "code": code}))
            await host_ws.send(json.dumps({"type": "opponent_joined"}))
            print(f"[+] Room {code} full — game starting")

        else:
            await ws.send(json.dumps({"type": "error", "msg": "Send host or join first"}))
            return

        # Relay loop — forward every message to the other player
        async for raw in ws:
            if code not in rooms:
                break
            peers = rooms[code]
            other = next((p for p in peers if p is not ws), None)
            if other and other.open:
                await other.send(raw)

    except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed, json.JSONDecodeError):
        pass
    finally:
        # Clean up room
        if code and code in rooms:
            rooms[code] = [p for p in rooms[code] if p is not ws]
            if not rooms[code]:
                del rooms[code]
                print(f"[-] Room {code} closed")
            else:
                # Notify remaining player
                remaining = rooms[code][0]
                if remaining.open:
                    await remaining.send(json.dumps({"type": "opponent_left"}))
                    print(f"[-] Player left room {code}")

async def main():
    import os
    port = int(os.environ.get("PORT", 8765))
    print(f"Relay server listening on ws://0.0.0.0:{port}")
    async with websockets.serve(handler, "0.0.0.0", port):
        await asyncio.Future()  # run forever

if __name__ == "__main__":
    asyncio.run(main())
