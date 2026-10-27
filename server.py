#!/usr/bin/env python3
"""Local MUD client server: serves the web UI and bridges WebSockets to telnet.

Standard library only. Binds to localhost by default, since it will open a
telnet connection to any host the page asks for.
"""
import argparse
import asyncio
import base64
import codecs
import hashlib
import json
import os
import struct
import webbrowser
from urllib.parse import parse_qs, urlparse

ROOT = os.path.dirname(os.path.abspath(__file__))
WS_GUID = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
MAX_FRAME = 1 << 20

IAC, DONT, DO, WONT, WILL, SB, SE = 255, 254, 253, 252, 251, 250, 240
OPT_ECHO = 1


def ws_frame(opcode, payload=b""):
    n = len(payload)
    head = bytes([0x80 | opcode])
    if n < 126:
        head += bytes([n])
    elif n < 65536:
        head += bytes([126]) + struct.pack(">H", n)
    else:
        head += bytes([127]) + struct.pack(">Q", n)
    return head + payload


async def ws_read_frame(reader):
    b1, b2 = await reader.readexactly(2)
    fin, opcode, n = b1 & 0x80, b1 & 0x0F, b2 & 0x7F
    if n == 126:
        n = struct.unpack(">H", await reader.readexactly(2))[0]
    elif n == 127:
        n = struct.unpack(">Q", await reader.readexactly(8))[0]
    if n > MAX_FRAME:
        raise ValueError("frame too large")
    mask = await reader.readexactly(4) if b2 & 0x80 else None
    data = await reader.readexactly(n)
    if mask:
        data = bytes(b ^ mask[i % 4] for i, b in enumerate(data))
    return fin, opcode, data


class Telnet:
    """Strips telnet negotiation from the stream and refuses every option
    except ECHO, which is reported so the UI can hide password entry."""

    def __init__(self):
        self.state = 0
        self.verb = 0
        self.echo = False
        self.refused = set()

    def feed(self, data):
        text, reply, events = bytearray(), bytearray(), []
        for b in data:
            if self.state == 0:
                if b == IAC:
                    self.state = 1
                else:
                    text.append(b)
            elif self.state == 1:
                if b == IAC:
                    text.append(IAC)
                    self.state = 0
                elif b in (DO, DONT, WILL, WONT):
                    self.verb = b
                    self.state = 2
                elif b == SB:
                    self.state = 3
                else:
                    self.state = 0
            elif self.state == 2:
                self.state = 0
                if self.verb == WILL and b == OPT_ECHO:
                    if not self.echo:
                        self.echo = True
                        reply += bytes([IAC, DO, b])
                        events.append({"t": "echo", "on": True})
                elif self.verb == WONT and b == OPT_ECHO:
                    if self.echo:
                        self.echo = False
                        events.append({"t": "echo", "on": False})
                elif self.verb == WILL and (b, WILL) not in self.refused:
                    self.refused.add((b, WILL))
                    reply += bytes([IAC, DONT, b])
                elif self.verb == DO and (b, DO) not in self.refused:
                    self.refused.add((b, DO))
                    reply += bytes([IAC, WONT, b])
            elif self.state == 3:
                if b == IAC:
                    self.state = 4
            else:
                self.state = 0 if b == SE else 3
        return bytes(text), bytes(reply), events


async def bridge(reader, writer, headers, query):
    key = headers.get("sec-websocket-key", "")
    accept = base64.b64encode(hashlib.sha1(key.encode() + WS_GUID).digest()).decode()
    writer.write(
        ("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n"
         "Connection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n" % accept).encode()
    )

    def send(obj):
        writer.write(ws_frame(1, json.dumps(obj).encode()))

    try:
        host = query["host"][0]
        port = int(query["port"][0])
        if not 0 < port < 65536:
            raise ValueError
        mud_r, mud_w = await asyncio.wait_for(asyncio.open_connection(host, port), 10)
    except (KeyError, ValueError, OSError, asyncio.TimeoutError) as e:
        send({"t": "error", "d": "Could not connect: %s" % (e or "timed out")})
        writer.write(ws_frame(8))
        return

    telnet = Telnet()
    decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")

    async def mud_to_ws():
        while True:
            data = await mud_r.read(4096)
            if not data:
                return
            text, reply, events = telnet.feed(data)
            if reply:
                mud_w.write(reply)
            for ev in events:
                send(ev)
            out = decoder.decode(text)
            if out:
                send({"t": "data", "d": out})
            await writer.drain()

    async def ws_to_mud():
        msg = b""
        while True:
            fin, op, data = await ws_read_frame(reader)
            if op == 8:
                return
            if op == 9:
                writer.write(ws_frame(10, data))
            elif op in (0, 1):
                msg += data
                if fin:
                    try:
                        obj = json.loads(msg)
                    except ValueError:
                        obj = {}
                    msg = b""
                    if obj.get("t") == "send":
                        line = str(obj.get("d", "")).encode("utf-8", "replace")
                        mud_w.write(line.replace(b"\xff", b"\xff\xff") + b"\r\n")
                        await mud_w.drain()

    tasks = [asyncio.ensure_future(mud_to_ws()), asyncio.ensure_future(ws_to_mud())]
    try:
        await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for t in tasks:
            t.cancel()
        mud_w.close()
        try:
            send({"t": "closed"})
            writer.write(ws_frame(8))
        except Exception:
            pass


async def handle(reader, writer):
    try:
        head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 10)
        lines = head.decode("latin-1").split("\r\n")
        _, target, _ = lines[0].split(" ", 2)
        headers = {}
        for line in lines[1:]:
            if ":" in line:
                k, v = line.split(":", 1)
                headers[k.strip().lower()] = v.strip()
        url = urlparse(target)

        if headers.get("upgrade", "").lower() == "websocket" and url.path == "/ws":
            origin = headers.get("origin")
            if origin and urlparse(origin).netloc != headers.get("host"):
                writer.write(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n")
            else:
                await bridge(reader, writer, headers, parse_qs(url.query))
        elif url.path in ("/", "/index.html"):
            with open(os.path.join(ROOT, "index.html"), "rb") as f:
                body = f.read()
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n"
                b"Content-Length: %d\r\nCache-Control: no-store\r\n\r\n" % len(body) + body
            )
        else:
            writer.write(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n")
        await writer.drain()
    except (asyncio.IncompleteReadError, asyncio.TimeoutError, ConnectionError, ValueError):
        pass
    finally:
        writer.close()


async def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--bind", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--no-browser", action="store_true")
    args = ap.parse_args()
    server = await asyncio.start_server(handle, args.bind, args.port)
    url = "http://%s:%d/" % (args.bind, args.port)
    print("MUD client running at", url)
    if not args.no_browser:
        webbrowser.open(url)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
