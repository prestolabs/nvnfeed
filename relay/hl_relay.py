#!/usr/bin/env python3
"""
hl_relay.py — Zero-dependency asyncio server that tails hl-node data files
via inotify and streams raw JSON lines to connected clients over TCP or WebSocket.

Two channels:
  D = node_raw_book_diffs_by_block  (L2 book diffs)
  F = node_fills_by_block           (fill pairs / trades)

Wire protocol (line-delimited, same over TCP and WebSocket):
  DB <seq> <relay_ns> <json>  — diff, batch-by-block format (one JSON per block)
  DL <seq> <relay_ns> <json>  — diff, line format (one JSON per event)
  FB <seq> <relay_ns> <json>  — fills, batch-by-block format (one JSON per block)
  FL <seq> <relay_ns> <json>  — fills, line format (one JSON per event)
  C 0 <json>                  — control message

  <relay_ns> = epoch nanoseconds when the relay dispatched the message

  The second character indicates the data format:
    B = batch-by-block (hl-node --batch-by-block): each JSON line wraps all
        events in one block with {block_number, block_time, local_time, events:[...]}
    L = line/single-event: each JSON line is an individual event without
        the block-level wrapper

Subscription is REQUIRED before data is sent. Clients have 10 seconds to subscribe.

TCP clients send a JSON line:
  {"coins":["BTC","ETH"],"compress":true}
  {} or {"coins":[]} → all coins, no filtering

WebSocket clients send a text frame after handshake:
  {"coins":["BTC","ETH"]}
  (connect via: wsdump ws://host:ws-port/ws)
"""

import argparse
import asyncio
import base64
import ctypes
import ctypes.util
import hashlib
import json
import logging
import os
import struct
import sys
import time
import zlib
from datetime import datetime, timezone

log = logging.getLogger("hl_relay")

# ── inotify via ctypes (no pip dependencies) ─────────────────────────────────

libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)
libc.inotify_init.restype = ctypes.c_int
libc.inotify_add_watch.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_uint32]
libc.inotify_add_watch.restype = ctypes.c_int

IN_MODIFY = 0x00000002
IN_CREATE = 0x00000100
INOTIFY_EVENT_SIZE = struct.calcsize("iIII")

# ── FileTailer ───────────────────────────────────────────────────────────────

class FileTailer:
    """Tails a file, reopening on hour rotation.
    Buffers incomplete lines to avoid dispatching partial JSON."""

    def __init__(self, path: str):
        self.path = path
        self.file = None
        self._buf = ""  # incomplete trailing line from previous read
        self._open_at_end()

    def _open_at_end(self):
        if os.path.exists(self.path):
            self.file = open(self.path, "r")
            self.file.seek(0, 2)

    def read_new_lines(self) -> list[str]:
        lines = []
        if self.file is None:
            self._open_at_end()
            return lines
        try:
            data = self.file.read()
        except (IOError, ValueError):
            self.file = None
            return lines
        if not data:
            return lines
        data = self._buf + data
        # Only dispatch lines terminated by a newline.
        # If the data doesn't end with \n, the last chunk is incomplete —
        # carry it over to the next read.
        if data.endswith("\n"):
            self._buf = ""
        else:
            last_nl = data.rfind("\n")
            if last_nl == -1:
                # No complete line at all — buffer everything
                self._buf = data
                return lines
            self._buf = data[last_nl + 1:]
            data = data[:last_nl + 1]
        for line in data.splitlines():
            stripped = line.strip()
            if stripped:
                lines.append(stripped)
        return lines

    def reopen(self, path: str):
        if self.file:
            self.file.close()
        self.path = path
        self._buf = ""
        self.file = open(path, "r")
        self.file.seek(0, 2)

    def close(self):
        if self.file:
            self.file.close()
            self.file = None
        self._buf = ""

# ── WebSocket protocol (RFC 6455 + RFC 7692 permessage-deflate) ──────────────

WS_MAGIC = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
WS_OP_TEXT = 0x1
WS_OP_CLOSE = 0x8
WS_OP_PING = 0x9
WS_OP_PONG = 0xA
WS_RSV1 = 0x40  # RSV1 bit = compressed frame (permessage-deflate)
_DEFLATE_TAIL = b"\x00\x00\xff\xff"


def _ws_accept_key(key: str) -> str:
    digest = hashlib.sha1(key.encode() + WS_MAGIC).digest()
    return base64.b64encode(digest).decode()


class WsDeflate:
    """Per-connection permessage-deflate state (RFC 7692).

    With context takeover (default): compressor/decompressor are reused across
    messages, so the sliding window carries over for better compression.
    """

    def __init__(self, server_no_context: bool = False,
                 client_no_context: bool = False,
                 server_max_window: int = 15,
                 client_max_window: int = 15):
        self.server_no_context = server_no_context
        self.client_no_context = client_no_context
        self.server_max_window = server_max_window
        self.client_max_window = client_max_window
        self._compressor = None if server_no_context else \
            zlib.compressobj(zlib.Z_DEFAULT_COMPRESSION,
                             zlib.DEFLATED, -server_max_window)
        self._decompressor = None if client_no_context else \
            zlib.decompressobj(-client_max_window)

    def compress(self, data: bytes) -> bytes:
        if self.server_no_context:
            c = zlib.compressobj(zlib.Z_DEFAULT_COMPRESSION,
                                 zlib.DEFLATED, -self.server_max_window)
        else:
            c = self._compressor
        out = c.compress(data) + c.flush(zlib.Z_SYNC_FLUSH)
        # Strip trailing 0x00 0x00 0xff 0xff per RFC 7692
        if out.endswith(_DEFLATE_TAIL):
            out = out[:-4]
        return out

    def decompress(self, data: bytes) -> bytes:
        if self.client_no_context:
            d = zlib.decompressobj(-self.client_max_window)
        else:
            d = self._decompressor
        return d.decompress(data + _DEFLATE_TAIL)


def _parse_deflate_params(ext_header: str) -> dict | None:
    """Parse Sec-WebSocket-Extensions header for permessage-deflate.
    Returns param dict if found, None otherwise."""
    for ext in ext_header.split(","):
        ext = ext.strip()
        if not ext.startswith("permessage-deflate"):
            continue
        params = {}
        parts = ext.split(";")
        for part in parts[1:]:
            part = part.strip()
            if "=" in part:
                k, v = part.split("=", 1)
                params[k.strip()] = v.strip()
            else:
                params[part] = True
        return params
    return None


def ws_encode_frame(payload: bytes, opcode: int = WS_OP_TEXT,
                    deflate: "WsDeflate | None" = None) -> bytes:
    """Encode a WebSocket frame (server→client, unmasked).
    If deflate is provided and opcode is TEXT, compress and set RSV1."""
    rsv1 = 0
    if deflate and opcode == WS_OP_TEXT:
        payload = deflate.compress(payload)
        rsv1 = WS_RSV1
    header = bytearray()
    header.append(0x80 | rsv1 | opcode)  # FIN=1
    length = len(payload)
    if length < 126:
        header.append(length)
    elif length < 65536:
        header.append(126)
        header.extend(struct.pack(">H", length))
    else:
        header.append(127)
        header.extend(struct.pack(">Q", length))
    return bytes(header) + payload


async def ws_read_frame(reader: asyncio.StreamReader,
                        deflate: "WsDeflate | None" = None
                        ) -> tuple[int, bytes]:
    """Read one WebSocket frame (client→server, masked).
    If RSV1 set and deflate provided, decompress payload."""
    header = await reader.readexactly(2)
    opcode = header[0] & 0x0F
    rsv1 = bool(header[0] & WS_RSV1)
    masked = bool(header[1] & 0x80)
    length = header[1] & 0x7F
    if length == 126:
        length = struct.unpack(">H", await reader.readexactly(2))[0]
    elif length == 127:
        length = struct.unpack(">Q", await reader.readexactly(8))[0]
    mask_key = await reader.readexactly(4) if masked else None
    payload = await reader.readexactly(length)
    if mask_key:
        payload = bytes(b ^ mask_key[i % 4] for i, b in enumerate(payload))
    if rsv1 and deflate:
        payload = deflate.decompress(payload)
    return opcode, payload


async def ws_handshake(reader: asyncio.StreamReader,
                       writer: asyncio.StreamWriter
                       ) -> tuple[bool, WsDeflate | None]:
    """Read HTTP upgrade headers and complete WebSocket handshake.
    Returns (success, deflate_state_or_None)."""
    headers: dict[str, str] = {}
    while True:
        line = await asyncio.wait_for(reader.readline(), timeout=5.0)
        text = line.decode().strip()
        if not text:
            break
        if ":" in text:
            key, val = text.split(":", 1)
            headers[key.strip().lower()] = val.strip()

    ws_key = headers.get("sec-websocket-key")
    if not ws_key:
        writer.write(b"HTTP/1.1 400 Bad Request\r\n\r\n")
        await writer.drain()
        return False, None

    accept = _ws_accept_key(ws_key)

    # Check for permessage-deflate extension
    deflate = None
    ext_resp = ""
    ext_header = headers.get("sec-websocket-extensions", "")
    if ext_header:
        params = _parse_deflate_params(ext_header)
        if params is not None:
            server_no_ctx = "server_no_context_takeover" in params
            client_no_ctx = "client_no_context_takeover" in params
            # window bits: value may be int, or True (bare param = use default 15)
            smw = params.get("server_max_window_bits", 15)
            server_max = int(smw) if smw is not True else 15
            cmw = params.get("client_max_window_bits", 15)
            client_max = int(cmw) if cmw is not True else 15
            deflate = WsDeflate(server_no_ctx, client_no_ctx,
                                server_max, client_max)
            # Echo back negotiated params
            ext_parts = ["permessage-deflate"]
            if server_no_ctx:
                ext_parts.append("server_no_context_takeover")
            if client_no_ctx:
                ext_parts.append("client_no_context_takeover")
            if server_max != 15:
                ext_parts.append(f"server_max_window_bits={server_max}")
            if client_max != 15:
                ext_parts.append(f"client_max_window_bits={client_max}")
            ext_resp = f"Sec-WebSocket-Extensions: {'; '.join(ext_parts)}\r\n"

    resp = (
        "HTTP/1.1 101 Switching Protocols\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        f"Sec-WebSocket-Accept: {accept}\r\n"
        f"{ext_resp}"
        "\r\n"
    )
    writer.write(resp.encode())
    await writer.drain()
    return True, deflate


# ── Coin filtering ───────────────────────────────────────────────────────────

def filter_diffs_line(raw: str, coins: set[str]) -> str | None:
    """Filter a book-diffs JSON line to only include events for given coins.
    Returns filtered JSON string, or None if no matching events."""
    # Quick reject: check if any coin name appears in the raw string
    if not any(f'"{c}"' in raw for c in coins):
        return None
    batch = json.loads(raw)
    events = batch.get("events", [])
    filtered = [e for e in events if e.get("coin") in coins]
    if not filtered:
        return None
    batch["events"] = filtered
    return json.dumps(batch, separators=(",", ":"))


def filter_fills_line(raw: str, coins: set[str]) -> str | None:
    """Filter a fills JSON line to only include fill pairs for given coins.
    Fill events come in pairs (buyer + seller); keep pairs together."""
    if not any(f'"{c}"' in raw for c in coins):
        return None
    batch = json.loads(raw)
    events = batch.get("events", [])
    filtered = []
    # Events are paired: [addr, fill_obj] at i, [addr, fill_obj] at i+1
    i = 0
    while i + 1 < len(events):
        pair_a = events[i]
        pair_b = events[i + 1]
        # Each element is [address, fill_obj]
        coin = pair_a[1].get("coin") if len(pair_a) >= 2 else None
        if coin in coins:
            filtered.append(pair_a)
            filtered.append(pair_b)
        i += 2
    if not filtered:
        return None
    batch["events"] = filtered
    return json.dumps(batch, separators=(",", ":"))

# ── Client state ─────────────────────────────────────────────────────────────

class ClientState:
    __slots__ = ("addr", "queue", "coins", "compress", "compressor",
                 "consecutive_drops", "writer", "active", "is_ws", "ws_deflate")

    def __init__(self, addr: str, writer: asyncio.StreamWriter,
                 coins: set[str] | None = None, compress: bool = False,
                 is_ws: bool = False, ws_deflate: WsDeflate | None = None):
        self.addr = addr
        self.writer = writer
        self.queue: asyncio.Queue[bytes | None] = asyncio.Queue(maxsize=2000)
        self.coins = coins  # None = all coins (unfiltered)
        self.compress = compress
        self.compressor = None
        if compress:
            self.compressor = zlib.compressobj(level=1, wbits=31)
        self.consecutive_drops = 0
        self.active = True
        self.is_ws = is_ws
        self.ws_deflate = ws_deflate  # permessage-deflate state (WS only)

# ── Dispatcher ───────────────────────────────────────────────────────────────

def _detect_batch_format(raw_line: str) -> str:
    """Detect whether a JSON line is batch-by-block ('B') or single-event ('L').
    Batch format has a "block_number" key; single-event format does not."""
    # Fast path: check for the key without full JSON parse
    return "B" if '"block_number"' in raw_line else "L"


class Dispatcher:
    def __init__(self):
        self.seq: int = 0
        self.clients: set[ClientState] = set()

    def next_seq(self) -> int:
        self.seq += 1
        return self.seq

    def dispatch(self, channel: str, raw_line: str):
        """Assign a sequence number and broadcast to all clients."""
        seq = self.next_seq()
        fmt = _detect_batch_format(raw_line)
        relay_ns = time.time_ns()
        prefix = f"{channel}{fmt} {seq} {relay_ns} "

        # Pre-build the unfiltered message (most clients will use this)
        unfiltered_msg = (prefix + raw_line + "\n").encode()

        to_remove = []
        for client in self.clients:
            if not client.active:
                continue

            msg = None
            if client.coins is None:
                # Unfiltered: zero-parse relay
                msg = unfiltered_msg
            else:
                # Filtered: need to parse and filter
                try:
                    if channel == "D":
                        filtered = filter_diffs_line(raw_line, client.coins)
                    else:
                        filtered = filter_fills_line(raw_line, client.coins)
                except (json.JSONDecodeError, KeyError, IndexError, TypeError) as e:
                    log.warning("Bad JSON line (seq=%d, len=%d): %s", seq, len(raw_line), e)
                    continue
                if filtered is None:
                    # No matching events — skip this message for this client
                    # (seq gap is expected and fine)
                    continue
                msg = (prefix + filtered + "\n").encode()

            if client.compress and client.compressor:
                msg = client.compressor.compress(msg)
                msg += client.compressor.flush(zlib.Z_SYNC_FLUSH)

            try:
                client.queue.put_nowait(msg)
                client.consecutive_drops = 0
            except asyncio.QueueFull:
                client.consecutive_drops += 1
                if client.consecutive_drops > 500:
                    log.warning("Disconnecting hopeless slow consumer: %s "
                                "(>500 consecutive drops)", client.addr)
                    client.active = False
                    # Sentinel to wake writer task
                    try:
                        client.queue.put_nowait(None)
                    except asyncio.QueueFull:
                        pass
                    to_remove.append(client)

        for client in to_remove:
            self.clients.discard(client)

# ── Path helpers ─────────────────────────────────────────────────────────────

def get_current_hour_path(base_dir: str) -> str:
    now = datetime.now(timezone.utc)
    return os.path.join(base_dir, "hourly", now.strftime("%Y%m%d"), str(now.hour))


def get_current_date_dir(base_dir: str) -> str:
    now = datetime.now(timezone.utc)
    return os.path.join(base_dir, "hourly", now.strftime("%Y%m%d"))


def get_hourly_base(base_dir: str) -> str:
    return os.path.join(base_dir, "hourly")

# ── TCP server ───────────────────────────────────────────────────────────────

async def client_writer(client: ClientState):
    """Drain the client queue and write to the socket."""
    try:
        while client.active:
            msg = await client.queue.get()
            if msg is None:
                break
            if client.is_ws:
                client.writer.write(
                    ws_encode_frame(msg, deflate=client.ws_deflate))
            else:
                client.writer.write(msg)
            await client.writer.drain()
    except (ConnectionError, OSError):
        pass
    finally:
        client.active = False
        try:
            if client.is_ws:
                try:
                    client.writer.write(ws_encode_frame(b"", WS_OP_CLOSE))
                    await client.writer.drain()
                except (ConnectionError, OSError):
                    pass
            client.writer.close()
            await client.writer.wait_closed()
        except (ConnectionError, OSError):
            pass


async def _send_ctrl(writer: asyncio.StreamWriter, msg: dict,
                     client: ClientState | None = None):
    """Send a control message. If client has compression, compress it."""
    raw = f"C 0 {json.dumps(msg, separators=(',', ':'))}\n".encode()
    if client and client.compress and client.compressor:
        raw = client.compressor.compress(raw)
        raw += client.compressor.flush(zlib.Z_SYNC_FLUSH)
    if client and client.is_ws:
        writer.write(ws_encode_frame(raw, deflate=client.ws_deflate))
    else:
        writer.write(raw)
    await writer.drain()


async def handle_client(reader: asyncio.StreamReader,
                        writer: asyncio.StreamWriter,
                        dispatcher: Dispatcher):
    addr = writer.get_extra_info("peername")
    addr_str = f"{addr[0]}:{addr[1]}" if addr else "unknown"
    log.info("TCP client connected: %s", addr_str)

    # Subscription is REQUIRED. Wait up to 10 seconds.
    coins = None
    compress = False
    try:
        line = await asyncio.wait_for(reader.readline(), timeout=10.0)
        if not line or not line.strip():
            raise ValueError("empty subscription")
        sub = json.loads(line.decode().strip())
        if "coins" in sub and isinstance(sub["coins"], list) and sub["coins"]:
            coins = set(sub["coins"])
        compress = bool(sub.get("compress"))
    except asyncio.TimeoutError:
        log.warning("TCP client %s: no subscription within 10s, disconnecting", addr_str)
        try:
            err = f'C 0 {{"status":"error","msg":"subscription required within 10s"}}\n'
            writer.write(err.encode())
            await writer.drain()
        except (ConnectionError, OSError):
            pass
        writer.close()
        return
    except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as e:
        log.warning("TCP client %s: invalid subscription: %s", addr_str, e)
        try:
            err = f'C 0 {{"status":"error","msg":"invalid subscription JSON"}}\n'
            writer.write(err.encode())
            await writer.drain()
        except (ConnectionError, OSError):
            pass
        writer.close()
        return

    client = ClientState(addr_str, writer, coins, compress)
    dispatcher.clients.add(client)

    # Send subscription confirmation
    status = {
        "status": "subscribed",
        "coins": sorted(coins) if coins else "all",
        "compress": compress,
    }
    try:
        await _send_ctrl(writer, status, client)
    except (ConnectionError, OSError):
        dispatcher.clients.discard(client)
        return

    log.info("TCP client %s subscribed: coins=%s compress=%s",
             addr_str, coins or "all", compress)

    # Run the writer task; it exits when client disconnects or is kicked
    try:
        await client_writer(client)
    finally:
        dispatcher.clients.discard(client)
        log.info("Client disconnected: %s", addr_str)

# ── WebSocket server ─────────────────────────────────────────────────────────

async def _ws_reader_loop(reader: asyncio.StreamReader,
                          writer: asyncio.StreamWriter,
                          client: ClientState):
    """Handle incoming WebSocket frames (ping/pong, close)."""
    try:
        while client.active:
            opcode, payload = await ws_read_frame(reader, client.ws_deflate)
            if opcode == WS_OP_PING:
                writer.write(ws_encode_frame(payload, WS_OP_PONG))
                await writer.drain()
            elif opcode == WS_OP_CLOSE:
                writer.write(ws_encode_frame(b"", WS_OP_CLOSE))
                await writer.drain()
                break
    except (asyncio.IncompleteReadError, ConnectionError, OSError):
        pass
    finally:
        client.active = False
        try:
            client.queue.put_nowait(None)
        except asyncio.QueueFull:
            pass


async def handle_ws_client(reader: asyncio.StreamReader,
                           writer: asyncio.StreamWriter,
                           dispatcher: Dispatcher):
    """Handle a WebSocket connection (called after TCP accept on ws-port)."""
    addr = writer.get_extra_info("peername")
    addr_str = f"{addr[0]}:{addr[1]}" if addr else "unknown"

    # Read the HTTP request line
    try:
        request_line = await asyncio.wait_for(reader.readline(), timeout=5.0)
    except asyncio.TimeoutError:
        writer.close()
        return
    if not request_line.startswith(b"GET "):
        writer.write(b"HTTP/1.1 400 Bad Request\r\n\r\n")
        await writer.drain()
        writer.close()
        return

    # Complete WebSocket handshake (reads remaining headers, negotiates deflate)
    try:
        ok, ws_deflate = await ws_handshake(reader, writer)
        if not ok:
            writer.close()
            return
    except (asyncio.TimeoutError, ConnectionError, OSError):
        writer.close()
        return

    deflate_str = "on" if ws_deflate else "off"
    log.info("WS client connected: %s (permessage-deflate: %s)", addr_str, deflate_str)

    # Subscription is REQUIRED. Wait up to 10 seconds for a text frame.
    coins = None
    try:
        opcode, payload = await asyncio.wait_for(
            ws_read_frame(reader, ws_deflate), timeout=10.0)
        if opcode == WS_OP_TEXT:
            sub = json.loads(payload.decode())
            if "coins" in sub and isinstance(sub["coins"], list) and sub["coins"]:
                coins = set(sub["coins"])
        elif opcode == WS_OP_CLOSE:
            writer.write(ws_encode_frame(b"", WS_OP_CLOSE))
            await writer.drain()
            writer.close()
            return
        else:
            raise ValueError("expected text frame with subscription")
    except asyncio.TimeoutError:
        log.warning("WS client %s: no subscription within 10s, disconnecting", addr_str)
        err = json.dumps({"status": "error", "msg": "subscription required within 10s"})
        writer.write(ws_encode_frame(f"C 0 {err}\n".encode(), deflate=ws_deflate))
        await writer.drain()
        writer.write(ws_encode_frame(b"", WS_OP_CLOSE))
        await writer.drain()
        writer.close()
        return
    except (json.JSONDecodeError, UnicodeDecodeError, ValueError,
            asyncio.IncompleteReadError) as e:
        log.warning("WS client %s: invalid subscription: %s", addr_str, e)
        try:
            err = json.dumps({"status": "error", "msg": f"invalid subscription: {e}"})
            writer.write(ws_encode_frame(f"C 0 {err}\n".encode(), deflate=ws_deflate))
            await writer.drain()
            writer.write(ws_encode_frame(b"", WS_OP_CLOSE))
            await writer.drain()
        except (ConnectionError, OSError):
            pass
        writer.close()
        return

    client = ClientState(addr_str, writer, coins, compress=False,
                         is_ws=True, ws_deflate=ws_deflate)
    dispatcher.clients.add(client)

    # Send subscription confirmation as WS text frame
    status = {
        "status": "subscribed",
        "coins": sorted(coins) if coins else "all",
        "compress": deflate_str,
    }
    try:
        await _send_ctrl(writer, status, client)
    except (ConnectionError, OSError):
        dispatcher.clients.discard(client)
        return

    log.info("WS client %s subscribed: coins=%s deflate=%s",
             addr_str, coins or "all", deflate_str)

    # Run reader (ping/pong/close) and writer concurrently
    reader_task = asyncio.create_task(_ws_reader_loop(reader, writer, client))
    try:
        await client_writer(client)
    finally:
        reader_task.cancel()
        dispatcher.clients.discard(client)
        log.info("WS client disconnected: %s", addr_str)


# ── inotify watcher (asyncio integration) ────────────────────────────────────

class InotifyWatcher:
    """Watches data directories via inotify, integrated with asyncio event loop."""

    def __init__(self, data_dir: str, dispatcher: Dispatcher):
        self.data_dir = data_dir
        self.dispatcher = dispatcher

        self.book_base = os.path.join(data_dir, "node_raw_book_diffs_by_block")
        self.fills_base = os.path.join(data_dir, "node_fills_by_block")

        self.ifd = libc.inotify_init()
        if self.ifd < 0:
            raise RuntimeError("Failed to initialize inotify")

        # wd -> (source_type, path)
        self.watch_map: dict[int, tuple[str, str]] = {}
        self.tailers: dict[str, FileTailer] = {}  # "book" / "fills"

    def _add_watch(self, path: str, source_type: str, mask: int = IN_MODIFY | IN_CREATE):
        if os.path.exists(path):
            wd = libc.inotify_add_watch(self.ifd, path.encode(), mask)
            if wd >= 0:
                self.watch_map[wd] = (source_type, path)
                return wd
        return -1

    def setup_watches(self):
        """Set up initial watches for current date dirs, hour files, and hourly base dirs."""
        for base, src_type in [(self.book_base, "book"), (self.fills_base, "fills")]:
            # Watch hourly base dir for new date dirs (midnight rotation)
            hourly_base = get_hourly_base(base)
            os.makedirs(hourly_base, exist_ok=True)
            self._add_watch(hourly_base, f"{src_type}_hourly_base", IN_CREATE)

            # Watch current date dir for new hour files
            date_dir = get_current_date_dir(base)
            os.makedirs(date_dir, exist_ok=True)
            self._add_watch(date_dir, f"{src_type}_date_dir", IN_CREATE)

            # Set up tailer for current hour file
            hour_path = get_current_hour_path(base)
            if os.path.exists(hour_path):
                self._add_watch(hour_path, src_type, IN_MODIFY)
                self.tailers[src_type] = FileTailer(hour_path)
                log.info("Tailing %s: %s", src_type, hour_path)
            else:
                log.info("Waiting for %s file: %s", src_type, hour_path)

    def _process_inotify_events(self):
        """Read and process all pending inotify events."""
        try:
            buf = os.read(self.ifd, 65536)
        except OSError:
            return

        offset = 0
        while offset < len(buf):
            wd, mask, cookie, name_len = struct.unpack_from("iIII", buf, offset)
            offset += INOTIFY_EVENT_SIZE
            name = buf[offset:offset + name_len].rstrip(b"\x00").decode()
            offset += name_len

            if wd not in self.watch_map:
                continue

            src_type, path = self.watch_map[wd]

            if src_type.endswith("_hourly_base") and (mask & IN_CREATE):
                # New date directory created (midnight)
                base_type = src_type.replace("_hourly_base", "")
                new_date_dir = os.path.join(path, name)
                self._add_watch(new_date_dir, f"{base_type}_date_dir", IN_CREATE)
                log.info("New date dir: %s", new_date_dir)

            elif src_type.endswith("_date_dir") and (mask & IN_CREATE):
                # New hour file created
                base_type = src_type.replace("_date_dir", "")
                new_hour_path = os.path.join(path, name)
                self._add_watch(new_hour_path, base_type, IN_MODIFY)
                # Close old tailer, open new one from the beginning
                if base_type in self.tailers:
                    self.tailers[base_type].close()
                self.tailers[base_type] = FileTailer(new_hour_path)
                log.info("New hour file for %s: %s", base_type, new_hour_path)

            elif src_type in ("book", "fills") and (mask & IN_MODIFY):
                self._read_and_dispatch(src_type)

    def _read_and_dispatch(self, src_type: str):
        """Read new lines from a tailer and dispatch them."""
        if src_type not in self.tailers:
            return
        channel = "D" if src_type == "book" else "F"
        lines = self.tailers[src_type].read_new_lines()
        for line in lines:
            self.dispatcher.dispatch(channel, line)

    async def run(self, loop: asyncio.AbstractEventLoop):
        """Register inotify fd with asyncio and process events."""
        ready = asyncio.Event()

        def on_readable():
            self._process_inotify_events()
            # Also poll tailers to catch any data not triggered by inotify
            for src_type in list(self.tailers):
                self._read_and_dispatch(src_type)

        loop.add_reader(self.ifd, on_readable)
        log.info("inotify watcher registered with asyncio event loop")

        try:
            # Periodically check for hour/date rotation and poll tailers
            while True:
                await asyncio.sleep(1.0)
                # Poll tailers as a safety net
                for src_type in list(self.tailers):
                    self._read_and_dispatch(src_type)
                # Check if we need to watch new date/hour dirs
                self._check_rotation()
        finally:
            loop.remove_reader(self.ifd)
            os.close(self.ifd)
            for tailer in self.tailers.values():
                tailer.close()

    def _check_rotation(self):
        """Check if date dir or hour file has changed and update watches."""
        for base, src_type in [(self.book_base, "book"), (self.fills_base, "fills")]:
            # Check for new date dir
            date_dir = get_current_date_dir(base)
            date_key = f"{src_type}_date_dir"
            if not any(v[0] == date_key and v[1] == date_dir
                       for v in self.watch_map.values()):
                if os.path.isdir(date_dir):
                    self._add_watch(date_dir, date_key, IN_CREATE)
                    log.info("Rotation: watching new date dir %s", date_dir)

            # Check for new hour file
            hour_path = get_current_hour_path(base)
            if src_type in self.tailers and self.tailers[src_type].path != hour_path:
                if os.path.exists(hour_path):
                    self._add_watch(hour_path, src_type, IN_MODIFY)
                    self.tailers[src_type].close()
                    self.tailers[src_type] = FileTailer(hour_path)
                    log.info("Rotation: new hour file for %s: %s", src_type, hour_path)
            elif src_type not in self.tailers and os.path.exists(hour_path):
                self._add_watch(hour_path, src_type, IN_MODIFY)
                self.tailers[src_type] = FileTailer(hour_path)
                log.info("First hour file for %s: %s", src_type, hour_path)

# ── Main ─────────────────────────────────────────────────────────────────────

async def async_main(args):
    dispatcher = Dispatcher()
    watcher = InotifyWatcher(args.data_dir, dispatcher)
    watcher.setup_watches()

    loop = asyncio.get_running_loop()
    tasks = []

    # Start TCP server
    tcp_server = await asyncio.start_server(
        lambda r, w: handle_client(r, w, dispatcher),
        host=args.host,
        port=args.port,
    )
    addrs = [s.getsockname() for s in tcp_server.sockets]
    log.info("TCP server listening on %s", addrs)

    # Start WebSocket server (if --ws-port given)
    ws_server = None
    if args.ws_port:
        ws_server = await asyncio.start_server(
            lambda r, w: handle_ws_client(r, w, dispatcher),
            host=args.host,
            port=args.ws_port,
        )
        ws_addrs = [s.getsockname() for s in ws_server.sockets]
        log.info("WebSocket server listening on %s", ws_addrs)

    async with tcp_server:
        watcher_task = asyncio.create_task(watcher.run(loop))
        tcp_task = asyncio.create_task(tcp_server.serve_forever())
        tasks = [watcher_task, tcp_task]

        if ws_server:
            ws_task = asyncio.create_task(ws_server.serve_forever())
            tasks.append(ws_task)

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass
        finally:
            for t in tasks:
                t.cancel()
            if ws_server:
                ws_server.close()


def main():
    parser = argparse.ArgumentParser(
        description="HL Relay: stream raw node data to TCP clients"
    )
    parser.add_argument("--port", type=int, default=8765,
                        help="TCP listen port (default: 8765)")
    parser.add_argument("--ws-port", type=int, default=None,
                        help="WebSocket listen port (e.g. 8766)")
    parser.add_argument("--host", default="0.0.0.0",
                        help="Bind address (default: 0.0.0.0)")
    parser.add_argument("--data-dir", default=os.path.expanduser("~/hl/data"),
                        help="Node data directory (default: ~/hl/data)")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Enable debug logging")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    ws_info = f" ws-port={args.ws_port}" if args.ws_port else ""
    log.info("Starting hl_relay: port=%d%s data_dir=%s",
             args.port, ws_info, args.data_dir)

    try:
        asyncio.run(async_main(args))
    except KeyboardInterrupt:
        log.info("Shutting down")


if __name__ == "__main__":
    main()
