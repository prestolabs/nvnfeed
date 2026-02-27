#!/usr/bin/env python3
"""WebSocket dump tool with optional permessage-deflate compression
and latency measurement.

Usage:
  python3 wsdump_deflate.py ws://host:port/ws '{"coins":["BTC"]}'
  python3 wsdump_deflate.py --latency ws://host:port/ws '{"coins":["BTC"]}'
  python3 wsdump_deflate.py --no-compress ws://host:port/ws '{"coins":["BTC"]}'
"""

import argparse
import asyncio
import base64
import hashlib
import json
import os
import struct
import sys
import time
import zlib
from datetime import datetime, timezone
from urllib.parse import urlparse

WS_MAGIC = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
_DEFLATE_TAIL = b"\x00\x00\xff\xff"


def _iso_to_epoch_ns(iso: str) -> int:
    """Parse ISO 8601 timestamp (from hl-node) to epoch nanoseconds.
    Format: '2026-02-27T06:53:10.122591240' (up to 9 fractional digits, UTC)."""
    # Split at '.' to handle nanosecond fractional part
    if "." in iso:
        base, frac = iso.split(".", 1)
        # Pad or truncate to 9 digits for nanoseconds
        frac = frac[:9].ljust(9, "0")
        dt = datetime.strptime(base, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
        epoch_s = int(dt.timestamp())
        return epoch_s * 1_000_000_000 + int(frac)
    else:
        dt = datetime.strptime(iso, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
        return int(dt.timestamp()) * 1_000_000_000


class LatencyTracker:
    """Tracks latency components and prints periodic stats."""

    def __init__(self, interval: float = 10.0):
        self.interval = interval
        self.node: list[float] = []      # local_time - block_time (ms)
        self.relay: list[float] = []     # relay_ns - local_time (ms)
        self.network: list[float] = []   # client_recv - relay_ns (ms)
        self.e2e: list[float] = []       # client_recv - block_time (ms)
        self._last_report = time.monotonic()

    def record(self, block_time_ns: int, local_time_ns: int,
               relay_ns: int, recv_ns: int, channel: str, block_num: int):
        node_ms = (local_time_ns - block_time_ns) / 1e6
        relay_ms = (relay_ns - local_time_ns) / 1e6
        net_ms = (recv_ns - relay_ns) / 1e6
        e2e_ms = (recv_ns - block_time_ns) / 1e6

        self.node.append(node_ms)
        self.relay.append(relay_ms)
        self.network.append(net_ms)
        self.e2e.append(e2e_ms)

        # Per-message compact line
        print(f"[{channel}] blk={block_num} "
              f"e2e={e2e_ms:7.1f}ms  node={node_ms:7.1f}ms  "
              f"relay={relay_ms:5.1f}ms  net={net_ms:7.1f}ms",
              flush=True)

        now = time.monotonic()
        if now - self._last_report >= self.interval:
            self._print_stats()
            self._last_report = now

    def _pct(self, data: list[float], p: float) -> float:
        if not data:
            return 0.0
        s = sorted(data)
        idx = int(len(s) * p / 100)
        return s[min(idx, len(s) - 1)]

    def _print_stats(self):
        n = len(self.e2e)
        if n == 0:
            return
        print(f"\n--- Latency stats (last {self.interval:.0f}s, {n} msgs) ---",
              file=sys.stderr)
        print(f"{'':>12s}  {'p50':>8s}  {'p90':>8s}  {'p99':>8s}  {'max':>8s}",
              file=sys.stderr)
        for label, data in [("e2e", self.e2e), ("node", self.node),
                            ("relay", self.relay), ("network", self.network)]:
            p50 = self._pct(data, 50)
            p90 = self._pct(data, 90)
            p99 = self._pct(data, 99)
            mx = max(data) if data else 0.0
            print(f"  {label:>10s}: {p50:7.1f}ms {p90:7.1f}ms {p99:7.1f}ms {mx:7.1f}ms",
                  file=sys.stderr)
        print(file=sys.stderr, flush=True)
        # Reset for next window
        self.node.clear()
        self.relay.clear()
        self.network.clear()
        self.e2e.clear()


async def main():
    parser = argparse.ArgumentParser(description="WebSocket dump with optional compression")
    parser.add_argument("ws_url", help="WebSocket URL, e.g. ws://host:port/ws")
    parser.add_argument("subscription", nargs="?", default="{}",
                        help='Subscription JSON (default: "{}")')
    parser.add_argument("--no-compress", action="store_true",
                        help="Disable permessage-deflate compression")
    parser.add_argument("--latency", action="store_true",
                        help="Enable latency measurement (parse relay timestamps)")
    parser.add_argument("--latency-interval", type=float, default=10.0,
                        help="Seconds between aggregate latency stats (default: 10)")
    args = parser.parse_args()

    url = urlparse(args.ws_url)
    sub = args.subscription
    host = url.hostname
    port = url.port or (443 if url.scheme == "wss" else 80)
    path = url.path or "/"
    use_ssl = url.scheme == "wss"
    request_deflate = not args.no_compress

    tracker = LatencyTracker(args.latency_interval) if args.latency else None

    if use_ssl:
        import ssl
        ctx = ssl.create_default_context()
        reader, writer = await asyncio.open_connection(host, port, ssl=ctx)
    else:
        reader, writer = await asyncio.open_connection(host, port)

    # WebSocket upgrade
    key = base64.b64encode(os.urandom(16)).decode()
    ext_header = "Sec-WebSocket-Extensions: permessage-deflate\r\n" if request_deflate else ""
    req = (
        f"GET {path} HTTP/1.1\r\n"
        f"Host: {host}:{port}\r\n"
        f"Upgrade: websocket\r\n"
        f"Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {key}\r\n"
        f"Sec-WebSocket-Version: 13\r\n"
        f"{ext_header}"
        f"\r\n"
    )
    writer.write(req.encode())
    await writer.drain()

    # Read response headers
    resp = b""
    while b"\r\n\r\n" not in resp:
        resp += await reader.read(4096)
    resp_str = resp.decode()
    deflate_on = "permessage-deflate" in resp_str
    accept_key = None
    for line in resp_str.split("\r\n"):
        if line.lower().startswith("sec-websocket-accept:"):
            accept_key = line.split(":", 1)[1].strip()

    # Verify accept key
    expected = base64.b64encode(hashlib.sha1(key.encode() + WS_MAGIC).digest()).decode()
    if accept_key != expected:
        print(f"ERROR: bad accept key: {accept_key} != {expected}", file=sys.stderr)
        sys.exit(1)

    status_line = resp_str.split("\r\n")[0]
    print(f"Connected: {status_line}", file=sys.stderr)
    print(f"Compression: {'permessage-deflate' if deflate_on else 'off'}", file=sys.stderr)
    if tracker:
        print(f"Latency tracking: on (stats every {args.latency_interval:.0f}s)", file=sys.stderr)
        print(f"NOTE: network latency includes clock skew between machines", file=sys.stderr)

    # Set up compressor/decompressor if negotiated
    compressor = zlib.compressobj(zlib.Z_DEFAULT_COMPRESSION, zlib.DEFLATED, -15) if deflate_on else None
    decompressor = zlib.decompressobj(-15) if deflate_on else None

    def encode_frame(data: bytes, opcode: int = 0x1) -> bytes:
        payload = data
        rsv1 = 0
        if compressor and opcode == 0x1:
            payload = compressor.compress(data) + compressor.flush(zlib.Z_SYNC_FLUSH)
            if payload.endswith(_DEFLATE_TAIL):
                payload = payload[:-4]
            rsv1 = 0x40
        mask_key = os.urandom(4)
        masked = bytes(b ^ mask_key[i % 4] for i, b in enumerate(payload))
        header = bytearray()
        header.append(0x80 | rsv1 | opcode)
        length = len(masked)
        if length < 126:
            header.append(0x80 | length)
        elif length < 65536:
            header.append(0x80 | 126)
            header.extend(struct.pack(">H", length))
        else:
            header.append(0x80 | 127)
            header.extend(struct.pack(">Q", length))
        header.extend(mask_key)
        return bytes(header) + masked

    async def read_frame() -> tuple[int, bytes]:
        h = await reader.readexactly(2)
        opcode = h[0] & 0x0F
        rsv1 = bool(h[0] & 0x40)
        length = h[1] & 0x7F
        if length == 126:
            length = struct.unpack(">H", await reader.readexactly(2))[0]
        elif length == 127:
            length = struct.unpack(">Q", await reader.readexactly(8))[0]
        masked = bool(h[1] & 0x80)
        mask_key = await reader.readexactly(4) if masked else None
        payload = await reader.readexactly(length)
        if mask_key:
            payload = bytes(b ^ mask_key[i % 4] for i, b in enumerate(payload))
        if rsv1 and decompressor:
            payload = decompressor.decompress(payload + _DEFLATE_TAIL)
        return opcode, payload

    # Send subscription
    writer.write(encode_frame(sub.encode()))
    await writer.drain()
    print(f"Sent subscription: {sub}", file=sys.stderr)

    # Read and print messages
    try:
        while True:
            opcode, payload = await read_frame()
            recv_ns = time.time_ns()
            if opcode == 0x1:  # text
                text = payload.decode()
                if tracker:
                    _process_latency(text, recv_ns, tracker)
                else:
                    print(text, flush=True)
            elif opcode == 0x8:  # close
                print("Server closed connection", file=sys.stderr)
                break
            elif opcode == 0x9:  # ping
                writer.write(encode_frame(payload, opcode=0xA))
                await writer.drain()
    except (asyncio.IncompleteReadError, ConnectionError, KeyboardInterrupt):
        pass
    finally:
        # Print final stats
        if tracker:
            tracker._print_stats()
        writer.close()


def _process_latency(text: str, recv_ns: int, tracker: LatencyTracker):
    """Parse wire protocol message and record latency.
    Wire format: '<chan><fmt> <seq> <relay_ns> <json>'
    JSON batch format has block_time, local_time fields."""
    # Skip control messages
    if text.startswith("C "):
        print(text, flush=True)
        return

    # Parse prefix: "DB 123 1709012345678901234 {..."
    try:
        parts = text.split(" ", 3)
        if len(parts) < 4:
            print(text, flush=True)
            return
        channel = parts[0]
        relay_ns = int(parts[2])
        json_str = parts[3]
    except (ValueError, IndexError):
        print(text, flush=True)
        return

    # Parse JSON for block_time and local_time (batch format only)
    if channel.endswith("B"):
        try:
            data = json.loads(json_str)
            block_time_iso = data.get("block_time")
            local_time_iso = data.get("local_time")
            block_num = data.get("block_number", 0)
            if block_time_iso and local_time_iso:
                block_time_ns = _iso_to_epoch_ns(block_time_iso)
                local_time_ns = _iso_to_epoch_ns(local_time_iso)
                tracker.record(block_time_ns, local_time_ns,
                               relay_ns, recv_ns, channel, block_num)
                return
        except (json.JSONDecodeError, KeyError, ValueError):
            pass

    # Fallback: can't extract timestamps, just print
    print(text, flush=True)


asyncio.run(main())
