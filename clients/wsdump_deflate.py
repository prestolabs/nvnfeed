#!/usr/bin/env python3
"""WebSocket dump tool with optional permessage-deflate compression
and latency measurement.

Usage:
  python3 wsdump_deflate.py ws://host:port/ws '{"coins":["BTC"]}'
  python3 wsdump_deflate.py --coins BTC,ETH,SOL ws://host:port/ws
  python3 wsdump_deflate.py --coins BTC --channels book ws://host:port/ws
  python3 wsdump_deflate.py --latency --coins BTC,ETH ws://host:port/ws
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
        # Window (reset each interval) — batch format (B)
        self.node: list[float] = []
        self.relay: list[float] = []
        self.network: list[float] = []
        self.e2e: list[float] = []
        # Accumulated (never reset) — batch format (B)
        self.all_node: list[float] = []
        self.all_relay: list[float] = []
        self.all_network: list[float] = []
        self.all_e2e: list[float] = []
        # Window/accumulated — line format (L)
        self.lnet: list[float] = []
        self.all_lnet: list[float] = []
        self.le2e: list[float] = []
        self.all_le2e: list[float] = []
        self._last_report = time.monotonic()
        self._start = time.monotonic()

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
        self.all_node.append(node_ms)
        self.all_relay.append(relay_ms)
        self.all_network.append(net_ms)
        self.all_e2e.append(e2e_ms)

        # Per-message compact line
        print(f"[{channel}] blk={block_num} "
              f"e2e={e2e_ms:7.1f}ms  node={node_ms:7.1f}ms  "
              f"relay={relay_ms:5.1f}ms  net={net_ms:7.1f}ms",
              flush=True)

        now = time.monotonic()
        if now - self._last_report >= self.interval:
            self._print_stats()
            self._last_report = now

    def record_net(self, relay_ns: int, recv_ns: int, channel: str, seq: int,
                   block_time_ns: int):
        """Record latency for line-format messages.
        block_time_ns: fill event time in ns (FL); relay_ns for book diffs (DL, no timestamp)."""
        net_ms = (recv_ns - relay_ns) / 1e6
        e2e_ms = (recv_ns - block_time_ns) / 1e6

        self.lnet.append(net_ms)
        self.all_lnet.append(net_ms)
        self.le2e.append(e2e_ms)
        self.all_le2e.append(e2e_ms)

        print(f"[{channel}] blk={seq} "
              f"e2e={e2e_ms:7.1f}ms  node=    N/Ams  "
              f"relay=  N/Ams  net={net_ms:7.1f}ms",
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

    def _print_section(self, title: str, datasets: list[tuple[str, list[float]]]):
        print(f"  {title}", file=sys.stderr)
        print(f"{'':>12s}  {'avg':>8s}  {'p25':>8s}  {'p50':>8s}  {'p75':>8s}  {'p99':>8s}  {'max':>8s}",
              file=sys.stderr)
        for label, data in datasets:
            avg = sum(data) / len(data) if data else 0.0
            p25 = self._pct(data, 25)
            p50 = self._pct(data, 50)
            p75 = self._pct(data, 75)
            p99 = self._pct(data, 99)
            mx = max(data) if data else 0.0
            print(f"  {label:>10s}: {avg:7.1f}ms {p25:7.1f}ms {p50:7.1f}ms {p75:7.1f}ms {p99:7.1f}ms {mx:7.1f}ms",
                  file=sys.stderr)

    def _print_stats(self):
        n = len(self.e2e)
        n_all = len(self.all_e2e)
        nl = len(self.lnet)
        nl_all = len(self.all_lnet)
        if n == 0 and n_all == 0 and nl == 0 and nl_all == 0:
            return
        elapsed = time.monotonic() - self._start
        print(f"\n--- Latency stats ---", file=sys.stderr)

        if n > 0:
            self._print_section(
                f"Last {self.interval:.0f}s ({n} msgs):",
                [("e2e", self.e2e), ("node", self.node),
                 ("relay", self.relay), ("network", self.network)])

        if n_all > 0:
            self._print_section(
                f"Accumulated ({n_all} msgs, {elapsed:.0f}s):",
                [("e2e", self.all_e2e), ("node", self.all_node),
                 ("relay", self.all_relay), ("network", self.all_network)])

        if nl > 0:
            self._print_section(
                f"Line-fmt last {self.interval:.0f}s ({nl} msgs):",
                [("e2e", self.le2e), ("network", self.lnet)])

        if nl_all > 0:
            self._print_section(
                f"Line-fmt accumulated ({nl_all} msgs, {elapsed:.0f}s):",
                [("e2e", self.all_le2e), ("network", self.all_lnet)])

        print(file=sys.stderr, flush=True)
        # Reset window
        self.node.clear()
        self.relay.clear()
        self.network.clear()
        self.e2e.clear()
        self.lnet.clear()
        self.le2e.clear()


async def main():
    parser = argparse.ArgumentParser(description="WebSocket dump with optional compression")
    parser.add_argument("ws_url", help="WebSocket URL, e.g. ws://host:port/ws")
    parser.add_argument("subscription", nargs="?", default="{}",
                        help='Subscription JSON (default: "{}")')
    parser.add_argument("--coins", type=str, default=None,
                        help="Comma-separated coin filter, e.g. BTC,ETH,SOL")
    parser.add_argument("--channels", type=str, default=None,
                        help="Comma-separated channel filter, e.g. book,trade")
    parser.add_argument("--no-compress", action="store_true",
                        help="Disable permessage-deflate compression")
    parser.add_argument("--latency", action="store_true",
                        help="Enable latency measurement (parse relay timestamps)")
    parser.add_argument("--latency-interval", type=float, default=10.0,
                        help="Seconds between aggregate latency stats (default: 10)")
    args = parser.parse_args()

    url = urlparse(args.ws_url)
    # Build subscription: --coins/--channels override the positional JSON arg
    if args.coins or args.channels:
        sub_obj = {}
        if args.coins:
            sub_obj["coins"] = [c.strip() for c in args.coins.split(",") if c.strip()]
        if args.channels:
            sub_obj["channels"] = [c.strip() for c in args.channels.split(",") if c.strip()]
        sub = json.dumps(sub_obj)
    else:
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
    ext_header = "Sec-WebSocket-Extensions: permessage-deflate; server_no_context_takeover\r\n" if request_deflate else ""
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
    deflate_on = False
    accept_key = None
    for line in resp_str.split("\r\n"):
        ll = line.lower()
        if ll.startswith("sec-websocket-accept:"):
            accept_key = line.split(":", 1)[1].strip()
        elif ll.startswith("sec-websocket-extensions:") and "permessage-deflate" in ll:
            deflate_on = True

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

    compressor = zlib.compressobj(zlib.Z_DEFAULT_COMPRESSION, zlib.DEFLATED, -15) if deflate_on else None

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

    server_no_context_takeover = False
    for line in resp_str.split("\r\n"):
        if line.lower().startswith("sec-websocket-extensions:"):
            ext = line.split(":", 1)[1]
            if "server_no_context_takeover" in ext:
                server_no_context_takeover = True

    recv_decompressor = None if server_no_context_takeover else zlib.decompressobj(-15)


    def decompress_message(data: bytes) -> bytes:
        try:
            if server_no_context_takeover:
                return zlib.decompressobj(-15).decompress(data + _DEFLATE_TAIL)
            else:
                return recv_decompressor.decompress(data + _DEFLATE_TAIL)
        except zlib.error as exc:
            sys.stderr.write(
                f"[ZLIB ERR] {exc}  no_ctx={server_no_context_takeover}\n"
                f"  payload hex ({len(data)} bytes): {data[:64].hex()}\n"
            )
            sys.stderr.flush()
            raise

    async def read_one_frame():
        h = await reader.readexactly(2)
        fin = bool(h[0] & 0x80)
        rsv1 = bool(h[0] & 0x40)
        opcode = h[0] & 0x0F

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

        return fin, rsv1, opcode, payload
    
    async def read_message():
        fragments = []
        msg_opcode = None
        compressed = False

        while True:
            fin, rsv1, opcode, payload = await read_one_frame()

            # control frames
            if opcode == 0x8:   # close
                return opcode, payload
            if opcode == 0x9:   # ping
                writer.write(encode_frame(payload, opcode=0xA))
                await writer.drain()
                continue
            if opcode == 0xA:   # pong
                continue

            # start of data message
            if opcode in (0x1, 0x2):
                msg_opcode = opcode
                compressed = rsv1
                fragments = [payload]
            elif opcode == 0x0:  # continuation
                if msg_opcode is None:
                    raise RuntimeError("unexpected continuation frame")
                fragments.append(payload)
            else:
                raise RuntimeError(f"unexpected opcode 0x{opcode:x}")

            if fin:
                data = b"".join(fragments)
                if compressed and deflate_on:
                    data = decompress_message(data)
                return msg_opcode, data

    # Send subscription
    writer.write(encode_frame(sub.encode()))
    await writer.drain()
    print(f"Sent subscription: {sub}", file=sys.stderr)

    # Read and print messages
    try:
        while True:
            opcode, payload = await read_message()
            recv_ns = time.time_ns()
            if opcode == 0x1:  # text
                # The C++ relay may coalesce multiple messages into one
                # WebSocket frame (newline-delimited). Process each line.
                for line in payload.decode().splitlines():
                    if not line:
                        continue
                    if tracker:
                        _process_latency(line, recv_ns, tracker)
                    else:
                        print(line, flush=True)
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
        seq = int(parts[1])
        relay_ns = int(parts[2])
        json_str = parts[3]
    except (ValueError, IndexError):
        print(text, flush=True)
        return

    fmt = channel[-1] if channel else ""

    if fmt == "B":
        # Batch format: JSON has block_time, local_time, block_number
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
    elif fmt == "L":
        # Line format: fills have a "time" field (epoch ms) → compute e2e.
        # Book diffs have no timestamp → net only.
        block_time_ns = relay_ns
        if channel.startswith("F"):
            try:
                event = json.loads(json_str)
                block_time_ns = event[1]["time"] * 1_000_000  # ms → ns
            except (json.JSONDecodeError, KeyError, IndexError, TypeError):
                pass
        tracker.record_net(relay_ns, recv_ns, channel, seq, block_time_ns)
        return

    # Fallback: can't extract timestamps, just print
    print(text, flush=True)


asyncio.run(main())
