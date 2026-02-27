#!/usr/bin/env python3
"""
Option A latency measurement: parse L2 book diffs and fills/trades directly
from hl-node output files via inotify, filter for BTC/ETH, and report latency.

Latency = now() - block_time  (end-to-end from block creation to our read)

Also reports:
  file_latency = local_time - block_time  (node propagation + apply + disk write)
  read_latency = now() - local_time       (inotify + our read/parse overhead)
"""

import argparse
import ctypes
import ctypes.util
import json
import os
import select
import struct
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone

# ── inotify via ctypes (no pip dependencies) ─────────────────────────────────

libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)
libc.inotify_init.restype = ctypes.c_int
libc.inotify_add_watch.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_uint32]
libc.inotify_add_watch.restype = ctypes.c_int

IN_MODIFY = 0x00000002
IN_CREATE = 0x00000100
INOTIFY_EVENT_SIZE = struct.calcsize("iIII")

# ── timestamp parsing ────────────────────────────────────────────────────────

def parse_iso_ns(ts: str) -> float:
    """Parse ISO 8601 timestamp with nanosecond precision to epoch seconds."""
    # Format: 2026-02-24T10:19:55.085296121
    dt_part, frac = ts.split(".")
    dt = datetime.strptime(dt_part, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
    # pad/truncate to 9 digits for nanoseconds
    frac = frac[:9].ljust(9, "0")
    return dt.timestamp() + int(frac) / 1e9


def now_epoch() -> float:
    return time.time()


# ── percentile computation ───────────────────────────────────────────────────

def percentiles(data: list[float], pcts: list[float]) -> dict[float, float]:
    if not data:
        return {p: 0.0 for p in pcts}
    s = sorted(data)
    n = len(s)
    result = {}
    for p in pcts:
        idx = int(p / 100.0 * (n - 1))
        result[p] = s[min(idx, n - 1)]
    return result


# ── file tailer ──────────────────────────────────────────────────────────────

class FileTailer:
    """Tails a file that may be rotated (new file created for each hour)."""

    def __init__(self, path: str):
        self.path = path
        self.file = None
        self.offset = 0
        self._open_at_end()

    def _open_at_end(self):
        if os.path.exists(self.path):
            self.file = open(self.path, "r")
            self.file.seek(0, 2)  # seek to end
            self.offset = self.file.tell()

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
        if data:
            for line in data.splitlines():
                if line.strip():
                    lines.append(line.strip())
        return lines

    def reopen(self, path: str):
        """Reopen on a new file (hour rotation)."""
        if self.file:
            self.file.close()
        self.path = path
        self.file = open(path, "r")
        self.file.seek(0, 2)
        self.offset = self.file.tell()


# ── main ─────────────────────────────────────────────────────────────────────

def get_current_hour_path(base_dir: str) -> str:
    now = datetime.now(timezone.utc)
    return os.path.join(base_dir, "hourly", now.strftime("%Y%m%d"), str(now.hour))


def get_current_date_dir(base_dir: str) -> str:
    now = datetime.now(timezone.utc)
    return os.path.join(base_dir, "hourly", now.strftime("%Y%m%d"))


def main():
    parser = argparse.ArgumentParser(
        description="Option A: measure latency by reading node files directly"
    )
    parser.add_argument(
        "--coins",
        default="BTC,ETH",
        help="Comma-separated coins to track (default: BTC,ETH)",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=60,
        help="Measurement duration in seconds (default: 60)",
    )
    parser.add_argument(
        "--data-dir",
        default=os.path.expanduser("~/hl/data"),
        help="Node data directory",
    )
    args = parser.parse_args()

    coins = set(args.coins.split(","))
    print(f"Tracking coins: {coins}")
    print(f"Duration: {args.duration}s")
    print()

    book_diffs_base = os.path.join(args.data_dir, "node_raw_book_diffs_by_block")
    fills_base = os.path.join(args.data_dir, "node_fills_by_block")

    # Set up inotify
    ifd = libc.inotify_init()
    if ifd < 0:
        print("Failed to init inotify", file=sys.stderr)
        sys.exit(1)

    # Watch the date directories for new hour files, and current hour files for modifications
    watch_to_source: dict[int, tuple[str, str]] = {}  # wd -> (source_type, path)
    tailers: dict[str, FileTailer] = {}  # source_type -> tailer

    def add_watch(path: str, source_type: str):
        if os.path.exists(path):
            wd = libc.inotify_add_watch(ifd, path.encode(), IN_MODIFY | IN_CREATE)
            if wd >= 0:
                watch_to_source[wd] = (source_type, path)

    # Watch date directories for new hour files
    for base, src_type in [(book_diffs_base, "book"), (fills_base, "fills")]:
        date_dir = get_current_date_dir(base)
        os.makedirs(date_dir, exist_ok=True)
        add_watch(date_dir, f"{src_type}_dir")

        # Set up tailer for current hour file
        hour_path = get_current_hour_path(base)
        if os.path.exists(hour_path):
            add_watch(hour_path, src_type)
            tailers[src_type] = FileTailer(hour_path)

    # Latency accumulators
    book_latencies_e2e = []  # end-to-end: now - block_time
    book_latencies_file = []  # file: local_time - block_time
    fills_latencies_e2e = []
    fills_latencies_file = []
    book_block_count = 0
    fills_block_count = 0
    btc_eth_book_events = 0
    btc_eth_fill_events = 0

    def process_book_line(line: str):
        nonlocal book_block_count, btc_eth_book_events
        t_read = now_epoch()
        try:
            d = json.loads(line)
        except json.JSONDecodeError:
            return

        events = d.get("events", [])
        # Check if any event is for our coins
        has_coin = any(e.get("coin") in coins for e in events)
        if not has_coin and events:
            return

        block_time = parse_iso_ns(d["block_time"])
        local_time = parse_iso_ns(d["local_time"])

        e2e = t_read - block_time
        file_lat = local_time - block_time

        if events:
            coin_events = [e for e in events if e.get("coin") in coins]
            if coin_events:
                book_block_count += 1
                btc_eth_book_events += len(coin_events)
                book_latencies_e2e.append(e2e * 1000)  # ms
                book_latencies_file.append(file_lat * 1000)
        else:
            # Empty block - still record latency for the block itself
            book_block_count += 1
            book_latencies_e2e.append(e2e * 1000)
            book_latencies_file.append(file_lat * 1000)

    def process_fills_line(line: str):
        nonlocal fills_block_count, btc_eth_fill_events
        t_read = now_epoch()
        try:
            d = json.loads(line)
        except json.JSONDecodeError:
            return

        events = d.get("events", [])
        # events are [address, fill_obj] pairs
        coin_fills = [e for e in events if len(e) >= 2 and e[1].get("coin") in coins]
        if not coin_fills:
            return

        block_time = parse_iso_ns(d["block_time"])
        local_time = parse_iso_ns(d["local_time"])

        e2e = t_read - block_time
        file_lat = local_time - block_time

        fills_block_count += 1
        btc_eth_fill_events += len(coin_fills)
        fills_latencies_e2e.append(e2e * 1000)
        fills_latencies_file.append(file_lat * 1000)

    print("Listening for file changes via inotify...")
    print(f"  Book diffs: {get_current_hour_path(book_diffs_base)}")
    print(f"  Fills:      {get_current_hour_path(fills_base)}")
    print()

    start_time = time.time()
    epoll = select.epoll()
    epoll.register(ifd, select.EPOLLIN)

    try:
        while time.time() - start_time < args.duration:
            remaining = args.duration - (time.time() - start_time)
            if remaining <= 0:
                break

            events = epoll.poll(timeout=min(remaining, 1.0))
            for fd, event_mask in events:
                if fd != ifd:
                    continue

                buf = os.read(ifd, 65536)
                offset = 0
                while offset < len(buf):
                    wd, mask, cookie, name_len = struct.unpack_from("iIII", buf, offset)
                    offset += INOTIFY_EVENT_SIZE
                    name = buf[offset : offset + name_len].rstrip(b"\x00").decode()
                    offset += name_len

                    if wd in watch_to_source:
                        src_type, path = watch_to_source[wd]

                        if src_type.endswith("_dir") and (mask & IN_CREATE):
                            # New hour file created
                            base_type = src_type.replace("_dir", "")
                            new_path = os.path.join(path, name)
                            add_watch(new_path, base_type)
                            tailers[base_type] = FileTailer(new_path)
                            print(f"  New hour file: {new_path}")

                        elif src_type in ("book", "fills") and (mask & IN_MODIFY):
                            if src_type in tailers:
                                lines = tailers[src_type].read_new_lines()
                                for line in lines:
                                    if src_type == "book":
                                        process_book_line(line)
                                    else:
                                        process_fills_line(line)

            # Also poll tailers even without inotify event (catch up)
            for src_type, tailer in tailers.items():
                lines = tailer.read_new_lines()
                for line in lines:
                    if src_type == "book":
                        process_book_line(line)
                    else:
                        process_fills_line(line)

            # Progress update
            elapsed = time.time() - start_time
            if int(elapsed) % 10 == 0 and int(elapsed) > 0:
                total = len(book_latencies_e2e) + len(fills_latencies_e2e)
                if total > 0:
                    sys.stdout.write(
                        f"\r  [{int(elapsed)}s] book_blocks={book_block_count} "
                        f"fill_blocks={fills_block_count} "
                        f"btc_eth_diffs={btc_eth_book_events} "
                        f"btc_eth_fills={btc_eth_fill_events}"
                    )
                    sys.stdout.flush()

    except KeyboardInterrupt:
        pass
    finally:
        epoll.unregister(ifd)
        epoll.close()
        os.close(ifd)

    elapsed = time.time() - start_time
    print(f"\n\nResults after {elapsed:.1f}s:")
    print("=" * 72)

    pcts = [25, 50, 75, 99, 99.9]

    def print_stats(name: str, e2e: list[float], file_lat: list[float]):
        if not e2e:
            print(f"\n{name}: no data collected")
            return

        print(f"\n{name} ({len(e2e)} samples)")
        print("-" * 72)

        e2e_p = percentiles(e2e, pcts)
        file_p = percentiles(file_lat, pcts)

        print(f"  {'Percentile':<12} {'End-to-End (ms)':>18} {'File Latency (ms)':>20} {'Read Overhead (ms)':>20}")
        for p in pcts:
            e = e2e_p[p]
            f = file_p[p]
            r = e - f
            label = f"p{p:g}"
            print(f"  {label:<12} {e:>18.1f} {f:>20.1f} {r:>20.1f}")

        print(f"  {'max':<12} {max(e2e):>18.1f} {max(file_lat):>20.1f} {max(e2e)-max(file_lat):>20.1f}")
        print(f"  {'min':<12} {min(e2e):>18.1f} {min(file_lat):>20.1f}")
        print(f"  {'mean':<12} {sum(e2e)/len(e2e):>18.1f} {sum(file_lat)/len(file_lat):>20.1f}")

    print_stats(
        f"Book Diffs (BTC/ETH events: {btc_eth_book_events})",
        book_latencies_e2e,
        book_latencies_file,
    )
    print_stats(
        f"Fills (BTC/ETH events: {btc_eth_fill_events})",
        fills_latencies_e2e,
        fills_latencies_file,
    )

    # Combined
    all_e2e = book_latencies_e2e + fills_latencies_e2e
    all_file = book_latencies_file + fills_latencies_file
    print_stats("Combined", all_e2e, all_file)

    print("\n" + "=" * 72)
    print("Key:")
    print("  End-to-End    = now() - block_time    (total latency you'd see)")
    print("  File Latency  = local_time - block_time (node propagation + apply + write)")
    print("  Read Overhead = End-to-End - File Latency (inotify + read + parse)")


if __name__ == "__main__":
    main()
