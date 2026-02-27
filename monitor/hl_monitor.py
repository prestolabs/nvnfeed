#!/usr/bin/env python3
"""
HL Node Monitor — monitors Hyperliquid non-validator node health via file-based metrics.

Tails data files under ~/hl/data/, exposes Prometheus metrics on :9100,
sends periodic reports and alerts to Slack.

Usage:
    ./pyrunner monitor/hl_monitor.py --port 9100 --data-dir ~/hl/data --report-interval 30
"""

import argparse
import asyncio
import collections
import ctypes
import ctypes.util
import json
import logging
import os
import struct
import sys
import time
from datetime import datetime, timezone

from prometheus_client import Counter, Gauge, start_http_server

from xunkemgmt_client.tool.slack_noti import send_to_slack

# ── Logging ──────────────────────────────────────────────────────────────────

log = logging.getLogger("hl_monitor")

# ── Constants ────────────────────────────────────────────────────────────────

SLACK_CHANNEL = "#onchain_hyperliquid_noti"

# inotify via ctypes (zero pip dependencies)
libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)
libc.inotify_init.restype = ctypes.c_int
libc.inotify_add_watch.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_uint32]
libc.inotify_add_watch.restype = ctypes.c_int

IN_MODIFY = 0x00000002
IN_CREATE = 0x00000100
INOTIFY_EVENT_SIZE = struct.calcsize("iIII")

# Process names to monitor
MONITORED_PROCESSES = {
    "hl-node": "hl-node",
    "hl-visor": "hl-visor",
}

# Latency categories to track (those with subcategory subdirs use subcat)
LATENCY_CATEGORIES_FLAT = [
    "bucket_guard",
    "execution_sender",
    "node_fast_backlog_from_node",
    "node_fast_begin_block_to_commit",
    "node_fast_block_duration",
    "node_slow_backlog_from_node",
    "node_slow_begin_block_to_commit",
    "node_slow_block_duration",
    "profiled_rw_lock",
    "proposer",
    "run_node_compute_resps_hash",
    "tokio_scheduled_observer_fast",
    "tokio_scheduled_observer_slow",
    "tokio_spawn_forever_scheduled",
]

LATENCY_CATEGORIES_WITH_SUBDIR = [
    "tcp_lz4",
]

LOG_SOURCES = ["infra", "trade", "visor"]
LOG_LEVELS = ["error", "warn"]


# ── Helpers ──────────────────────────────────────────────────────────────────

def parse_iso_ns(ts: str) -> float:
    """Parse ISO 8601 timestamp with nanosecond precision to epoch seconds."""
    dt_part, frac = ts.split(".")
    dt = datetime.strptime(dt_part, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
    frac = frac[:9].ljust(9, "0")
    return dt.timestamp() + int(frac) / 1e9


def now_epoch() -> float:
    return time.time()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def current_date_str() -> str:
    return utc_now().strftime("%Y%m%d")


def current_hour() -> int:
    return utc_now().hour


def fmt_ms(seconds: float) -> str:
    """Format seconds as a human-readable millisecond string."""
    if seconds < 0.001:
        return f"{seconds * 1e6:.0f}us"
    if seconds < 1.0:
        return f"{seconds * 1000:.1f}ms"
    return f"{seconds:.2f}s"


def fmt_bytes(b: float) -> str:
    """Format bytes as human-readable."""
    if b < 1024:
        return f"{b:.0f}B"
    if b < 1024 ** 2:
        return f"{b / 1024:.1f}K"
    if b < 1024 ** 3:
        return f"{b / 1024 ** 2:.1f}M"
    return f"{b / 1024 ** 3:.1f}G"


def fmt_rate(mbps: float) -> str:
    """Format MB/s rate."""
    return f"{mbps:.2f}MB/s"


# ── FileTailer ───────────────────────────────────────────────────────────────

class FileTailer:
    """Tails a file, reopening on rotation.
    Buffers incomplete lines to avoid dispatching partial JSON."""

    def __init__(self, path: str, from_beginning: bool = False):
        self.path = path
        self.file = None
        self._buf = ""
        if from_beginning:
            self._open_at_beginning()
        else:
            self._open_at_end()

    def _open_at_end(self):
        if os.path.exists(self.path):
            self.file = open(self.path, "r")
            self.file.seek(0, 2)

    def _open_at_beginning(self):
        if os.path.exists(self.path):
            self.file = open(self.path, "r")

    def read_new_lines(self) -> list:
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
        if data.endswith("\n"):
            self._buf = ""
        else:
            last_nl = data.rfind("\n")
            if last_nl == -1:
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
        if os.path.exists(path):
            self.file = open(path, "r")
            self.file.seek(0, 2)
        else:
            self.file = None

    def close(self):
        if self.file:
            self.file.close()
            self.file = None
        self._buf = ""


# ── Prometheus Metrics ───────────────────────────────────────────────────────

class MetricsState:
    """Holds all Prometheus metrics and internal state for the monitor."""

    def __init__(self):
        # -- Block timing --
        self.block_height = Gauge("hl_block_height", "Latest block height", ["chain"])
        self.block_propagation_delay = Gauge(
            "hl_block_propagation_delay_seconds",
            "Block propagation delay (begin_block_wall_time - block_time)",
            ["chain"],
        )
        self.block_apply_duration = Gauge(
            "hl_block_apply_duration_seconds",
            "Block apply duration",
            ["chain"],
        )
        self.blocks_total = Counter("hl_blocks_total", "Total blocks processed", ["chain"])
        self.last_block_age = Gauge(
            "hl_monitor_last_block_age_seconds",
            "Seconds since last block was seen",
            ["chain"],
        )

        # -- Latency summaries --
        self.latency_mean = Gauge(
            "hl_latency_mean_seconds", "Latency mean", ["category", "subcategory"]
        )
        self.latency_median = Gauge(
            "hl_latency_median_seconds", "Latency median", ["category", "subcategory"]
        )
        self.latency_p90 = Gauge(
            "hl_latency_p90_seconds", "Latency p90", ["category", "subcategory"]
        )
        self.latency_p95 = Gauge(
            "hl_latency_p95_seconds", "Latency p95", ["category", "subcategory"]
        )
        self.latency_max = Gauge(
            "hl_latency_max_seconds", "Latency max", ["category", "subcategory"]
        )
        self.latency_total_n = Gauge(
            "hl_latency_total_n", "Latency total sample count", ["category", "subcategory"]
        )

        # -- Visor --
        self.visor_height = Gauge("hl_visor_height", "Visor ABCI height")
        self.visor_clock_skew = Gauge(
            "hl_visor_clock_skew_seconds", "Visor clock skew (wall - consensus)"
        )

        # -- Peers --
        self.peer_total_inbound_mbps = Gauge(
            "hl_peer_total_inbound_mbps", "Total inbound peer bandwidth MB/s"
        )
        self.peer_total_outbound_mbps = Gauge(
            "hl_peer_total_outbound_mbps", "Total outbound peer bandwidth MB/s"
        )
        self.peer_count = Gauge("hl_peer_count", "Number of connected peers")

        # -- Logs --
        self.log_lines_total = Counter(
            "hl_log_lines_total", "Total log lines by source and level", ["source", "level"]
        )

        # -- Process --
        self.process_up = Gauge(
            "hl_process_up", "Process is running (1=up, 0=down)", ["process"]
        )
        self.process_rss_bytes = Gauge(
            "hl_process_rss_bytes", "Process RSS in bytes", ["process"]
        )

        # -- Disk --
        self.disk_used_bytes = Gauge("hl_disk_used_bytes", "Disk used bytes")
        self.disk_avail_bytes = Gauge("hl_disk_avail_bytes", "Disk available bytes")
        self.disk_use_fraction = Gauge("hl_disk_use_fraction", "Disk usage fraction 0-1")

        # ── Internal state (not exported to Prometheus) ──
        self.fast_height = 0
        self.fast_prop_delay = 0.0
        self.fast_apply_dur = 0.0
        self.fast_last_time = 0.0
        self.fast_block_count = 0

        self.slow_height = 0
        self.slow_prop_delay = 0.0
        self.slow_apply_dur = 0.0
        self.slow_last_time = 0.0

        self.visor_h = 0
        self.visor_skew = 0.0

        self.peer_in_mbps = 0.0
        self.peer_out_mbps = 0.0
        self.peer_cnt = 0

        # Latency snapshots: {(category, subcategory): {mean, med, p90, p95, max, total_n}}
        self.latency_snap = {}

        # Process state: {name: {"up": bool, "rss": int}}
        self.proc_state = {}

        # Disk state
        self.disk_used = 0
        self.disk_avail = 0
        self.disk_total = 0
        self.disk_pct = 0.0

        # Log counters for the sliding window (last 5 min)
        # {(source, level): deque of timestamps}
        self.log_window = collections.defaultdict(lambda: collections.deque())

        # Block rate tracking
        self._block_times_fast = collections.deque()  # timestamps of recent blocks


# ── Parse callbacks ──────────────────────────────────────────────────────────

def parse_block_time(line: str, chain: str, state: MetricsState):
    """Parse a line from node_fast_block_times or node_slow_block_times."""
    try:
        obj = json.loads(line)
    except json.JSONDecodeError:
        return

    height = obj.get("height", 0)
    block_time_str = obj.get("block_time", "")
    begin_str = obj.get("begin_block_wall_time", "")
    apply_dur = obj.get("apply_duration", 0.0)

    if not block_time_str or not begin_str:
        return

    try:
        bt = parse_iso_ns(block_time_str)
        bw = parse_iso_ns(begin_str)
    except (ValueError, IndexError):
        return

    prop_delay = max(bw - bt, 0.0)
    now = now_epoch()

    state.block_height.labels(chain=chain).set(height)
    state.block_propagation_delay.labels(chain=chain).set(prop_delay)
    state.block_apply_duration.labels(chain=chain).set(apply_dur)
    state.blocks_total.labels(chain=chain).inc()
    state.last_block_age.labels(chain=chain).set(0)

    if chain == "fast":
        state.fast_height = height
        state.fast_prop_delay = prop_delay
        state.fast_apply_dur = apply_dur
        state.fast_last_time = now
        state.fast_block_count += 1
        state._block_times_fast.append(now)
        # Keep only last 60s of block timestamps for rate calculation
        cutoff = now - 60
        while state._block_times_fast and state._block_times_fast[0] < cutoff:
            state._block_times_fast.popleft()
    else:
        state.slow_height = height
        state.slow_prop_delay = prop_delay
        state.slow_apply_dur = apply_dur
        state.slow_last_time = now


def parse_latency_summary(line: str, category: str, subcategory: str, state: MetricsState):
    """Parse a line from latency_summaries/<cat>[/<subcat>]/<date>."""
    try:
        obj = json.loads(line)
    except json.JSONDecodeError:
        return

    mean = obj.get("mean", 0.0)
    med = obj.get("med", 0.0)
    p90 = obj.get("p90", 0.0)
    p95 = obj.get("p95", 0.0)
    mx = obj.get("max", 0.0)
    total_n = obj.get("total_n", 0)

    state.latency_mean.labels(category=category, subcategory=subcategory).set(mean)
    state.latency_median.labels(category=category, subcategory=subcategory).set(med)
    state.latency_p90.labels(category=category, subcategory=subcategory).set(p90)
    state.latency_p95.labels(category=category, subcategory=subcategory).set(p95)
    state.latency_max.labels(category=category, subcategory=subcategory).set(mx)
    state.latency_total_n.labels(category=category, subcategory=subcategory).set(total_n)

    state.latency_snap[(category, subcategory)] = {
        "mean": mean, "med": med, "p90": p90, "p95": p95, "max": mx, "total_n": total_n,
    }


def parse_visor_abci(line: str, state: MetricsState):
    """Parse a line from visor_abci_states/hourly/<date>/<hour>."""
    try:
        arr = json.loads(line)
    except json.JSONDecodeError:
        return

    if not isinstance(arr, list) or len(arr) < 2:
        return

    obj = arr[1]
    height = obj.get("height", 0)
    consensus_str = obj.get("consensus_time", "")
    wall_str = obj.get("wall_clock_time", "")

    state.visor_height.set(height)
    state.visor_h = height

    if consensus_str and wall_str:
        try:
            ct = parse_iso_ns(consensus_str)
            wt = parse_iso_ns(wall_str)
            skew = abs(wt - ct)
            state.visor_clock_skew.set(skew)
            state.visor_skew = skew
        except (ValueError, IndexError):
            pass


def parse_tcp_traffic(line: str, state: MetricsState):
    """Parse a line from tcp_traffic/hourly/<date>/<hour>."""
    try:
        arr = json.loads(line)
    except json.JSONDecodeError:
        return

    if not isinstance(arr, list) or len(arr) < 2:
        return

    entries = arr[1]
    if not isinstance(entries, list):
        return

    total_in = 0.0
    total_out = 0.0
    peers = set()

    for entry in entries:
        if not isinstance(entry, list) or len(entry) < 2:
            continue
        key = entry[0]
        mbps = entry[1]
        if not isinstance(key, list) or len(key) < 3:
            continue
        direction = key[0]  # "In" or "Out"
        ip = key[1]
        peers.add(ip)
        if direction == "In":
            total_in += mbps
        else:
            total_out += mbps

    state.peer_total_inbound_mbps.set(total_in)
    state.peer_total_outbound_mbps.set(total_out)
    state.peer_count.set(len(peers))

    state.peer_in_mbps = total_in
    state.peer_out_mbps = total_out
    state.peer_cnt = len(peers)


def parse_log_line(line: str, source: str, level: str, state: MetricsState):
    """Count a log line from log/<source>/<level>/<date>."""
    state.log_lines_total.labels(source=source, level=level).inc()
    now = now_epoch()
    state.log_window[(source, level)].append(now)
    # Trim to 5 min window
    cutoff = now - 300
    dq = state.log_window[(source, level)]
    while dq and dq[0] < cutoff:
        dq.popleft()


# ── Alert Manager ────────────────────────────────────────────────────────────

class AlertManager:
    """Evaluates alert conditions with cooldown-based dedup."""

    # (name, cooldown_seconds, severity)
    ALERT_DEFS = [
        ("block_stall", 300, "critical"),
        ("propagation_delay", 120, "warning"),
        ("apply_duration_spike", 120, "warning"),
        ("process_down", 60, "critical"),
        ("disk_warning", 600, "warning"),
        ("disk_critical", 300, "critical"),
        ("error_burst", 300, "warning"),
        ("visor_lag", 300, "warning"),
        ("clock_skew", 300, "warning"),
    ]

    def __init__(self):
        self.last_fired = {}  # alert_name -> epoch
        self.active_alerts = []  # list of currently active alert strings

    def _can_fire(self, name: str, cooldown: float) -> bool:
        last = self.last_fired.get(name, 0)
        return (now_epoch() - last) >= cooldown

    def _fire(self, name: str, severity: str, msg: str):
        self.last_fired[name] = now_epoch()
        self.active_alerts.append(msg)
        mention = ["here"] if severity == "critical" else None
        try:
            send_to_slack(
                f"[{severity.upper()}] {msg}",
                SLACK_CHANNEL,
                "msg",
                mention_list=mention,
            )
        except Exception as e:
            log.error("Failed to send Slack alert: %s", e)
        log.warning("ALERT [%s] %s: %s", severity, name, msg)

    def evaluate(self, state: MetricsState):
        """Run all alert checks against current state."""
        self.active_alerts = []
        now = now_epoch()

        # block_stall: no new blocks for >30s
        if state.fast_last_time > 0:
            age = now - state.fast_last_time
            state.last_block_age.labels(chain="fast").set(age)
            if age > 30 and self._can_fire("block_stall", 300):
                self._fire(
                    "block_stall", "critical",
                    f"No new fast blocks for {age:.0f}s (last height={state.fast_height})",
                )

        # propagation_delay: >500ms
        if state.fast_prop_delay > 0.5 and self._can_fire("propagation_delay", 120):
            self._fire(
                "propagation_delay", "warning",
                f"Propagation delay {state.fast_prop_delay * 1000:.0f}ms",
            )

        # apply_duration_spike: >100ms
        if state.fast_apply_dur > 0.1 and self._can_fire("apply_duration_spike", 120):
            self._fire(
                "apply_duration_spike", "warning",
                f"Apply duration spike {state.fast_apply_dur * 1000:.0f}ms",
            )

        # process_down
        for pname, pstate in state.proc_state.items():
            if not pstate.get("up", False):
                if self._can_fire("process_down", 60):
                    self._fire("process_down", "critical", f"{pname} is DOWN")

        # disk_critical: >90%
        if state.disk_pct > 0.9:
            if self._can_fire("disk_critical", 300):
                self._fire(
                    "disk_critical", "critical",
                    f"Disk usage {state.disk_pct * 100:.0f}%",
                )
        # disk_warning: >80%
        elif state.disk_pct > 0.8:
            if self._can_fire("disk_warning", 600):
                self._fire(
                    "disk_warning", "warning",
                    f"Disk usage {state.disk_pct * 100:.0f}%",
                )

        # error_burst: >10 errors in 5min
        total_errors = 0
        for (source, level), dq in state.log_window.items():
            if level == "error":
                total_errors += len(dq)
        if total_errors > 10 and self._can_fire("error_burst", 300):
            self._fire("error_burst", "warning", f"{total_errors} errors in last 5min")

        # visor_lag: >10000 blocks (~15min) behind fast chain
        # Note: visor writer buffers ~60s, so normal lag can be up to ~660 blocks.
        # Use 10000 threshold to avoid false positives from buffered writes.
        if state.fast_height > 0 and state.visor_h > 0:
            lag = state.fast_height - state.visor_h
            if lag > 10000 and self._can_fire("visor_lag", 300):
                self._fire("visor_lag", "warning", f"Visor lagging {lag} blocks behind fast chain")

        # clock_skew: >2s
        if state.visor_skew > 2.0 and self._can_fire("clock_skew", 300):
            self._fire("clock_skew", "warning", f"Clock skew {state.visor_skew:.2f}s")


# ── MonitorWatcher ───────────────────────────────────────────────────────────

class MonitorWatcher:
    """Manages inotify watches on all data sources, with date/hour rotation."""

    def __init__(self, data_dir: str, state: MetricsState):
        self.data_dir = data_dir
        self.state = state

        self.inotify_fd = libc.inotify_init()
        if self.inotify_fd < 0:
            raise OSError(f"inotify_init failed: errno={ctypes.get_errno()}")

        # wd -> (tag, meta)
        self.watch_map = {}
        # tag -> FileTailer
        self.tailers = {}
        # tag -> meta (for polling fallback)
        self.tag_meta = {}
        # Track current date/hour for rotation
        self._current_date = current_date_str()
        self._current_hour = current_hour()
        # Discovered latency subcategories
        self._latency_subcats = {}  # category -> [subcat, ...]

        self._setup_watches()

    def _add_watch(self, path: str, tag: str, meta: dict, mask: int = IN_MODIFY):
        # Always register tag_meta so polling fallback works even if file doesn't exist yet
        if not tag.startswith("dir_"):
            self.tag_meta[tag] = meta
        if not os.path.exists(path):
            return -1
        wd = libc.inotify_add_watch(
            self.inotify_fd, path.encode("utf-8"), mask
        )
        if wd >= 0:
            self.watch_map[wd] = (tag, meta)
        return wd

    def _setup_watches(self):
        """Set up initial watches for all data sources."""
        date = self._current_date
        hour = str(self._current_hour)

        # Block times (flat date files)
        for chain in ["fast", "slow"]:
            base = os.path.join(self.data_dir, f"node_{chain}_block_times")
            path = os.path.join(base, date)
            tag = f"block_{chain}"
            self.tailers[tag] = FileTailer(path)
            self._add_watch(path, tag, {"chain": chain, "type": "block_time"})

        # Latency summaries (flat date files, some with subdirectories)
        for cat in LATENCY_CATEGORIES_FLAT:
            base = os.path.join(self.data_dir, "latency_summaries", cat)
            path = os.path.join(base, date)
            tag = f"latency_{cat}"
            self.tailers[tag] = FileTailer(path)
            self._add_watch(path, tag, {"type": "latency", "category": cat, "subcategory": ""})

        for cat in LATENCY_CATEGORIES_WITH_SUBDIR:
            base = os.path.join(self.data_dir, "latency_summaries", cat)
            if os.path.isdir(base):
                self._latency_subcats[cat] = []
                for subcat in sorted(os.listdir(base)):
                    subdir = os.path.join(base, subcat)
                    if os.path.isdir(subdir):
                        self._latency_subcats[cat].append(subcat)
                        path = os.path.join(subdir, date)
                        tag = f"latency_{cat}_{subcat}"
                        self.tailers[tag] = FileTailer(path)
                        self._add_watch(
                            path, tag,
                            {"type": "latency", "category": cat, "subcategory": subcat},
                        )

        # Visor ABCI states (hourly)
        visor_path = os.path.join(
            self.data_dir, "visor_abci_states", "hourly", date, hour
        )
        self.tailers["visor"] = FileTailer(visor_path)
        self._add_watch(visor_path, "visor", {"type": "visor"})

        # TCP traffic (hourly)
        tcp_path = os.path.join(
            self.data_dir, "tcp_traffic", "hourly", date, hour
        )
        self.tailers["tcp"] = FileTailer(tcp_path)
        self._add_watch(tcp_path, "tcp", {"type": "tcp"})

        # Log files (flat date files under log/<source>/<level>/)
        for source in LOG_SOURCES:
            for level in LOG_LEVELS:
                path = os.path.join(self.data_dir, "log", source, level, date)
                tag = f"log_{source}_{level}"
                self.tailers[tag] = FileTailer(path)
                self._add_watch(
                    path, tag,
                    {"type": "log", "source": source, "level": level},
                )

        # Watch date directories for new file creation (rotation)
        for chain in ["fast", "slow"]:
            base = os.path.join(self.data_dir, f"node_{chain}_block_times")
            if os.path.isdir(base):
                self._add_watch(base, f"dir_block_{chain}", {"type": "dir"}, IN_CREATE)

        visor_date_dir = os.path.join(self.data_dir, "visor_abci_states", "hourly", date)
        if os.path.isdir(visor_date_dir):
            self._add_watch(visor_date_dir, "dir_visor", {"type": "dir"}, IN_CREATE)

        tcp_date_dir = os.path.join(self.data_dir, "tcp_traffic", "hourly", date)
        if os.path.isdir(tcp_date_dir):
            self._add_watch(tcp_date_dir, "dir_tcp", {"type": "dir"}, IN_CREATE)

    def on_inotify_readable(self):
        """Called when the inotify fd is readable."""
        buf = os.read(self.inotify_fd, 8192)
        offset = 0
        triggered_tags = set()
        while offset < len(buf):
            wd, mask, cookie, name_len = struct.unpack_from("iIII", buf, offset)
            offset += INOTIFY_EVENT_SIZE + name_len
            if wd in self.watch_map:
                tag, meta = self.watch_map[wd]
                triggered_tags.add(tag)

        # Process all triggered tailers
        for tag in triggered_tags:
            if tag.startswith("dir_"):
                continue
            self._read_tailer(tag)

    def poll_all_tailers(self):
        """Poll all tailers for new data (fallback for writers that don't trigger inotify)."""
        for tag in list(self.tailers.keys()):
            self._read_tailer(tag)

    def _read_tailer(self, tag: str):
        """Read new lines from a tailer and dispatch them."""
        tailer = self.tailers.get(tag)
        if tailer is None:
            return
        meta = self.tag_meta.get(tag)
        if meta is None:
            return
        lines = tailer.read_new_lines()
        for line in lines:
            self._dispatch(tag, meta, line)

    def _dispatch(self, tag: str, meta: dict, line: str):
        """Route a line to the appropriate parser."""
        dtype = meta["type"]
        if dtype == "block_time":
            parse_block_time(line, meta["chain"], self.state)
        elif dtype == "latency":
            parse_latency_summary(line, meta["category"], meta["subcategory"], self.state)
        elif dtype == "visor":
            parse_visor_abci(line, self.state)
        elif dtype == "tcp":
            parse_tcp_traffic(line, self.state)
        elif dtype == "log":
            parse_log_line(line, meta["source"], meta["level"], self.state)

    def check_rotation(self):
        """Check if date or hour has changed, reopen files accordingly."""
        new_date = current_date_str()
        new_hour = current_hour()
        date_changed = new_date != self._current_date
        hour_changed = new_hour != self._current_hour

        if not date_changed and not hour_changed:
            return

        log.info(
            "Rotation check: date %s->%s, hour %d->%d",
            self._current_date, new_date, self._current_hour, new_hour,
        )

        if date_changed:
            self._current_date = new_date
            self._current_hour = new_hour
            self._rotate_date_files()
            self._rotate_hourly_files()
        elif hour_changed:
            self._current_hour = new_hour
            self._rotate_hourly_files()

    def _rotate_date_files(self):
        """Reopen all flat date-keyed files for the new date."""
        date = self._current_date

        for chain in ["fast", "slow"]:
            tag = f"block_{chain}"
            base = os.path.join(self.data_dir, f"node_{chain}_block_times")
            new_path = os.path.join(base, date)
            self._reopen_tailer(tag, new_path)

        for cat in LATENCY_CATEGORIES_FLAT:
            tag = f"latency_{cat}"
            base = os.path.join(self.data_dir, "latency_summaries", cat)
            new_path = os.path.join(base, date)
            self._reopen_tailer(tag, new_path)

        for cat in LATENCY_CATEGORIES_WITH_SUBDIR:
            for subcat in self._latency_subcats.get(cat, []):
                tag = f"latency_{cat}_{subcat}"
                base = os.path.join(self.data_dir, "latency_summaries", cat, subcat)
                new_path = os.path.join(base, date)
                self._reopen_tailer(tag, new_path)

        for source in LOG_SOURCES:
            for level in LOG_LEVELS:
                tag = f"log_{source}_{level}"
                new_path = os.path.join(self.data_dir, "log", source, level, date)
                self._reopen_tailer(tag, new_path)

    def _rotate_hourly_files(self):
        """Reopen hourly files for the new hour."""
        date = self._current_date
        hour = str(self._current_hour)

        visor_path = os.path.join(
            self.data_dir, "visor_abci_states", "hourly", date, hour
        )
        self._reopen_tailer("visor", visor_path)

        tcp_path = os.path.join(
            self.data_dir, "tcp_traffic", "hourly", date, hour
        )
        self._reopen_tailer("tcp", tcp_path)

        # Update directory watches
        visor_date_dir = os.path.join(self.data_dir, "visor_abci_states", "hourly", date)
        if os.path.isdir(visor_date_dir):
            self._add_watch(visor_date_dir, "dir_visor", {"type": "dir"}, IN_CREATE)

        tcp_date_dir = os.path.join(self.data_dir, "tcp_traffic", "hourly", date)
        if os.path.isdir(tcp_date_dir):
            self._add_watch(tcp_date_dir, "dir_tcp", {"type": "dir"}, IN_CREATE)

    def _reopen_tailer(self, tag: str, new_path: str):
        """Reopen a tailer at a new path, updating watches."""
        tailer = self.tailers.get(tag)
        if tailer:
            tailer.reopen(new_path)
        else:
            self.tailers[tag] = FileTailer(new_path)

        meta = self.tag_meta.get(tag)
        if meta and os.path.exists(new_path):
            self._add_watch(new_path, tag, meta)
            log.info("Reopened %s -> %s", tag, new_path)

    def close(self):
        for tailer in self.tailers.values():
            tailer.close()
        os.close(self.inotify_fd)


# ── Process health ───────────────────────────────────────────────────────────

def scan_processes(state: MetricsState):
    """Scan /proc for monitored processes, update state."""
    found = {name: False for name in MONITORED_PROCESSES}
    rss_map = {name: 0 for name in MONITORED_PROCESSES}

    try:
        for pid_str in os.listdir("/proc"):
            if not pid_str.isdigit():
                continue
            pid = int(pid_str)
            try:
                cmdline_path = f"/proc/{pid}/cmdline"
                with open(cmdline_path, "r") as f:
                    cmdline = f.read()
            except (IOError, PermissionError):
                continue

            for name, pattern in MONITORED_PROCESSES.items():
                if pattern in cmdline:
                    found[name] = True
                    # Read RSS from /proc/<pid>/status
                    try:
                        with open(f"/proc/{pid}/status", "r") as f:
                            for sline in f:
                                if sline.startswith("VmRSS:"):
                                    parts = sline.split()
                                    if len(parts) >= 2:
                                        rss_kb = int(parts[1])
                                        rss_map[name] = rss_kb * 1024
                                    break
                    except (IOError, PermissionError, ValueError):
                        pass
                    break
    except (IOError, PermissionError):
        pass

    for name in MONITORED_PROCESSES:
        up = found[name]
        rss = rss_map[name]
        state.process_up.labels(process=name).set(1 if up else 0)
        if up and rss > 0:
            state.process_rss_bytes.labels(process=name).set(rss)
        state.proc_state[name] = {"up": up, "rss": rss}


# ── Disk health ──────────────────────────────────────────────────────────────

def check_disk(state: MetricsState, path: str = "/"):
    """Check disk usage via statvfs."""
    try:
        st = os.statvfs(path)
        total = st.f_frsize * st.f_blocks
        avail = st.f_frsize * st.f_bavail
        used = total - avail
        pct = used / total if total > 0 else 0.0

        state.disk_used_bytes.set(used)
        state.disk_avail_bytes.set(avail)
        state.disk_use_fraction.set(pct)

        state.disk_used = used
        state.disk_avail = avail
        state.disk_total = total
        state.disk_pct = pct
    except OSError as e:
        log.error("statvfs failed: %s", e)


# ── Report formatter ─────────────────────────────────────────────────────────

def format_report(state: MetricsState, alerts: AlertManager) -> str:
    """Format the periodic monitoring report."""
    now = utc_now().strftime("%Y-%m-%d %H:%M:%S UTC")
    sep = "=" * 80

    # Block rate
    if len(state._block_times_fast) >= 2:
        span = state._block_times_fast[-1] - state._block_times_fast[0]
        rate = (len(state._block_times_fast) - 1) / span if span > 0 else 0
    else:
        rate = 0.0

    # Block age
    fast_age = now_epoch() - state.fast_last_time if state.fast_last_time > 0 else -1

    # Visor lag
    visor_lag = state.fast_height - state.visor_h if state.fast_height > 0 and state.visor_h > 0 else 0

    # Log counts in last 5min
    error_count = sum(
        len(dq) for (src, lvl), dq in state.log_window.items() if lvl == "error"
    )
    warn_count = sum(
        len(dq) for (src, lvl), dq in state.log_window.items() if lvl == "warn"
    )

    # Format process info
    proc_parts = []
    for name in MONITORED_PROCESSES:
        ps = state.proc_state.get(name, {})
        if ps.get("up", False):
            rss = ps.get("rss", 0)
            proc_parts.append(f"{name}: UP (RSS={fmt_bytes(rss)})")
        else:
            proc_parts.append(f"{name}: DOWN")
    proc_line = "   ".join(proc_parts)

    # Format disk
    disk_str = (
        f"{fmt_bytes(state.disk_used)} / {fmt_bytes(state.disk_total)} "
        f"({state.disk_pct * 100:.0f}%)"
    )

    # Format latency lines
    latency_lines = []
    display_cats = [
        ("node_fast_block_duration", "", "fast_block_duration"),
        ("node_fast_begin_block_to_commit", "", "fast_begin_block_commit"),
        ("tokio_spawn_forever_scheduled", "", "tokio_spawn_scheduled"),
    ]
    for cat, subcat, label in display_cats:
        snap = state.latency_snap.get((cat, subcat))
        if snap:
            latency_lines.append(
                f"         {label + ':':30s} "
                f"p50={fmt_ms(snap['med'])}  p90={fmt_ms(snap['p90'])}  "
                f"p95={fmt_ms(snap['p95'])}  max={fmt_ms(snap['max'])}"
            )

    # Alert summary
    alert_str = ", ".join(alerts.active_alerts) if alerts.active_alerts else "none"

    lines = [
        sep,
        f"HL Node Monitor{now:>{80 - 15 - 1}}",
        sep,
        f"BLOCKS   fast: height={state.fast_height:,}  "
        f"prop={fmt_ms(state.fast_prop_delay)}  "
        f"apply={fmt_ms(state.fast_apply_dur)}  "
        f"rate={rate:.1f}/s  "
        f"age={fast_age:.1f}s",
        f"         slow: height={state.slow_height:,}  "
        f"prop={fmt_ms(state.slow_prop_delay)}  "
        f"apply={fmt_ms(state.slow_apply_dur)}",
        f"VISOR    height={state.visor_h:,}  lag={visor_lag}  skew={state.visor_skew:.2f}s",
    ]

    if latency_lines:
        lines.append(f"LATENCY{latency_lines[0][7:]}" if latency_lines else "")
        for ll in latency_lines[1:]:
            lines.append(ll)

    lines.extend([
        f"PEERS    in={fmt_rate(state.peer_in_mbps)}  "
        f"out={fmt_rate(state.peer_out_mbps)}  "
        f"count={state.peer_cnt}",
        f"PROCS    {proc_line}",
        f"DISK     {disk_str}",
        f"LOGS     errors={error_count}  warnings={warn_count} (last 5min)",
        f"ALERTS   {alert_str}",
        sep,
    ])

    return "\n".join(lines)


# ── Async main loop ─────────────────────────────────────────────────────────

async def periodic_report_terminal(state: MetricsState, alerts: AlertManager, interval: float):
    """Print periodic report to stderr."""
    while True:
        await asyncio.sleep(interval)
        try:
            report = format_report(state, alerts)
            sys.stderr.write(report + "\n")
            sys.stderr.flush()
        except Exception as e:
            log.error("Error in periodic_report_terminal: %s", e)


async def periodic_report_slack(state: MetricsState, alerts: AlertManager, interval: float):
    """Send periodic report to Slack."""
    while True:
        await asyncio.sleep(interval)
        try:
            report = format_report(state, alerts)
            send_to_slack(f"```{report}```", SLACK_CHANNEL, "msg")
        except Exception as e:
            log.error("Failed to send report to Slack: %s", e)


async def periodic_alerts(state: MetricsState, alerts: AlertManager):
    """Evaluate alert conditions every 10s."""
    while True:
        await asyncio.sleep(10)
        try:
            alerts.evaluate(state)
        except Exception as e:
            log.error("Error in periodic_alerts: %s", e)


async def periodic_process_check(state: MetricsState):
    """Scan processes every 15s."""
    while True:
        try:
            scan_processes(state)
        except Exception as e:
            log.error("Error in process check: %s", e)
        await asyncio.sleep(15)


async def periodic_disk_check(state: MetricsState, disk_path: str):
    """Check disk usage every 60s."""
    while True:
        try:
            check_disk(state, disk_path)
        except Exception as e:
            log.error("Error in disk check: %s", e)
        await asyncio.sleep(60)


async def periodic_poll(watcher: MonitorWatcher):
    """Poll all tailers every 2s as fallback for writers that don't trigger inotify."""
    while True:
        await asyncio.sleep(2)
        try:
            watcher.poll_all_tailers()
        except Exception as e:
            log.error("Error in periodic poll: %s", e)


async def rotation_checker(watcher: MonitorWatcher):
    """Check for date/hour file rotation every 5s."""
    while True:
        await asyncio.sleep(5)
        try:
            watcher.check_rotation()
        except Exception as e:
            log.error("Error in rotation checker: %s", e)


async def async_main(args):
    """Main async entry point."""
    state = MetricsState()
    alerts = AlertManager()

    # Start Prometheus HTTP server
    log.info("Starting Prometheus metrics server on port %d", args.port)
    start_http_server(args.port)

    # Create watcher
    data_dir = os.path.expanduser(args.data_dir)
    log.info("Watching data directory: %s", data_dir)
    watcher = MonitorWatcher(data_dir, state)

    # Register inotify fd with event loop
    loop = asyncio.get_event_loop()
    loop.add_reader(watcher.inotify_fd, watcher.on_inotify_readable)

    # Initial scans
    scan_processes(state)
    check_disk(state, "/")

    # Launch periodic tasks
    tasks = [
        asyncio.create_task(periodic_report_terminal(state, alerts, args.report_interval)),
        asyncio.create_task(periodic_report_slack(state, alerts, args.slack_interval)),
        asyncio.create_task(periodic_alerts(state, alerts)),
        asyncio.create_task(periodic_process_check(state)),
        asyncio.create_task(periodic_disk_check(state, "/")),
        asyncio.create_task(periodic_poll(watcher)),
        asyncio.create_task(rotation_checker(watcher)),
    ]

    log.info("HL Node Monitor started. Press Ctrl+C to stop.")

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass
    finally:
        loop.remove_reader(watcher.inotify_fd)
        watcher.close()
        log.info("HL Node Monitor stopped.")


def main():
    parser = argparse.ArgumentParser(
        description="HL Node Monitor — Hyperliquid node health monitoring daemon",
    )
    parser.add_argument(
        "--port", type=int, default=9100,
        help="Prometheus metrics HTTP port (default: 9100)",
    )
    parser.add_argument(
        "--data-dir", type=str, default=os.path.expanduser("~/hl/data"),
        help="Path to HL data directory (default: ~/hl/data)",
    )
    parser.add_argument(
        "--report-interval", type=float, default=30,
        help="Seconds between terminal reports (default: 30)",
    )
    parser.add_argument(
        "--slack-interval", type=float, default=300,
        help="Seconds between Slack reports (default: 300)",
    )
    parser.add_argument(
        "--log-level", type=str, default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stderr,
    )

    try:
        asyncio.run(async_main(args))
    except KeyboardInterrupt:
        log.info("Interrupted by user.")


if __name__ == "__main__":
    main()
