#!/usr/bin/env python3
"""
Prune Hyperliquid non-validator node data older than a configurable age.

Hourly layout: /home/ubuntu/hl/data/<source>/hourly/<YYYYMMDD>/<H>
  - <YYYYMMDD> is a date directory
  - <H> is a file named by hour (0-23)

Block layout: /home/ubuntu/hl/data/<source>/<YYYYMMDD>/<block>
  - <source> is periodic_abci_states, periodic_abci_state_statuses, etc.
  - <YYYYMMDD> is a date directory
  - <block> is a file named by block number

replica_cmds layout: /home/ubuntu/hl/data/replica_cmds/<session>/<YYYYMMDD>/<block>
  - <session> is an ISO timestamp directory (e.g. 2026-03-02T16:00:07Z)
  - <YYYYMMDD> is a date directory
  - <block> is an NDJSON file named by block number

Usage:
  python3 prune_hourly.py                  # dry-run, 2h retention
  python3 prune_hourly.py --execute        # actually delete
  python3 prune_hourly.py --max-age 4      # keep only last 4 hours

Deployment:
  crontab -e
  0 * * * * /usr/bin/python3 /home/ubuntu/projects/cluade-code-check-hl/prune_hourly.py --execute >> /home/ubuntu/hl/prune.log 2>&1
"""

import argparse
import logging
import os
import shutil
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Set

DATA_ROOT = "/home/ubuntu/hl/data"

HOURLY_SOURCES = [
    "crit_msg_stats",
    "evm_block_and_receipts",
    "latency_buckets",
    "latency_summaries",
    "node_fast_block_times",
    "node_fills_by_block",
    "node_logs",
    "node_order_statuses_by_block",
    "node_raw_book_diffs_by_block",
    "node_slow_block_times",
    "node_trades_by_block",
    "node_twap_statuses_by_block",
    "rate_limited_ips",
    "tcp_lz4_stats",
    "tcp_traffic",
    "tokio_spawn_forever_metrics",
    "visor_abci_states",
    "visor_child_stderr",
]

# Sources with layout: <source>/<YYYYMMDD>/<block_file>
# No "hourly" subdirectory; uses mtime for age.
BLOCK_SOURCES = [
    "periodic_abci_state_statuses",
    "periodic_abci_states",
]


def _open_file_paths(root: str) -> Set[str]:
    """Return absolute paths under *root* that are currently open by any process.

    Scans Linux /proc/<pid>/fd/ symlinks. Falls back to an empty set on
    non-Linux or on permission errors.
    """
    open_paths: Set[str] = set()
    try:
        pids = [p for p in os.listdir("/proc") if p.isdigit()]
    except OSError:
        return open_paths
    for pid in pids:
        fd_dir = os.path.join("/proc", pid, "fd")
        try:
            fds = os.listdir(fd_dir)
        except OSError:
            continue
        for fd in fds:
            try:
                target = os.readlink(os.path.join(fd_dir, fd))
                if target.startswith(root):
                    open_paths.add(target)
            except OSError:
                continue
    return open_paths


def parse_hour_path(date_str: str, hour_str: str) -> datetime | None:
    """Parse YYYYMMDD + H into a UTC datetime, or None if invalid."""
    try:
        dt = datetime.strptime(date_str, "%Y%m%d").replace(tzinfo=timezone.utc)
        hour = int(hour_str)
        if 0 <= hour <= 23:
            return dt.replace(hour=hour)
    except (ValueError, TypeError):
        pass
    return None


def prune(max_age_hours: int, execute: bool, open_files: Set[str] | None = None) -> dict:
    """Scan all hourly sources and delete entries older than max_age_hours.

    Returns a dict of stats: {deleted_files, deleted_dirs, freed_bytes, errors, skipped}.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
    open_files = open_files or set()
    stats = {"deleted_files": 0, "deleted_dirs": 0, "freed_bytes": 0, "errors": 0, "skipped": 0}

    for source in HOURLY_SOURCES:
        hourly_dir = os.path.join(DATA_ROOT, source, "hourly")
        if not os.path.isdir(hourly_dir):
            continue

        for date_name in sorted(os.listdir(hourly_dir)):
            date_path = os.path.join(hourly_dir, date_name)
            if not os.path.isdir(date_path):
                continue

            for entry in sorted(os.listdir(date_path)):
                entry_path = os.path.join(date_path, entry)
                dt = parse_hour_path(date_name, entry)
                if dt is None:
                    continue
                if dt >= cutoff:
                    continue

                # This entry is older than cutoff — delete it
                if entry_path in open_files:
                    logging.info("SKIPPED (open) %s", entry_path)
                    stats["skipped"] += 1
                    continue
                try:
                    if os.path.isfile(entry_path) or os.path.islink(entry_path):
                        size = os.path.getsize(entry_path)
                        if execute:
                            os.remove(entry_path)
                        logging.info(
                            "%s %s (%.1f MB)",
                            "DELETED" if execute else "WOULD DELETE",
                            entry_path,
                            size / 1e6,
                        )
                        stats["deleted_files"] += 1
                        stats["freed_bytes"] += size
                    elif os.path.isdir(entry_path):
                        size = sum(
                            os.path.getsize(os.path.join(dp, f))
                            for dp, _, fns in os.walk(entry_path)
                            for f in fns
                        )
                        if execute:
                            shutil.rmtree(entry_path)
                        logging.info(
                            "%s %s (%.1f MB)",
                            "DELETED" if execute else "WOULD DELETE",
                            entry_path,
                            size / 1e6,
                        )
                        stats["deleted_dirs"] += 1
                        stats["freed_bytes"] += size
                except OSError as e:
                    logging.error("Failed to remove %s: %s", entry_path, e)
                    stats["errors"] += 1

            # Remove empty date directory
            try:
                if os.path.isdir(date_path) and not os.listdir(date_path):
                    if execute:
                        os.rmdir(date_path)
                    logging.info(
                        "%s empty dir %s",
                        "REMOVED" if execute else "WOULD REMOVE",
                        date_path,
                    )
            except OSError as e:
                logging.error("Failed to remove empty dir %s: %s", date_path, e)

    return stats


def prune_block_sources(max_age_hours: int, execute: bool, open_files: Set[str] | None = None) -> dict:
    """Prune BLOCK_SOURCES data older than max_age_hours.

    Layout: <source>/<YYYYMMDD>/<block_file>
    Uses file mtime for age since block numbers don't encode time.

    Returns a dict of stats: {deleted_files, deleted_dirs, freed_bytes, errors, skipped}.
    """
    cutoff_ts = time.time() - max_age_hours * 3600
    open_files = open_files or set()
    stats = {"deleted_files": 0, "deleted_dirs": 0, "freed_bytes": 0, "errors": 0, "skipped": 0}

    for source in BLOCK_SOURCES:
        source_dir = os.path.join(DATA_ROOT, source)
        if not os.path.isdir(source_dir):
            continue

        for date_name in sorted(os.listdir(source_dir)):
            date_path = os.path.join(source_dir, date_name)
            if not os.path.isdir(date_path):
                continue

            for entry in sorted(os.listdir(date_path)):
                entry_path = os.path.join(date_path, entry)
                try:
                    mtime = os.path.getmtime(entry_path)
                except OSError:
                    continue
                if mtime >= cutoff_ts:
                    continue

                if entry_path in open_files:
                    logging.info("SKIPPED (open) %s", entry_path)
                    stats["skipped"] += 1
                    continue

                try:
                    size = os.path.getsize(entry_path)
                    if execute:
                        os.remove(entry_path)
                    logging.info(
                        "%s %s (%.1f MB)",
                        "DELETED" if execute else "WOULD DELETE",
                        entry_path,
                        size / 1e6,
                    )
                    stats["deleted_files"] += 1
                    stats["freed_bytes"] += size
                except OSError as e:
                    logging.error("Failed to remove %s: %s", entry_path, e)
                    stats["errors"] += 1

            # Remove empty date directory
            try:
                if os.path.isdir(date_path) and not os.listdir(date_path):
                    if execute:
                        os.rmdir(date_path)
                    logging.info(
                        "%s empty dir %s",
                        "REMOVED" if execute else "WOULD REMOVE",
                        date_path,
                    )
                    stats["deleted_dirs"] += 1
            except OSError as e:
                logging.error("Failed to remove empty dir %s: %s", date_path, e)

    return stats


def prune_replica_cmds(max_age_hours: int, execute: bool, open_files: Set[str] | None = None) -> dict:
    """Prune replica_cmds data older than max_age_hours.

    Layout: replica_cmds/<session>/<YYYYMMDD>/<block_number>
    Uses file mtime for age since block numbers don't encode time.

    Returns a dict of stats: {deleted_files, deleted_dirs, freed_bytes, errors, skipped}.
    """
    cutoff_ts = time.time() - max_age_hours * 3600
    open_files = open_files or set()
    stats = {"deleted_files": 0, "deleted_dirs": 0, "freed_bytes": 0, "errors": 0, "skipped": 0}
    replica_dir = os.path.join(DATA_ROOT, "replica_cmds")
    if not os.path.isdir(replica_dir):
        return stats

    for session_name in sorted(os.listdir(replica_dir)):
        session_path = os.path.join(replica_dir, session_name)
        if not os.path.isdir(session_path):
            continue

        for date_name in sorted(os.listdir(session_path)):
            date_path = os.path.join(session_path, date_name)
            if not os.path.isdir(date_path):
                continue

            for block_name in sorted(os.listdir(date_path)):
                block_path = os.path.join(date_path, block_name)
                try:
                    mtime = os.path.getmtime(block_path)
                except OSError:
                    continue
                if mtime >= cutoff_ts:
                    continue

                if block_path in open_files:
                    logging.info("SKIPPED (open) %s", block_path)
                    stats["skipped"] += 1
                    continue

                try:
                    size = os.path.getsize(block_path)
                    if execute:
                        os.remove(block_path)
                    logging.info(
                        "%s %s (%.1f MB)",
                        "DELETED" if execute else "WOULD DELETE",
                        block_path,
                        size / 1e6,
                    )
                    stats["deleted_files"] += 1
                    stats["freed_bytes"] += size
                except OSError as e:
                    logging.error("Failed to remove %s: %s", block_path, e)
                    stats["errors"] += 1

            # Remove empty date directory
            try:
                if os.path.isdir(date_path) and not os.listdir(date_path):
                    if execute:
                        os.rmdir(date_path)
                    logging.info(
                        "%s empty dir %s",
                        "REMOVED" if execute else "WOULD REMOVE",
                        date_path,
                    )
                    stats["deleted_dirs"] += 1
            except OSError as e:
                logging.error("Failed to remove empty dir %s: %s", date_path, e)

        # Remove empty session directory
        try:
            if os.path.isdir(session_path) and not os.listdir(session_path):
                if execute:
                    os.rmdir(session_path)
                logging.info(
                    "%s empty dir %s",
                    "REMOVED" if execute else "WOULD REMOVE",
                    session_path,
                )
                stats["deleted_dirs"] += 1
        except OSError as e:
            logging.error("Failed to remove empty dir %s: %s", session_path, e)

    return stats


def main():
    parser = argparse.ArgumentParser(
        description="Prune Hyperliquid node hourly data older than N hours"
    )
    parser.add_argument(
        "--max-age",
        type=int,
        default=2,
        help="Hours of data to retain (default: 2)",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually delete files (default: dry-run)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    mode = "EXECUTE" if args.execute else "DRY-RUN"
    logging.info("Prune start: mode=%s, max_age=%dh", mode, args.max_age)

    open_files = _open_file_paths(DATA_ROOT)
    if open_files:
        logging.info("Open files under %s: %d", DATA_ROOT, len(open_files))

    stats = prune(args.max_age, args.execute, open_files)
    for extra in (
        prune_block_sources(args.max_age, args.execute, open_files),
        prune_replica_cmds(args.max_age, args.execute, open_files),
    ):
        for key in stats:
            stats[key] += extra[key]

    total_mb = stats["freed_bytes"] / 1e6
    logging.info(
        "Prune done: files=%d, dirs=%d, freed=%.1f MB, skipped=%d, errors=%d",
        stats["deleted_files"],
        stats["deleted_dirs"],
        total_mb,
        stats["skipped"],
        stats["errors"],
    )

    if stats["errors"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
