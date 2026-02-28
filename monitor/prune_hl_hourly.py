#!/usr/bin/env python3
"""
Prune Hyperliquid non-validator node hourly data older than a configurable age.

Layout: /home/ubuntu/hl/data/<source>/hourly/<YYYYMMDD>/<H>
  - <YYYYMMDD> is a date directory
  - <H> is a file named by hour (0-23)

Usage:
  python3 prune_hourly.py                  # dry-run, 8h retention
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
from datetime import datetime, timedelta, timezone

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
    "periodic_abci_state_statuses",
    "periodic_abci_states",
    "rate_limited_ips",
    "replica_cmds",
    "tcp_lz4_stats",
    "tcp_traffic",
    "tokio_spawn_forever_metrics",
    "visor_abci_states",
    "visor_child_stderr",
]


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


def prune(max_age_hours: int, execute: bool) -> dict:
    """Scan all hourly sources and delete entries older than max_age_hours.

    Returns a dict of stats: {deleted_files, deleted_dirs, freed_bytes, errors}.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
    stats = {"deleted_files": 0, "deleted_dirs": 0, "freed_bytes": 0, "errors": 0}

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


def main():
    parser = argparse.ArgumentParser(
        description="Prune Hyperliquid node hourly data older than N hours"
    )
    parser.add_argument(
        "--max-age",
        type=int,
        default=2,
        help="Hours of data to retain (default: 8)",
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

    stats = prune(args.max_age, args.execute)

    total_mb = stats["freed_bytes"] / 1e6
    logging.info(
        "Prune done: files=%d, dirs=%d, freed=%.1f MB, errors=%d",
        stats["deleted_files"],
        stats["deleted_dirs"],
        total_mb,
        stats["errors"],
    )

    if stats["errors"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
