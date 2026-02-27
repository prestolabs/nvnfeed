# hl-feed

Low-latency market data feed infrastructure for a [Hyperliquid](https://hyperliquid.xyz/) non-validator node. Tails raw node data files via inotify and streams L2 book diffs and fills/trades to remote clients over TCP or WebSocket (with permessage-deflate compression).

## Architecture

```
hl-node (non-validator)
  │  writes files to ~/hl/data/ (inotify, unbuffered)
  │
  ├── relay/hl_relay.py          ← tails files, streams to TCP / WebSocket clients
  │     ├── TCP  :8765           (raw line protocol, optional gzip)
  │     └── WS   :8766           (permessage-deflate)
  │
  ├── benchmark/hl_client.cc     ← C++ direct file watcher (lowest latency, local only)
  │
  └── file_latency_stats.py      ← local inotify latency measurement tool
```

Remote clients connect via `clients/wsdump_deflate.py` (or any WebSocket/TCP client that speaks the wire protocol).

## Quick Start

### 1. Start the HL node

```bash
# Copy node/run-hl.sh to your home directory and run it, or:
~/hl-visor run-non-validator \
    --write-trades \
    --write-fills \
    --write-raw-book-diffs \
    --batch-by-block \
    --disable-output-file-buffering
```

See [node/run-hl.sh](node/run-hl.sh) for flag documentation and caveats (e.g. `--serve-info` causes a 2-2.5s stall every 10 seconds — do **not** use it).

### 2. Start the relay

```bash
python3 relay/hl_relay.py --port 8765 --ws-port 8766
```

No pip dependencies required — uses only the Python 3.12+ standard library.

### 3. Connect a client (from any machine)

```bash
# Dump raw data
python3 clients/wsdump_deflate.py ws://HOST:8766/ws '{"coins":["BTC","ETH"]}'

# With latency measurement
python3 clients/wsdump_deflate.py --latency ws://HOST:8766/ws '{"coins":["BTC","ETH"]}'
```

## Directory Structure

```
├── relay/
│   └── hl_relay.py              # Relay server (asyncio, inotify, TCP + WebSocket)
├── clients/
│   └── wsdump_deflate.py        # WebSocket client with latency measurement
├── benchmark/
│   ├── hl_client.cc             # C++ direct file-watching client (local benchmark)
│   └── run_hl_client.sh         # Build & run script for hl_client
├── node/
│   └── run-hl.sh                # hl-node launch script with flag docs
├── docs/
│   ├── latency_analysis.md      # Detailed latency chain breakdown
│   └── optimization_summary.md  # Optimization history and results
├── monitor/                     # (placeholder) Node health monitoring
├── logs/                        # Runtime logs
└── file_latency_stats.py        # Local file-level latency measurement tool
```

## Wire Protocol

The relay uses a line-delimited text protocol, identical over TCP and WebSocket:

```
DB <seq> <relay_ns> <json>    diffs, batch-by-block format
DL <seq> <relay_ns> <json>    diffs, line format (one event per line)
FB <seq> <relay_ns> <json>    fills, batch-by-block format
FL <seq> <relay_ns> <json>    fills, line format
C  0     <json>               control message
```

| Field      | Description |
|------------|-------------|
| `D` / `F`  | Channel: **D**iffs (L2 book) or **F**ills (trades) |
| `B` / `L`  | Format: **B**atch-by-block or single **L**ine per event |
| `seq`      | Monotonically increasing sequence number (gaps = filtered out) |
| `relay_ns` | Epoch nanoseconds when the relay dispatched the message |

### Subscription

Clients must send a subscription within 10 seconds of connecting:

```json
{"coins": ["BTC", "ETH"]}
```

Send `{}` or `{"coins": []}` to receive all coins (unfiltered).

TCP clients can additionally request gzip compression: `{"coins": ["BTC"], "compress": true}`.

### Batch-by-block JSON format

When the node runs with `--batch-by-block` (recommended), each JSON line wraps all events in a block:

```json
{
  "block_number": 907489555,
  "block_time": "2026-02-27T06:53:10.122591240",
  "local_time": "2026-02-27T06:53:10.236040674",
  "events": [...]
}
```

## Latency Measurement

### With `--latency` flag on the client

```bash
python3 clients/wsdump_deflate.py --latency ws://HOST:8766/ws '{"coins":["BTC","ETH"]}'
```

Prints per-message breakdown and periodic aggregate stats (p50/p90/p99/max):

```
[DB] blk=907489555 e2e=  150.4ms  node=  113.4ms  relay=  2.0ms  net=   35.0ms

--- Latency stats (last 10s, 342 msgs) ---
                  p50       p90       p99       max
       e2e:   148.0ms   182.0ms   231.0ms   312.0ms
      node:   112.0ms   134.0ms   167.0ms   198.0ms
     relay:     2.0ms     3.0ms     5.0ms     8.0ms
   network:    34.0ms    45.0ms    58.0ms   106.0ms
```

| Component | What it measures | Clock skew? |
|-----------|-----------------|-------------|
| **node**  | `local_time - block_time` — consensus propagation + block apply + disk write | No (same machine) |
| **relay** | `relay_ns - local_time` — inotify wakeup + file read + dispatch | No (same machine) |
| **network** | `client_recv - relay_ns` — TCP/WS send + network transit | Yes (cross-machine) |
| **e2e** | `client_recv - block_time` — total end-to-end | Yes (cross-machine) |

### Local file-level measurement

```bash
python3 file_latency_stats.py --coins BTC,ETH --duration 60
```

Measures latency directly from node data files on the same machine (no relay overhead).

### C++ benchmark (lowest possible latency)

```bash
cd benchmark
bash run_hl_client.sh --coins BTC,ETH --duration 300
```

Reads node files directly via inotify, maintains L2 order books, and extracts trades. Achieves ~1ms client overhead; latency is dominated by the ~110ms consensus propagation floor.

## Latency Budget

Typical end-to-end latency from block production to client receipt:

| Stage | Typical | Notes |
|-------|---------|-------|
| Consensus propagation | ~110-130ms | Irreducible for non-validators |
| Block apply + disk write | ~12-15ms | With `--disable-output-file-buffering` |
| inotify + relay dispatch | ~1-3ms | |
| Network (same region) | ~1-5ms | Depends on distance |
| **Total (local C++ client)** | **~130-150ms p50** | |
| **Total (remote WS client)** | **~140-180ms p50** | |

## Node Flags Reference

| Flag | Purpose | Recommendation |
|------|---------|----------------|
| `--write-raw-book-diffs` | L2 order book diffs (~807 MB/hr) | Required for book data |
| `--write-fills` | Fill pairs / trades (~31 MB/hr) | Required for trade data |
| `--write-trades` | Trade summaries (~1.8 MB/hr) | Optional |
| `--batch-by-block` | Batch all events per block into one JSON line | Recommended |
| `--disable-output-file-buffering` | Flush writes to page cache immediately | Required for low latency |
| `--write-order-statuses` | All order lifecycle events (~6.7 GB/hr) | Not needed for book/trade feeds |
| `--serve-info` | Enables info endpoint | **Do not use** — causes 2-2.5s stalls every 10s |
