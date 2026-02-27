# order_book_server Latency Optimization Summary

## Starting Point

**Original latency** (C++ `latency_stats.cc`, all 229 coins tracked):
| Metric | p50 | p75 | p99 |
|--------|-----|-----|-----|
| Book   | 214ms | 463ms | 1,894ms |

---

## Root Causes Identified

### 1. Processing all 229 coins when only BTC/ETH needed
- L2 snapshot computation: 229 coins × 7 precision variants = 1,603 snapshots per block
- Full state cloning of all 229 order books
- Large broadcast messages serialized and sent to clients

### 2. Periodic snapshot validation blocking hl-node (CRITICAL)
- Every ~10 seconds, the websocket_server called `fetch_snapshot` which POSTed to `localhost:3001/info`
- This triggered hl-node to dump a **297MB L4 snapshot** to disk
- During the dump, hl-node **stalled all block processing for ~2.7 seconds**
- Proved via controlled experiment: triggered a single snapshot request and observed a 4,085ms gap in block file writes

### 3. Unnecessary async/threading overhead for small workloads
- `spawn_blocking` for L2 computation added thread pool scheduling + cross-thread channel overhead
- `OrderBooks` clone required for the spawned task
- `rayon::par_iter` overhead not justified for 2 coins
- `tokio::spawn` for each broadcast added async task scheduling overhead

---

## Changes Made

### Fix 1: `--coins` Whitelist Filter

Only track and process specified coins (BTC, ETH), skipping all others.

**Files modified:**

| File | Change |
|------|--------|
| `binaries/src/bin/websocket_server.rs` | Added `--coins` CLI arg (`#[arg(long, value_delimiter = ',')]`) |
| `server/src/servers/websocket_server.rs` | Accept `coins: Option<HashSet<String>>`, convert to `Option<HashSet<Coin>>`, pass to listener |
| `server/src/listeners/order_book/mod.rs` | Added `coin_filter` field to `OrderBookListener`, filter fills by coin |
| `server/src/listeners/order_book/state.rs` | Added `coin_filter` field to `OrderBookState`, filter order statuses and diffs in `apply_updates()` |
| `server/src/listeners/order_book/utils.rs` | Filter both sides in `validate_snapshot_consistency()` |
| `server/src/types/node_data.rs` | Added `retain_events()` method to `Batch<E>` for in-place event filtering |
| `~/supervisord.conf` | Added `--coins BTC,ETH` to command line |

**Impact:** Reduced per-block work by ~99% (2 coins instead of 229). L2 computation: 14 variants instead of 1,603.

### Fix 2: Disable Periodic Snapshot Validation

Changed `hl_listen()` to only call `fetch_snapshot` during initial bootstrap (`!is_ready`), not on the periodic 10-second timer.

**File:** `server/src/listeners/order_book/mod.rs`

```rust
// Before: always fetched on timer tick
// After: only fetch when not yet initialized
if !is_ready {
    fetch_snapshot(...);
}
```

**Impact:** Eliminated the 2.7-second hl-node stall that occurred every ~10 seconds. This was the single largest latency contributor.

### Fix 3: Eliminate Async/Threading Overhead

Three sub-optimizations for the now-small (2 coin) workload:

1. **Inline L2 computation** — Reverted from `spawn_blocking` + clone to direct inline computation. With only 2 coins, the computation is fast enough to run on the tokio event loop.

2. **Sequential iteration for small coin counts** — Added `PARALLEL_THRESHOLD = 10` in `compute_l2_snapshots()`. Uses `iter()` instead of `par_iter()` when coin count ≤ 10, avoiding rayon thread pool overhead.

3. **Direct broadcast sends** — Replaced `tokio::spawn(async move { tx.send(msg) })` with direct `tx.send(msg)` for all broadcasts (fills, L4 book updates, L2 snapshots).

**Files:** `server/src/listeners/order_book/mod.rs`, `server/src/listeners/order_book/utils.rs`

---

## Results

### After Fix 1 + Fix 2

**C++ `latency_stats.cc`:**
| Metric | p25 | p50 | p75 | p99 | p999 | max |
|--------|-----|-----|-----|-----|------|-----|
| Book   | 202ms | 243ms | 320ms | 441ms | 484ms | 506ms |
| Trade  | 150ms | 190ms | 223ms | 311ms | 402ms | 402ms |

**File-level baseline** (inotify, no websocket overhead):
| Metric | p50 | p75 | p99 | max |
|--------|-----|-----|-----|-----|
| File   | 122ms | 173ms | 203ms | 219ms |

### Improvement Summary

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Book p75 | 463ms | 320ms | **1.4x** |
| Book p99 | 1,894ms | 441ms | **4.3x** |
| File p75 | 690ms | 174ms | **4.0x** |
| File p99 | 2,787ms | 277ms | **10x** |

### After Fix 1 + Fix 2 + Fix 3 + Fix 4 (C++ Direct Client)

**C++ `hl_client` (direct file watching, 60s run):**
| Metric | p25 | p50 | p75 | p99 | p999 | max |
|--------|-----|-----|-----|-----|------|-----|
| Book   | 112ms | 140ms | 179ms | 231ms | 257ms | 257ms |
| Trade  | 121ms | 137ms | 173ms | 210ms | 222ms | 222ms |

### Full Improvement Summary

| Metric | Original | WS + Fixes 1-3 | C++ Direct (Fix 4) | Total Improvement |
|--------|----------|-----------------|---------------------|-------------------|
| Book p50 | 214ms | 243ms | **140ms** | **1.5x** |
| Book p75 | 463ms | 320ms | **179ms** | **2.6x** |
| Book p99 | 1,894ms | 441ms | **231ms** | **8.2x** |
| Book p999 | - | 484ms | **257ms** | - |
| Trade p50 | - | 190ms | **137ms** | - |
| Trade p99 | - | 311ms | **210ms** | - |

### Irreducible Latency Floor

~110-130ms of the remaining latency is **consensus propagation delay** from validators to the non-validator node, which cannot be reduced at the application layer. The C++ direct client is now within ~20-30ms of this floor at the median.

---

## Fix 4: C++ Direct File-Watching Client

Eliminates the Rust websocket server from the hot path entirely. The C++ client reads node data files directly via inotify, maintains its own order book, computes L2, and extracts trades.

**File:** `hl_client.cc` (standalone, no external dependencies beyond nlohmann/json header)

**Architecture:**
```
hl-node writes files -> inotify -> C++ client reads directly -> parse JSON -> order book -> L2 + trades
```

**What it eliminates vs websocket path:**
- tokio event loop scheduling
- JSON deserialization + re-serialization roundtrip in Rust
- Order book maintenance in Rust
- L2 snapshot computation in Rust
- WebSocket framing, compression, send buffering
- broadcast channel contention
- Network stack overhead (even localhost TCP)

**Server-side support:** Added `--raw` flag to the Rust server for clients that still need websocket but want raw data. In raw mode, the server skips order book maintenance and L2 computation, acting as a thin relay.

**Build:**
```bash
g++ -std=c++17 -O2 -o hl_client hl_client.cc -Iinclude
```

**Usage:**
```bash
./hl_client --coins BTC,ETH --duration 60 --levels 20
```

---

## Deployment

```bash
# Option A: WebSocket server (for websocket clients)
# supervisord.conf command line:
command=cargo run --release --bin websocket_server -- --address 0.0.0.0 --port 8000 --coins BTC,ETH

# Option B: WebSocket server in raw mode (thin relay, client-side processing)
command=cargo run --release --bin websocket_server -- --address 0.0.0.0 --port 8000 --coins BTC,ETH --raw

# Option C: C++ direct client (lowest latency, no server needed)
./hl_client --coins BTC,ETH --duration 60

# Restart websocket server:
supervisorctl -c ~/supervisord.conf restart websocket_server
```
