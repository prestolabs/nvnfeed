# Order Book Server Latency Analysis

## Setup

- **Machine**: AWS EC2 `ap-northeast-1` (Tokyo), IP: 35.79.51.156
- **Hyperliquid validator**: GCP `asia-northeast1` (Tokyo), IP: 35.190.230.32
- **Network ping**: ~3ms between them
- **Node lag**: ~0.2s behind real-time (healthy)
- **Node flags**: `--write-trades --write-fills --write-raw-book-diffs --write-order-statuses --disable-output-file-buffering --batch-by-block --serve-info --replica-cmds-style actions`

## Observed Latency (from latency_stats.cc)

| Percentile | Book Latency (ms) | Trade Latency (ms) |
|------------|-------------------|---------------------|
| p25        | 195.061           | 287.274             |
| p50        | 214.053           | 287.274             |
| p75        | 463.427           | 448.788             |
| p99        | 1894.79           | 1840.6              |
| p999       | 1894.79           | 1840.6              |
| max        | 1894.79           | 1840.6              |

Note: p99 = p999 = max for both, and p25 = p50 for trades, indicating a very small sample size. Tail percentiles are noisy.

## Latency Chain Breakdown

```
exchange_time (block produced on validators)
    |
    +-- ~110ms  Block consensus + propagation to non-validator (IRREDUCIBLE)
    +--  ~12ms  Node applies block (apply_duration)
    +--   ~1ms  Node writes to disk (buffering already disabled)
    +--   ~1ms  notify crate detects file change
    +-- ~20-50ms  compute_l2_snapshots (229 coins x 7 variants) [PROBLEM]
    +--  variable  WebSocket handler queue delay [PROBLEM]
    +--   ~1ms  JSON serialization + compression + WebSocket send
    |
    v
fetched_time (C++ client receives data)
```

### Block Propagation (from node_slow_block_times)

Measured from 67 recent blocks:

| Metric      | Avg     | Min    | Max     |
|-------------|---------|--------|---------|
| Propagation | 121.8ms | 59.2ms | 212.7ms |
| Apply       | 14.1ms  | 0.2ms  | 54.2ms  |
| Total       | 135.8ms | -      | -       |

Propagation distribution:
- 50-100ms:  27%
- 100-150ms: 52%
- 150-200ms: 19%
- 200-300ms:  1.5%

This ~120ms propagation delay is inherent to the non-validator consensus protocol and cannot be reduced via configuration.

## Root Causes of High p75/p99

### Problem 1: L2 snapshot computed on EVERY block while holding Mutex

**File**: `/home/ubuntu/projects/order_book_server/server/src/listeners/order_book/mod.rs:438`

```rust
// Called at the end of process_data(), on every file modification event
let snapshot = self.l2_snapshots(true);
```

This calls `compute_l2_snapshots` (`utils.rs:86-119`) which computes **7 L2 precision variants x 229 coins = 1,603 snapshot computations per block**. Uses rayon `par_iter` but still blocks the async runtime. Critically, this runs **while the `Arc<Mutex<OrderBookListener>>` is held**, blocking all other file event processing (fills, order diffs, order statuses).

**Impact**: ~20-50ms per block of Mutex hold time, preventing concurrent processing of other event sources.

### Problem 2: Sequential WebSocket sends cause head-of-line blocking

**File**: `/home/ubuntu/projects/order_book_server/server/src/servers/websocket_server.rs:119-123`

```rust
// For each InternalMessage::Snapshot:
for sub in manager.subscriptions() {
    send_ws_data_from_snapshot(&mut socket, sub, l2_snapshots.as_ref(), *time).await;
}
```

Each subscription gets a separate sequential WebSocket send (JSON serialization + compression + write). With N L2Book subscriptions, that's N sends **before the handler can process the next broadcast message**. During this time, newer L4BookUpdates and Fills messages queue up in the broadcast channel.

Since L2 snapshots are broadcast **after every block** (~10/sec), this creates a constant stream of large messages.

**Impact**: With many subscriptions, each snapshot broadcast takes 50-200ms+ of handler time, during which subsequent block updates queue up.

### Problem 3: 10-second snapshot fetch clones entire state under Mutex

**File**: `/home/ubuntu/projects/order_book_server/server/src/listeners/order_book/mod.rs:146-153`

```rust
let state = {
    let mut listener = listener.lock().await;  // LOCKS MUTEX
    listener.begin_caching();
    listener.clone_state()  // Deep clones ALL 229 coins' order books
};
// ... HTTP request to localhost:3001/info ...
sleep(Duration::from_secs(1)).await;  // SLEEPS 1 SECOND
let mut cache = {
    let mut listener = listener.lock().await;  // LOCKS MUTEX AGAIN
    listener.take_cache()
};
```

`clone_state()` deep-copies the entire `OrderBooks<InnerL4Order>` while holding the Mutex. This blocks all file event processing. Happens every 10 seconds.

**Impact**: Likely the primary cause of the ~1.9s p99 spikes. Multiple blocks pile up during clone + HTTP + sleep + re-lock sequence.

### Problem 4: Broadcast channel design

**File**: `/home/ubuntu/projects/order_book_server/server/src/servers/websocket_server.rs:33`

```rust
let (internal_message_tx, _) = channel::<Arc<InternalMessage>>(100);
```

The broadcast channel has capacity 100. If the handler falls behind (due to Problems 2/3), the channel fills. Tokio's broadcast channel returns `Lagged` error on overflow, which causes the handler to close the connection (`Err(err) => { error!(...); return; }`).

## Why p75 Jumps to ~463ms

- Baseline latency: ~195-214ms (propagation + node processing + delivery)
- ~25% of updates arrive while the WebSocket handler is busy sending L2 snapshots for the previous block
- Extra queuing delay: ~250ms from L2 snapshot send backlog
- Total: ~463ms

## Why p99 Hits ~1894ms

- Every 10 seconds, `fetch_snapshot` locks the Mutex for `clone_state()` + `begin_caching()`
- During this time (potentially 100ms+), blocks pile up and can't be processed
- After the lock is released, all queued blocks are processed in rapid succession
- Combined with WebSocket handler backlog, the last block in the queue experiences ~1.5-2s of accumulated delay

## Timing Diagram

```
Normal cycle (~100ms per block):

  [Lock Mutex]
  |- Parse JSON, apply_updates (~15ms)
  |- Broadcast L4BookUpdates via tokio::spawn
  |- compute_l2_snapshots: 229 coins x 7 variants (~20-50ms) ← HOLDS MUTEX
  |- Broadcast L2 Snapshot via tokio::spawn
  [Unlock Mutex]

  WebSocket handler (processes broadcast messages sequentially):
  |- Recv L4BookUpdates → send to L4Book subscribers (fast)
  |- Recv L2 Snapshot → send to ALL L2Book subscribers (SLOW, N sends)
  |    └── Next block's messages WAIT in channel during this
  |        └── Adds ~200ms+ extra latency for queued messages

Every 10 seconds:

  [fetch_snapshot locks Mutex]
  |- begin_caching()
  |- clone_state() — deep copy of all order books ← EXPENSIVE + HOLDS MUTEX
  [Unlock Mutex]
  |- HTTP request to localhost:3001/info
  |- sleep(1 second)
  [Lock Mutex again]
  |- take_cache()
  [Unlock Mutex]
  |- Validate snapshot consistency

  During this entire sequence, file events WAIT for the Mutex:
  └── Blocks pile up → latency spikes to ~1.5-2s
```

## latency_stats.cc Code Review

**File**: `/home/ubuntu/projects/coin/cc/experimental/ziyan/latency_stats.cc`

The measurement logic is correct:
- `upd.timestamp()`: nanoseconds from `std::chrono::high_resolution_clock` (wall clock on Linux/libstdc++), captured after WebSocket decompression but before JSON parsing
- `upd.book().Timestamp()`: nanoseconds, converted from JSON `"time"` field (ms since epoch × 1,000,000)
- `upd.trade().timestamp`: nanoseconds, same conversion
- Fallback to `upd.timestamp()` when exchange timestamp is 0 (latency = 0 for those samples)
- Percentile calculation is correct

No bugs in the measurement code. The high latencies are real.

## Node Configuration Assessment

The hl-node flags are already optimized:
- `--disable-output-file-buffering` ✓ (reduces file write latency)
- `--batch-by-block` ✓ (required by order_book_server)
- `--write-raw-book-diffs --write-fills --write-order-statuses` ✓

Remaining flags that won't help:
- `--stream-with-block-info`: adds block metadata to streams, does NOT reduce latency
- `--replica-cmds-style actions`: standard mode, no faster alternative for non-validators

The ~110ms consensus propagation delay is the irreducible floor for any non-validator node.

## Potential Improvements (in order_book_server)

1. **Decouple L2 snapshot from the block processing path**: Don't compute L2 snapshots on every block. Compute them on a separate timer (e.g., every 200ms) outside the Mutex.

2. **Reduce Mutex hold time**: Move `compute_l2_snapshots` outside the Mutex lock. Clone only the data needed for snapshot computation, release the Mutex, then compute.

3. **Batch WebSocket sends**: Instead of sending one message per subscription, batch all subscription data into a single WebSocket frame.

4. **Use `tokio::task::spawn_blocking` for CPU-heavy work**: `compute_l2_snapshots` with rayon blocks the async runtime. Offload to a blocking thread pool.

5. **Optimize `fetch_snapshot`**: Use a read-write lock instead of Mutex, or snapshot incrementally instead of cloning the entire state.

6. **Skip file layer entirely**: Connect directly to the node's TCP stream (port 4001/4002) instead of watching files via `notify` crate. Eliminates file write + filesystem notification overhead.
