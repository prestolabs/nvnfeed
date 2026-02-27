# HL Node Monitor — Metrics & Alerting Design

## Overview

The HL Node Monitor (`monitor/hl_monitor.py`) is a single-process asyncio daemon that tails
file-based metrics produced by the Hyperliquid non-validator node under `~/hl/data/`. It exposes
all metrics via Prometheus on `:9100/metrics` and sends periodic reports and alerts to Slack.

## Usage

### Prerequisites

- conda environment: `coin2_prod_env_310`
- `prometheus_client` installed in the conda env (`pip install prometheus_client`)
- Slack bot invited to `#onchain_hyperliquid_noti`

### Running

```bash
# Basic (defaults: terminal report every 30s, Slack report every 300s)
./pyrunner monitor/hl_monitor.py

# Custom intervals and port
./pyrunner monitor/hl_monitor.py --port 9100 --report-interval 10 --slack-interval 600

# Debug logging
./pyrunner monitor/hl_monitor.py --log-level DEBUG
```

### CLI Options

| Flag | Default | Description |
|------|---------|-------------|
| `--port` | `9100` | Prometheus metrics HTTP port |
| `--data-dir` | `~/hl/data` | Path to HL data directory |
| `--report-interval` | `30` | Seconds between terminal (stderr) reports |
| `--slack-interval` | `300` | Seconds between Slack reports |
| `--log-level` | `INFO` | Logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |

### Running as a background service

```bash
# Run in background with nohup
nohup ./pyrunner monitor/hl_monitor.py --port 9100 --report-interval 30 --slack-interval 300 \
    >> logs/hl_monitor.log 2>&1 &

# Check it's running
curl -s localhost:9100/metrics | grep hl_block_height
```

### Verifying

```bash
# Check Prometheus metrics are exposed
curl -s localhost:9100/metrics | grep hl_block_height
curl -s localhost:9100/metrics | grep hl_process_up
curl -s localhost:9100/metrics | grep hl_disk_use_fraction

# Test Slack connectivity
./pyrunner -c "from xunkemgmt_client.tool.slack_noti import send_to_slack; send_to_slack('test', '#onchain_hyperliquid_noti', 'msg')"
```

## Data Sources

| Source | Path Pattern | Update Freq | Format |
|--------|-------------|-------------|--------|
| Fast block times | `node_fast_block_times/<YYYYMMDD>` | ~11/sec | JSON lines |
| Slow block times | `node_slow_block_times/<YYYYMMDD>` | ~11/sec | JSON lines |
| Latency summaries | `latency_summaries/<category>/<YYYYMMDD>` | ~30s | JSON lines |
| Latency summaries (subcat) | `latency_summaries/<category>/<subcategory>/<YYYYMMDD>` | ~30s | JSON lines |
| Visor ABCI states | `visor_abci_states/hourly/<YYYYMMDD>/<H>` | ~5s | JSON lines (array) |
| TCP traffic | `tcp_traffic/hourly/<YYYYMMDD>/<H>` | ~30s | JSON lines (array) |
| Logs | `log/{infra,trade,visor}/{error,warn}/<YYYYMMDD>` | sporadic | text lines |
| Process health | `/proc/<pid>/*` | polled 15s | procfs |
| Disk | `os.statvfs(/)` | polled 60s | syscall |

## Prometheus Metrics Reference

### Block Timing

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `hl_block_height` | Gauge | `chain` | Latest block height |
| `hl_block_propagation_delay_seconds` | Gauge | `chain` | Block propagation delay (begin_block_wall_time - block_time) |
| `hl_block_apply_duration_seconds` | Gauge | `chain` | Time to apply the block |
| `hl_blocks_total` | Counter | `chain` | Total number of blocks processed |
| `hl_monitor_last_block_age_seconds` | Gauge | `chain` | Seconds since last block was seen |

Label values for `chain`: `fast`, `slow`

### Latency Summaries

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `hl_latency_mean_seconds` | Gauge | `category`, `subcategory` | Mean latency |
| `hl_latency_median_seconds` | Gauge | `category`, `subcategory` | Median (p50) latency |
| `hl_latency_p90_seconds` | Gauge | `category`, `subcategory` | 90th percentile latency |
| `hl_latency_p95_seconds` | Gauge | `category`, `subcategory` | 95th percentile latency |
| `hl_latency_max_seconds` | Gauge | `category`, `subcategory` | Maximum latency |
| `hl_latency_total_n` | Gauge | `category`, `subcategory` | Total sample count |

Categories (flat — `subcategory=""`):
- `bucket_guard`
- `execution_sender`
- `node_fast_backlog_from_node`
- `node_fast_begin_block_to_commit`
- `node_fast_block_duration`
- `node_slow_backlog_from_node`
- `node_slow_begin_block_to_commit`
- `node_slow_block_duration`
- `profiled_rw_lock`
- `proposer`
- `run_node_compute_resps_hash`
- `tokio_scheduled_observer_fast`
- `tokio_scheduled_observer_slow`
- `tokio_spawn_forever_scheduled`

Categories with subcategories:
- `tcp_lz4` → `in_4001`, `in_4002`

### Visor

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `hl_visor_height` | Gauge | — | Current visor ABCI height |
| `hl_visor_clock_skew_seconds` | Gauge | — | Clock skew between wall time and consensus time |

### Peers (TCP Traffic)

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `hl_peer_total_inbound_mbps` | Gauge | — | Total inbound bandwidth (MB/s) |
| `hl_peer_total_outbound_mbps` | Gauge | — | Total outbound bandwidth (MB/s) |
| `hl_peer_count` | Gauge | — | Number of unique connected peers |

### Logs

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `hl_log_lines_total` | Counter | `source`, `level` | Total log lines counted |

Label values: `source` ∈ {`infra`, `trade`, `visor`}, `level` ∈ {`error`, `warn`}

### Process Health

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `hl_process_up` | Gauge | `process` | Process running (1=up, 0=down) |
| `hl_process_rss_bytes` | Gauge | `process` | Resident set size in bytes |

Label values: `process` ∈ {`hl-node`, `hl-visor`}

### Disk

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `hl_disk_used_bytes` | Gauge | — | Disk space used |
| `hl_disk_avail_bytes` | Gauge | — | Disk space available |
| `hl_disk_use_fraction` | Gauge | — | Disk usage as fraction (0.0–1.0) |

## Alert Definitions

| Alert Name | Condition | Cooldown | Severity | Slack |
|------------|-----------|----------|----------|-------|
| `block_stall` | No new fast blocks for >30s | 300s | critical | @here |
| `propagation_delay` | Fast chain propagation delay >500ms | 120s | warning | plain |
| `apply_duration_spike` | Fast chain apply duration >100ms | 120s | warning | plain |
| `process_down` | `hl-node` or `hl-visor` not found in /proc | 60s | critical | @here |
| `disk_warning` | Disk usage >80% | 600s | warning | plain |
| `disk_critical` | Disk usage >90% | 300s | critical | @here |
| `error_burst` | >10 error log lines in last 5 minutes | 300s | warning | plain |
| `visor_lag` | Visor height >10000 blocks (~15min) behind fast chain | 300s | warning | plain |
| `clock_skew` | Visor clock skew >2 seconds | 300s | warning | plain |

## Architecture

```
asyncio event loop (single thread)
  │
  ├── inotify fd (loop.add_reader) → MonitorWatcher → parse callbacks → MetricsState
  ├── prometheus_client HTTP server :9100 /metrics
  ├── periodic_report_terminal() → stderr every --report-interval (default 30s)
  ├── periodic_report_slack()    → Slack every --slack-interval (default 300s)
  ├── periodic_alerts()          → Slack alerts every 10s
  ├── periodic_poll()            → poll all tailers every 2s (inotify fallback)
  ├── periodic_process_check()   → /proc scan every 15s
  ├── periodic_disk_check()      → statvfs every 60s
  └── rotation_checker()         → date/hour file rotation every 5s
```

### File Rotation

- **Date rotation**: At UTC midnight, all flat date-keyed files (`<YYYYMMDD>`) rotate.
  The `rotation_checker` detects the new date string and reopens all tailers.
- **Hour rotation**: At each UTC hour boundary, hourly files (`hourly/<YYYYMMDD>/<H>`)
  rotate. The checker reopens visor and TCP traffic tailers.
- inotify watches are added for both the data files (IN_MODIFY) and parent directories
  (IN_CREATE) to detect new files promptly.

### Periodic Report

A formatted text report is printed to stderr every `--report-interval` seconds (default 30)
and sent to Slack every `--slack-interval` seconds (default 300). The report includes:
- Block heights, propagation delay, apply duration, block rate, age
- Visor height, lag, clock skew
- Key latency percentiles
- Peer bandwidth and count
- Process status and RSS
- Disk usage
- Log error/warning counts (last 5 minutes)
- Active alerts
