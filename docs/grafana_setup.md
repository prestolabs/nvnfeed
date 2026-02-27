# Prometheus + Grafana Setup for HL Node Monitor

## Prerequisites

- HL Node Monitor running: `./pyrunner monitor/hl_monitor.py --port 9100`
- Ubuntu/Debian server

## 1. Prometheus Setup

### Install

```bash
sudo apt-get update
sudo apt-get install -y prometheus
```

Or download the latest binary:

```bash
cd /tmp
wget https://github.com/prometheus/prometheus/releases/download/v2.51.0/prometheus-2.51.0.linux-amd64.tar.gz
tar xzf prometheus-2.51.0.linux-amd64.tar.gz
sudo cp prometheus-2.51.0.linux-amd64/prometheus /usr/local/bin/
sudo cp prometheus-2.51.0.linux-amd64/promtool /usr/local/bin/
```

### Configure

Edit `/etc/prometheus/prometheus.yml`:

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: "hl_node_monitor"
    static_configs:
      - targets: ["localhost:9100"]
        labels:
          instance: "hl-node-1"

  # Optional: if running node_exporter alongside
  - job_name: "node_exporter"
    static_configs:
      - targets: ["localhost:9090"]
```

### Storage & Retention

```yaml
# Add to prometheus CLI flags or systemd unit:
# --storage.tsdb.retention.time=30d
# --storage.tsdb.retention.size=10GB
# --storage.tsdb.path=/var/lib/prometheus/data
```

### Start Prometheus

```bash
sudo systemctl enable prometheus
sudo systemctl start prometheus

# Verify
curl -s http://localhost:9091/api/v1/targets | python3 -m json.tool
```

### Verify Scrape

```bash
# Check that HL metrics are being scraped
curl -s http://localhost:9100/metrics | grep hl_block_height
```

## 2. Grafana Setup

### Install

```bash
sudo apt-get install -y apt-transport-https software-properties-common
sudo mkdir -p /etc/apt/keyrings/
wget -q -O - https://apt.grafana.com/gpg.key | gpg --dearmor | sudo tee /etc/apt/keyrings/grafana.gpg > /dev/null
echo "deb [signed-by=/etc/apt/keyrings/grafana.gpg] https://apt.grafana.com stable main" | sudo tee /etc/apt/sources.list.d/grafana.list
sudo apt-get update
sudo apt-get install -y grafana
```

### Start

```bash
sudo systemctl enable grafana-server
sudo systemctl start grafana-server
```

Default URL: `http://<server-ip>:3000` (admin/admin)

### Add Prometheus Data Source

1. Go to **Configuration → Data Sources → Add data source**
2. Select **Prometheus**
3. Set URL to `http://localhost:9091`
4. Click **Save & Test**

Or via API:

```bash
curl -X POST http://admin:admin@localhost:3000/api/datasources \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "Prometheus",
    "type": "prometheus",
    "url": "http://localhost:9091",
    "access": "proxy",
    "isDefault": true
  }'
```

### Import Dashboard

1. Go to **Dashboards → Import**
2. Paste the JSON below
3. Select the Prometheus data source
4. Click **Import**

## 3. Dashboard JSON

Save the following as `hl_node_dashboard.json` and import into Grafana:

```json
{
  "dashboard": {
    "id": null,
    "uid": "hl-node-monitor",
    "title": "HL Node Monitor",
    "tags": ["hyperliquid", "node", "monitoring"],
    "timezone": "utc",
    "refresh": "10s",
    "time": {
      "from": "now-1h",
      "to": "now"
    },
    "panels": [
      {
        "title": "Fast Block Height",
        "type": "stat",
        "gridPos": { "h": 4, "w": 4, "x": 0, "y": 0 },
        "targets": [
          {
            "expr": "hl_block_height{chain=\"fast\"}",
            "legendFormat": "Height"
          }
        ],
        "fieldConfig": {
          "defaults": {
            "thresholds": {
              "mode": "absolute",
              "steps": [
                { "color": "green", "value": null }
              ]
            }
          }
        }
      },
      {
        "title": "Propagation Delay",
        "type": "timeseries",
        "gridPos": { "h": 8, "w": 8, "x": 4, "y": 0 },
        "targets": [
          {
            "expr": "hl_block_propagation_delay_seconds{chain=\"fast\"}",
            "legendFormat": "Fast"
          },
          {
            "expr": "hl_block_propagation_delay_seconds{chain=\"slow\"}",
            "legendFormat": "Slow"
          }
        ],
        "fieldConfig": {
          "defaults": {
            "unit": "s",
            "thresholds": {
              "mode": "absolute",
              "steps": [
                { "color": "green", "value": null },
                { "color": "yellow", "value": 0.3 },
                { "color": "red", "value": 0.5 }
              ]
            }
          }
        }
      },
      {
        "title": "Apply Duration",
        "type": "timeseries",
        "gridPos": { "h": 8, "w": 8, "x": 12, "y": 0 },
        "targets": [
          {
            "expr": "hl_block_apply_duration_seconds{chain=\"fast\"}",
            "legendFormat": "Fast"
          },
          {
            "expr": "hl_block_apply_duration_seconds{chain=\"slow\"}",
            "legendFormat": "Slow"
          }
        ],
        "fieldConfig": {
          "defaults": {
            "unit": "s",
            "thresholds": {
              "mode": "absolute",
              "steps": [
                { "color": "green", "value": null },
                { "color": "yellow", "value": 0.05 },
                { "color": "red", "value": 0.1 }
              ]
            }
          }
        }
      },
      {
        "title": "Block Rate",
        "type": "stat",
        "gridPos": { "h": 4, "w": 4, "x": 20, "y": 0 },
        "targets": [
          {
            "expr": "rate(hl_blocks_total{chain=\"fast\"}[1m])",
            "legendFormat": "blocks/s"
          }
        ],
        "fieldConfig": {
          "defaults": {
            "unit": "cps",
            "thresholds": {
              "mode": "absolute",
              "steps": [
                { "color": "red", "value": null },
                { "color": "yellow", "value": 5 },
                { "color": "green", "value": 10 }
              ]
            }
          }
        }
      },
      {
        "title": "Last Block Age",
        "type": "stat",
        "gridPos": { "h": 4, "w": 4, "x": 20, "y": 4 },
        "targets": [
          {
            "expr": "hl_monitor_last_block_age_seconds{chain=\"fast\"}",
            "legendFormat": "Age"
          }
        ],
        "fieldConfig": {
          "defaults": {
            "unit": "s",
            "thresholds": {
              "mode": "absolute",
              "steps": [
                { "color": "green", "value": null },
                { "color": "yellow", "value": 10 },
                { "color": "red", "value": 30 }
              ]
            }
          }
        }
      },
      {
        "title": "Latency — Block Duration (fast)",
        "type": "timeseries",
        "gridPos": { "h": 8, "w": 12, "x": 0, "y": 8 },
        "targets": [
          {
            "expr": "hl_latency_median_seconds{category=\"node_fast_block_duration\",subcategory=\"\"}",
            "legendFormat": "p50"
          },
          {
            "expr": "hl_latency_p90_seconds{category=\"node_fast_block_duration\",subcategory=\"\"}",
            "legendFormat": "p90"
          },
          {
            "expr": "hl_latency_p95_seconds{category=\"node_fast_block_duration\",subcategory=\"\"}",
            "legendFormat": "p95"
          },
          {
            "expr": "hl_latency_max_seconds{category=\"node_fast_block_duration\",subcategory=\"\"}",
            "legendFormat": "max"
          }
        ],
        "fieldConfig": {
          "defaults": {
            "unit": "s"
          }
        }
      },
      {
        "title": "Latency — Begin Block to Commit (fast)",
        "type": "timeseries",
        "gridPos": { "h": 8, "w": 12, "x": 12, "y": 8 },
        "targets": [
          {
            "expr": "hl_latency_median_seconds{category=\"node_fast_begin_block_to_commit\",subcategory=\"\"}",
            "legendFormat": "p50"
          },
          {
            "expr": "hl_latency_p90_seconds{category=\"node_fast_begin_block_to_commit\",subcategory=\"\"}",
            "legendFormat": "p90"
          },
          {
            "expr": "hl_latency_p95_seconds{category=\"node_fast_begin_block_to_commit\",subcategory=\"\"}",
            "legendFormat": "p95"
          },
          {
            "expr": "hl_latency_max_seconds{category=\"node_fast_begin_block_to_commit\",subcategory=\"\"}",
            "legendFormat": "max"
          }
        ],
        "fieldConfig": {
          "defaults": {
            "unit": "s"
          }
        }
      },
      {
        "title": "Latency — Tokio Spawn Scheduled",
        "type": "timeseries",
        "gridPos": { "h": 8, "w": 12, "x": 0, "y": 16 },
        "targets": [
          {
            "expr": "hl_latency_median_seconds{category=\"tokio_spawn_forever_scheduled\",subcategory=\"\"}",
            "legendFormat": "p50"
          },
          {
            "expr": "hl_latency_p90_seconds{category=\"tokio_spawn_forever_scheduled\",subcategory=\"\"}",
            "legendFormat": "p90"
          },
          {
            "expr": "hl_latency_p95_seconds{category=\"tokio_spawn_forever_scheduled\",subcategory=\"\"}",
            "legendFormat": "p95"
          },
          {
            "expr": "hl_latency_max_seconds{category=\"tokio_spawn_forever_scheduled\",subcategory=\"\"}",
            "legendFormat": "max"
          }
        ],
        "fieldConfig": {
          "defaults": {
            "unit": "s"
          }
        }
      },
      {
        "title": "Latency — TCP LZ4",
        "type": "timeseries",
        "gridPos": { "h": 8, "w": 12, "x": 12, "y": 16 },
        "targets": [
          {
            "expr": "hl_latency_p90_seconds{category=\"tcp_lz4\"}",
            "legendFormat": "p90 {{subcategory}}"
          },
          {
            "expr": "hl_latency_max_seconds{category=\"tcp_lz4\"}",
            "legendFormat": "max {{subcategory}}"
          }
        ],
        "fieldConfig": {
          "defaults": {
            "unit": "s"
          }
        }
      },
      {
        "title": "Process Status",
        "type": "stat",
        "gridPos": { "h": 4, "w": 6, "x": 0, "y": 24 },
        "targets": [
          {
            "expr": "hl_process_up{process=\"hl-node\"}",
            "legendFormat": "hl-node"
          },
          {
            "expr": "hl_process_up{process=\"hl-visor\"}",
            "legendFormat": "hl-visor"
          }
        ],
        "fieldConfig": {
          "defaults": {
            "mappings": [
              { "type": "value", "options": { "0": { "text": "DOWN", "color": "red" } } },
              { "type": "value", "options": { "1": { "text": "UP", "color": "green" } } }
            ]
          }
        }
      },
      {
        "title": "Process RSS",
        "type": "timeseries",
        "gridPos": { "h": 8, "w": 6, "x": 6, "y": 24 },
        "targets": [
          {
            "expr": "hl_process_rss_bytes{process=\"hl-node\"}",
            "legendFormat": "hl-node"
          },
          {
            "expr": "hl_process_rss_bytes{process=\"hl-visor\"}",
            "legendFormat": "hl-visor"
          }
        ],
        "fieldConfig": {
          "defaults": {
            "unit": "bytes"
          }
        }
      },
      {
        "title": "Disk Usage",
        "type": "gauge",
        "gridPos": { "h": 8, "w": 6, "x": 12, "y": 24 },
        "targets": [
          {
            "expr": "hl_disk_use_fraction * 100",
            "legendFormat": "Usage %"
          }
        ],
        "fieldConfig": {
          "defaults": {
            "unit": "percent",
            "min": 0,
            "max": 100,
            "thresholds": {
              "mode": "absolute",
              "steps": [
                { "color": "green", "value": null },
                { "color": "yellow", "value": 70 },
                { "color": "orange", "value": 80 },
                { "color": "red", "value": 90 }
              ]
            }
          }
        }
      },
      {
        "title": "Peer Bandwidth & Count",
        "type": "timeseries",
        "gridPos": { "h": 8, "w": 6, "x": 18, "y": 24 },
        "targets": [
          {
            "expr": "hl_peer_total_inbound_mbps",
            "legendFormat": "Inbound MB/s"
          },
          {
            "expr": "hl_peer_total_outbound_mbps",
            "legendFormat": "Outbound MB/s"
          },
          {
            "expr": "hl_peer_count",
            "legendFormat": "Peer Count"
          }
        ],
        "fieldConfig": {
          "defaults": {},
          "overrides": [
            {
              "matcher": { "id": "byName", "options": "Peer Count" },
              "properties": [
                { "id": "custom.axisPlacement", "value": "right" },
                { "id": "unit", "value": "short" }
              ]
            }
          ]
        }
      },
      {
        "title": "Visor Height & Lag",
        "type": "stat",
        "gridPos": { "h": 4, "w": 8, "x": 0, "y": 32 },
        "targets": [
          {
            "expr": "hl_visor_height",
            "legendFormat": "Height"
          },
          {
            "expr": "hl_block_height{chain=\"fast\"} - hl_visor_height",
            "legendFormat": "Lag (blocks)"
          }
        ]
      },
      {
        "title": "Visor Clock Skew",
        "type": "timeseries",
        "gridPos": { "h": 8, "w": 8, "x": 8, "y": 32 },
        "targets": [
          {
            "expr": "hl_visor_clock_skew_seconds",
            "legendFormat": "Clock Skew"
          }
        ],
        "fieldConfig": {
          "defaults": {
            "unit": "s",
            "thresholds": {
              "mode": "absolute",
              "steps": [
                { "color": "green", "value": null },
                { "color": "yellow", "value": 1 },
                { "color": "red", "value": 2 }
              ]
            }
          }
        }
      },
      {
        "title": "Log Error/Warning Rate",
        "type": "timeseries",
        "gridPos": { "h": 8, "w": 8, "x": 16, "y": 32 },
        "targets": [
          {
            "expr": "rate(hl_log_lines_total{level=\"error\"}[5m])",
            "legendFormat": "errors/s {{source}}"
          },
          {
            "expr": "rate(hl_log_lines_total{level=\"warn\"}[5m])",
            "legendFormat": "warns/s {{source}}"
          }
        ],
        "fieldConfig": {
          "defaults": {
            "unit": "cps"
          }
        }
      }
    ],
    "templating": {
      "list": []
    },
    "annotations": {
      "list": []
    },
    "schemaVersion": 39,
    "version": 1
  },
  "overwrite": true
}
```

## 4. Dashboard Layout

| Row | Panel | Type | Metrics |
|-----|-------|------|---------|
| **1 — Block Health** | Fast Block Height | stat | `hl_block_height{chain="fast"}` |
| | Propagation Delay | timeseries | `hl_block_propagation_delay_seconds` |
| | Apply Duration | timeseries | `hl_block_apply_duration_seconds` |
| | Block Rate | stat | `rate(hl_blocks_total{chain="fast"}[1m])` |
| | Last Block Age | stat | `hl_monitor_last_block_age_seconds{chain="fast"}` |
| **2 — Latency** | Block Duration (fast) | timeseries | `hl_latency_{median,p90,p95,max}_seconds{category="node_fast_block_duration"}` |
| | Begin Block to Commit | timeseries | `hl_latency_*{category="node_fast_begin_block_to_commit"}` |
| | Tokio Spawn Scheduled | timeseries | `hl_latency_*{category="tokio_spawn_forever_scheduled"}` |
| | TCP LZ4 | timeseries | `hl_latency_*{category="tcp_lz4"}` |
| **3 — Node Health** | Process Status | stat | `hl_process_up` |
| | Process RSS | timeseries | `hl_process_rss_bytes` |
| | Disk Usage | gauge | `hl_disk_use_fraction` |
| | Peer BW & Count | timeseries | `hl_peer_total_{inbound,outbound}_mbps`, `hl_peer_count` |
| **4 — Visor** | Height & Lag | stat | `hl_visor_height`, lag calculation |
| | Clock Skew | timeseries | `hl_visor_clock_skew_seconds` |
| **5 — Logs** | Error/Warn Rate | timeseries | `rate(hl_log_lines_total[5m])` |

## 5. Quick Start

```bash
# 1. Start the monitor
./pyrunner monitor/hl_monitor.py --port 9100

# 2. Verify metrics are exposed
curl -s localhost:9100/metrics | grep hl_block_height

# 3. Start Prometheus (ensure config points to localhost:9100)
sudo systemctl start prometheus

# 4. Start Grafana
sudo systemctl start grafana-server

# 5. Import dashboard JSON via Grafana UI or API:
curl -X POST http://admin:admin@localhost:3000/api/dashboards/db \
  -H 'Content-Type: application/json' \
  -d @hl_node_dashboard.json
```
