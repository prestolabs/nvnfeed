#!/bin/bash
set -e

cd "$(dirname "$0")"

echo "Compiling hl_client..."
g++ -std=c++17 -O2 -o hl_client hl_client.cc -Iinclude
echo "Build OK"

LOGFILE="hl_client_$(date -u +%Y%m%d_%H%M%S).log"

echo "Logging to: $LOGFILE"
echo "Starting hl_client (Ctrl+C to stop)..."

./hl_client --duration 0 --stats-interval 300 "$@" 2>&1 | tee "$LOGFILE"
