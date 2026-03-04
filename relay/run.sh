#!/bin/bash
set -e

cd "$(dirname "$0")"

# Build if binary doesn't exist or source is newer
if [ ! -f hl_relay ] || [ hl_relay.cc -nt hl_relay ]; then
    echo "Building hl_relay..."
    bash build.sh
fi

echo "Starting hl_relay (Ctrl+C to stop)..."
exec ./hl_relay "$@"
