#!/bin/bash
set -e

cd "$(dirname "$0")"

echo "Compiling hl_relay (C++)..."
g++ -std=c++17 -g -O2 -Wall -Wextra -Werror -I. -DSPDLOG_HEADER_ONLY -o hl_relay hl_relay.cc -lpthread
echo "Build OK: relay/hl_relay"
