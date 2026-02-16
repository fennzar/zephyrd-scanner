#!/bin/bash
# Start a zephyrd test daemon with isolated ports and a specified data directory.
# Usage: ./scripts/start-test-daemon.sh <data-dir> [rpc-port] [max-wait-secs]
# Example: ./scripts/start-test-daemon.sh ./chain-data/chain_89400 18767
#
# The daemon runs with --no-sync --offline to prevent network activity.
# PID is written to <data-dir>/zephyrd.pid for cleanup.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Source env vars (ZEPHYRD_BIN etc.) from .env.test
if [[ -f "$PROJECT_DIR/.env.test" ]]; then
  set -a; source "$PROJECT_DIR/.env.test"; set +a
fi

ZEPHYRD_BIN="${ZEPHYRD_BIN:?ZEPHYRD_BIN not set — add it to .env.test}"

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <data-dir> [rpc-port] [max-wait-secs]"
  exit 1
fi

DATA_DIR="$(realpath "$1")"
RPC_PORT="${2:-18767}"
P2P_PORT=$((RPC_PORT - 1))
PID_FILE="$DATA_DIR/zephyrd.pid"
MAX_WAIT="${3:-120}"

if [[ ! -d "$DATA_DIR" ]]; then
  echo "Error: Data directory does not exist: $DATA_DIR"
  exit 1
fi

if [[ ! -x "$ZEPHYRD_BIN" ]]; then
  echo "Error: zephyrd binary not found or not executable: $ZEPHYRD_BIN"
  exit 1
fi

# Check if a daemon is already running with this PID file
if [[ -f "$PID_FILE" ]]; then
  OLD_PID=$(cat "$PID_FILE")
  if kill -0 "$OLD_PID" 2>/dev/null; then
    echo "Daemon already running (PID $OLD_PID). Stop it first with stop-test-daemon.sh"
    exit 1
  fi
  rm -f "$PID_FILE"
fi

# Clean stale LMDB lock if present
rm -f "$DATA_DIR/lmdb/lock.mdb" 2>/dev/null

echo "Starting zephyrd test daemon..."
echo "  Data dir:  $DATA_DIR"
echo "  RPC port:  $RPC_PORT"
echo "  P2P port:  $P2P_PORT"

"$ZEPHYRD_BIN" \
  --data-dir "$DATA_DIR" \
  --rpc-bind-port "$RPC_PORT" \
  --rpc-bind-ip 127.0.0.1 \
  --p2p-bind-port "$P2P_PORT" \
  --no-zmq \
  --no-sync \
  --offline \
  --non-interactive \
  --log-level 1 \
  --disable-rpc-ban \
  --detach \
  --pidfile "$PID_FILE"

# Wait for RPC to become responsive
echo -n "Waiting for RPC to become responsive"
WAITED=0
while [[ $WAITED -lt $MAX_WAIT ]]; do
  RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" \
    -X POST "http://127.0.0.1:$RPC_PORT/get_height" \
    -H "Content-Type: application/json" 2>/dev/null || true)
  if [[ "$RESPONSE" == "200" ]]; then
    echo " OK"
    HEIGHT=$(curl -s -X POST "http://127.0.0.1:$RPC_PORT/get_height" \
      -H "Content-Type: application/json" | python3 -c "import sys,json; print(json.load(sys.stdin).get('height','?'))" 2>/dev/null || echo "?")
    echo "Daemon is up at height: $HEIGHT"
    exit 0
  fi
  echo -n "."
  sleep 1
  WAITED=$((WAITED + 1))
done

echo " TIMEOUT"
echo "Error: Daemon did not respond within ${MAX_WAIT}s"

# Clean up if we can
if [[ -f "$PID_FILE" ]]; then
  PID=$(cat "$PID_FILE")
  kill "$PID" 2>/dev/null || true
  rm -f "$PID_FILE"
fi

exit 1
