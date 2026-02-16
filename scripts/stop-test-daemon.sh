#!/bin/bash
# Stop a test zephyrd daemon by PID file.
# Usage: ./scripts/stop-test-daemon.sh <data-dir>
# Example: ./scripts/stop-test-daemon.sh ./chain-data/chain_89400

set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <data-dir>"
  exit 1
fi

DATA_DIR="$(realpath "$1")"
PID_FILE="$DATA_DIR/zephyrd.pid"

if [[ ! -f "$PID_FILE" ]]; then
  echo "No PID file found at $PID_FILE — daemon may not be running"
  exit 0
fi

PID=$(cat "$PID_FILE")

if ! kill -0 "$PID" 2>/dev/null; then
  echo "Process $PID is not running, cleaning up stale PID file"
  rm -f "$PID_FILE"
  exit 0
fi

echo "Stopping zephyrd daemon (PID $PID)..."
kill "$PID"

# Wait for graceful shutdown
MAX_WAIT=30
WAITED=0
while kill -0 "$PID" 2>/dev/null && [[ $WAITED -lt $MAX_WAIT ]]; do
  sleep 1
  WAITED=$((WAITED + 1))
done

if kill -0 "$PID" 2>/dev/null; then
  echo "Daemon did not stop after ${MAX_WAIT}s, sending SIGKILL..."
  kill -9 "$PID" 2>/dev/null || true
  sleep 1
fi

rm -f "$PID_FILE"
echo "Daemon stopped."
