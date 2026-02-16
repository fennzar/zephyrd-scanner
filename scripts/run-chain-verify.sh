#!/bin/bash
# Run chain verification tests against a specific chain snapshot.
# Usage: ./scripts/run-chain-verify.sh [height]
#   height: target chain height (e.g., 89400, 90300, ..., or "current")
#           If omitted, runs all heights.
#
# This script:
#   1. Sets ZEPHYR_RPC_URL to point at the test daemon port
#   2. Sets DATA_STORE=postgres and DATABASE_URL for the test DB
#   3. Runs bun test tests/chain-verify.test.ts
#
# Prerequisites:
#   - Chain snapshots created via create-chain-snapshots.sh
#   - The test starts/stops the daemon automatically

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Source .env.test for DATABASE_URL, DATA_STORE, etc.
if [[ -f "$PROJECT_DIR/.env.test" ]]; then
  set -a; source "$PROJECT_DIR/.env.test"; set +a
fi

RPC_PORT=18767
TARGET_HEIGHT="${1:-}"

# Override RPC settings for chain verification
export ZEPHYR_RPC_URL="http://127.0.0.1:$RPC_PORT"
export RPC_TIMEOUT_MS=30000
export CHAIN_VERIFY_RPC_PORT="$RPC_PORT"

if [[ -n "$TARGET_HEIGHT" ]]; then
  export CHAIN_VERIFY_HEIGHT="$TARGET_HEIGHT"
  echo "Running chain verification for height: $TARGET_HEIGHT"
else
  echo "Running chain verification for all heights"
fi

cd "$PROJECT_DIR"
bun test tests/chain-verify.test.ts --timeout 600000
