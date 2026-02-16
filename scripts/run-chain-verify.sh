#!/bin/bash
# Run chain verification tests against a specific chain snapshot.
#
# Usage: ./scripts/run-chain-verify.sh [height] [--delta=FROM_HEIGHT]
#   height: target chain height (e.g., 89400, 90300, ..., or "current")
#           If omitted, runs all heights.
#   --delta=FROM: skip DB reset, continue scanning from FROM_HEIGHT.
#           Use after a previous run to only scan the delta.
#           Example: ./scripts/run-chain-verify.sh 360100 --delta=295100
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

RPC_PORT="${CHAIN_VERIFY_RPC_PORT:-18767}"
TARGET_HEIGHT=""
DELTA_FROM=""

for arg in "$@"; do
  case "$arg" in
    --delta=*) DELTA_FROM="${arg#--delta=}" ;;
    *) TARGET_HEIGHT="$arg" ;;
  esac
done

# Override RPC settings for chain verification
export ZEPHYR_RPC_URL="http://127.0.0.1:$RPC_PORT"
export RPC_TIMEOUT_MS=30000
export CHAIN_VERIFY_RPC_PORT="$RPC_PORT"
# Disable walkthrough mode (per-block RPC comparison) — we only need final comparison
export WALKTHROUGH_MODE=false

if [[ -n "$TARGET_HEIGHT" ]]; then
  export CHAIN_VERIFY_HEIGHT="$TARGET_HEIGHT"
  echo "Running chain verification for height: $TARGET_HEIGHT"
else
  echo "Running chain verification for all heights"
fi

if [[ -n "$DELTA_FROM" ]]; then
  export CHAIN_VERIFY_DELTA="$DELTA_FROM"
  echo "Delta mode: continuing from height $DELTA_FROM (DB state preserved)"
fi

cd "$PROJECT_DIR"
bun test tests/chain-verify.test.ts --timeout 3600000
