#!/bin/bash
# Create LMDB chain snapshots at specific heights for chain verification tests.
#
# Strategy: for the first snapshot, sync from scratch against the local production
# node using --test-drop-download-height. For subsequent snapshots, copy the
# previous snapshot's LMDB and sync up from there — only the delta needs syncing.
#
# Prerequisites:
#   - Production zephyrd chain data at ~/.zephyr/ (need not be running — script
#     will start it if necessary)
#   - ~12GB free disk per snapshot
#
# Usage: ./scripts/create-chain-snapshots.sh [height ...]
#   No args     → create all snapshots
#   89400 90300 → create only those two
#
# Produces snapshots under ./chain-data/:
#   chain_89400, chain_90300, chain_94300, chain_99300,
#   chain_139300, chain_189300, chain_current

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
CHAIN_DATA_DIR="$PROJECT_DIR/chain-data"

# Source env vars (ZEPHYRD_BIN, ZEPHYR_LMDB_SRC etc.) from .env.test
if [[ -f "$PROJECT_DIR/.env.test" ]]; then
  set -a; source "$PROJECT_DIR/.env.test"; set +a
fi

ZEPHYRD_BIN="${ZEPHYRD_BIN:?ZEPHYRD_BIN not set — add it to .env.test}"
PROD_DATA_DIR="$HOME/.zephyr"
PROD_RPC_PORT=17767
PROD_P2P_PORT=17766

# Sync daemon uses these isolated ports
SYNC_RPC_PORT=18767
SYNC_P2P_PORT=18766

STOP_DAEMON="$SCRIPT_DIR/stop-test-daemon.sh"

# All available target heights (ascending order for sync efficiency)
# Includes hardfork boundary heights for comprehensive testing
ALL_HEIGHTS=(89400 90300 94300 99300 139300 189300 295100 360100 481600 536100)

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

get_height() {
  local port=$1
  curl -s --max-time 5 -X POST "http://127.0.0.1:$port/get_height" \
    -H "Content-Type: application/json" 2>/dev/null \
    | python3 -c "import sys,json; print(json.load(sys.stdin).get('height', 0))" 2>/dev/null || echo "0"
}

wait_for_rpc() {
  local port=$1
  local max_wait=${2:-120}
  local waited=0
  while [[ $waited -lt $max_wait ]]; do
    local code
    code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 3 \
      -X POST "http://127.0.0.1:$port/get_height" \
      -H "Content-Type: application/json" 2>/dev/null || echo "000")
    if [[ "$code" == "200" ]]; then
      return 0
    fi
    sleep 1
    waited=$((waited + 1))
  done
  return 1
}

# ---- Production daemon management ----

ensure_production_daemon() {
  log "Checking production daemon on port $PROD_RPC_PORT..."

  local height
  height=$(get_height "$PROD_RPC_PORT")

  if [[ "$height" != "0" && -n "$height" ]]; then
    log "Production daemon is running at height $height"
    return 0
  fi

  log "Production daemon not responding. Checking for stale process..."
  local prod_pid_file="$PROD_DATA_DIR/zephyrd.pid"

  # Kill stale process if PID file exists
  if [[ -f "$prod_pid_file" ]]; then
    local old_pid
    old_pid=$(cat "$prod_pid_file")
    if kill -0 "$old_pid" 2>/dev/null; then
      log "Found stale daemon (PID $old_pid), waiting for it to finish starting..."
      if wait_for_rpc "$PROD_RPC_PORT" 60; then
        height=$(get_height "$PROD_RPC_PORT")
        log "Production daemon came up at height $height"
        return 0
      fi
      log "Stale daemon unresponsive. Killing PID $old_pid..."
      kill "$old_pid" 2>/dev/null; sleep 3; kill -9 "$old_pid" 2>/dev/null; sleep 1
    fi
    rm -f "$prod_pid_file"
  fi

  if [[ ! -d "$PROD_DATA_DIR/lmdb" ]]; then
    log "ERROR: No production chain data at $PROD_DATA_DIR/lmdb"
    exit 1
  fi

  # Clean stale LMDB lock
  rm -f "$PROD_DATA_DIR/lmdb/lock.mdb" 2>/dev/null

  log "Starting production daemon..."
  "$ZEPHYRD_BIN" \
    --data-dir "$PROD_DATA_DIR" \
    --rpc-bind-port "$PROD_RPC_PORT" \
    --rpc-bind-ip 127.0.0.1 \
    --p2p-bind-port "$PROD_P2P_PORT" \
    --no-zmq \
    --no-sync \
    --non-interactive \
    --log-level 1 \
    --disable-rpc-ban \
    --detach \
    --pidfile "$PROD_DATA_DIR/zephyrd.pid"

  log "Waiting for production daemon RPC..."
  if ! wait_for_rpc "$PROD_RPC_PORT" 300; then
    log "ERROR: Production daemon failed to start within 300s"
    exit 1
  fi

  height=$(get_height "$PROD_RPC_PORT")
  log "Production daemon started at height $height"
  STARTED_PROD_DAEMON=true
}

# ---- Sync a chain to a target height ----
# Usage: sync_to_height <target_height> [base_data_dir]
# If base_data_dir is provided, copies its LMDB first so only the delta needs syncing.

sync_to_height() {
  local target_height=$1
  local base_dir="${2:-}"
  local label="chain_${target_height}"
  local data_dir="$CHAIN_DATA_DIR/$label"
  local pid_file="$data_dir/zephyrd.pid"

  if [[ -d "$data_dir/lmdb" ]]; then
    # Verify the existing snapshot is at the right height
    log "$label already exists, verifying..."

    rm -f "$data_dir/lmdb/lock.mdb" 2>/dev/null

    "$ZEPHYRD_BIN" \
      --data-dir "$data_dir" \
      --rpc-bind-port "$SYNC_RPC_PORT" \
      --rpc-bind-ip 127.0.0.1 \
      --p2p-bind-port "$SYNC_P2P_PORT" \
      --no-zmq --no-sync --offline --non-interactive \
      --log-level 0 \
      --detach --pidfile "$pid_file" 2>/dev/null

    if wait_for_rpc "$SYNC_RPC_PORT" 60; then
      local existing_height
      existing_height=$(get_height "$SYNC_RPC_PORT")
      "$STOP_DAEMON" "$data_dir" 2>/dev/null || true
      sleep 2

      if [[ "$existing_height" == "$target_height" ]]; then
        log "$label verified at height $existing_height — skipping"
        return 0
      else
        log "$label at wrong height ($existing_height, expected $target_height) — recreating"
        rm -rf "$data_dir"
      fi
    else
      "$STOP_DAEMON" "$data_dir" 2>/dev/null || true
      sleep 2
      log "$label exists but daemon won't start — recreating"
      rm -rf "$data_dir"
    fi
  fi

  log "=== Creating $label (syncing to height $target_height) ==="
  mkdir -p "$data_dir"

  # If a base snapshot was provided, copy its LMDB so we only sync the delta
  if [[ -n "$base_dir" && -d "$base_dir/lmdb" ]]; then
    log "Copying base LMDB from $(basename "$base_dir")..."
    cp -a "$base_dir/lmdb" "$data_dir/lmdb"
    rm -f "$data_dir/lmdb/lock.mdb" 2>/dev/null
    log "Base copy done — syncing delta to $target_height"
  fi

  # Start daemon that syncs from the local production node
  # --test-drop-download-height causes it to discard blocks after the target
  # --add-exclusive-node syncs only from our local production daemon
  log "Starting sync daemon → target height $target_height"

  rm -f "$data_dir/lmdb/lock.mdb" 2>/dev/null

  "$ZEPHYRD_BIN" \
    --data-dir "$data_dir" \
    --rpc-bind-port "$SYNC_RPC_PORT" \
    --rpc-bind-ip 127.0.0.1 \
    --p2p-bind-port "$SYNC_P2P_PORT" \
    --no-zmq \
    --non-interactive \
    --log-level 1 \
    --disable-rpc-ban \
    --add-exclusive-node "127.0.0.1:$PROD_P2P_PORT" \
    --test-drop-download-height "$target_height" \
    --fast-block-sync 1 \
    --db-sync-mode fastest \
    --detach \
    --pidfile "$pid_file"

  # Wait for RPC to become available (daemon is starting + syncing)
  log "Waiting for sync daemon RPC..."
  if ! wait_for_rpc "$SYNC_RPC_PORT" 120; then
    log "ERROR: Sync daemon failed to start"
    "$STOP_DAEMON" "$data_dir" 2>/dev/null || true
    return 1
  fi

  # Monitor sync progress — wait 60s before first check, then every 30s
  log "  Waiting 60s for initial sync..."
  sleep 60

  local prev_height=0
  local stall_count=0
  local max_stall=10  # consecutive stalled checks (at 30s each = 5min)

  while true; do
    local current_height
    current_height=$(get_height "$SYNC_RPC_PORT")

    if [[ "$current_height" == "0" || -z "$current_height" ]]; then
      log "  WARNING: Daemon not responding, waiting..."
      sleep 10
      stall_count=$((stall_count + 1))
      if [[ $stall_count -gt 20 ]]; then
        log "ERROR: Daemon unresponsive for too long"
        "$STOP_DAEMON" "$data_dir" 2>/dev/null || true
        return 1
      fi
      continue
    fi

    local pct=0
    if [[ "$target_height" -gt 0 ]]; then
      pct=$((current_height * 100 / target_height))
    fi
    log "  Syncing: height $current_height / $target_height ($pct%)"

    if [[ "$current_height" -ge "$target_height" ]]; then
      log "  Reached target height: $current_height"
      break
    fi

    if [[ "$current_height" == "$prev_height" ]]; then
      stall_count=$((stall_count + 1))
      if [[ $stall_count -ge $max_stall ]]; then
        if [[ "$current_height" -ge $((target_height - 10)) ]]; then
          log "  Close enough to target ($current_height), stopping"
          break
        fi
        log "ERROR: Sync stalled at height $current_height for ${max_stall} checks"
        "$STOP_DAEMON" "$data_dir" 2>/dev/null || true
        return 1
      fi
    else
      stall_count=0
    fi

    prev_height=$current_height
    sleep 30
  done

  # Stop the sync daemon
  local final_height
  final_height=$(get_height "$SYNC_RPC_PORT")
  log "Stopping sync daemon at height $final_height"
  "$STOP_DAEMON" "$data_dir"
  sleep 2

  # If we overshot, pop back to exact target
  if [[ "$final_height" -gt "$target_height" ]]; then
    local overshoot=$((final_height - target_height))
    log "Overshot by $overshoot blocks, popping back..."

    rm -f "$data_dir/lmdb/lock.mdb" 2>/dev/null

    "$ZEPHYRD_BIN" \
      --data-dir "$data_dir" \
      --rpc-bind-port "$SYNC_RPC_PORT" \
      --rpc-bind-ip 127.0.0.1 \
      --p2p-bind-port "$SYNC_P2P_PORT" \
      --no-zmq --no-sync --offline --non-interactive \
      --log-level 1 --disable-rpc-ban \
      --detach --pidfile "$pid_file"

    if wait_for_rpc "$SYNC_RPC_PORT" 60; then
      curl -s -X POST "http://127.0.0.1:$SYNC_RPC_PORT/pop_blocks" \
        -H "Content-Type: application/json" \
        -d "{\"nblocks\": $overshoot}" > /dev/null

      final_height=$(get_height "$SYNC_RPC_PORT")
      log "After pop: height $final_height"
    fi

    "$STOP_DAEMON" "$data_dir" 2>/dev/null || true
    sleep 2
  fi

  # Clear txpool from the snapshot (avoids slow re-validation on future starts)
  clear_txpool "$data_dir/lmdb"

  log "$label created at height $final_height"
}

# ---- Create chain_current (copy of production chain) ----

create_current_snapshot() {
  local dest="$CHAIN_DATA_DIR/chain_current"

  if [[ -d "$dest/lmdb" ]]; then
    log "chain_current already exists, skipping..."
    return 0
  fi

  local prod_height
  prod_height=$(get_height "$PROD_RPC_PORT")
  log "=== Creating chain_current (height $prod_height) ==="
  log "Copying production LMDB... (this may take a while for ~12GB)"

  mkdir -p "$dest"
  cp -a "$PROD_DATA_DIR/lmdb" "$dest/lmdb"
  clear_txpool "$dest/lmdb"

  log "chain_current created at height $prod_height"
}

# ---- Txpool clearing ----

clear_txpool() {
  local lmdb_dir="$1"
  local mdb_drop_bin=""

  if command -v mdb_drop &>/dev/null; then
    mdb_drop_bin="mdb_drop"
  elif [[ -x /tmp/mdb_drop ]]; then
    mdb_drop_bin="/tmp/mdb_drop"
  else
    local lmdb_src="${ZEPHYR_LMDB_SRC:-}"
    if [[ -f "$lmdb_src/mdb_drop.c" ]]; then
      gcc -o /tmp/mdb_drop "$lmdb_src/mdb_drop.c" "$lmdb_src/mdb.c" "$lmdb_src/midl.c" \
        -lpthread -I"$lmdb_src" 2>/dev/null && mdb_drop_bin="/tmp/mdb_drop"
    fi
  fi

  if [[ -z "$mdb_drop_bin" ]]; then
    log "  WARNING: mdb_drop not available — txpool not cleared"
    return 0
  fi

  rm -f "$lmdb_dir/lock.mdb" 2>/dev/null
  "$mdb_drop_bin" -d -s txpool_blob "$lmdb_dir" 2>/dev/null && log "  txpool_blob cleared" || true
  "$mdb_drop_bin" -d -s txpool_meta "$lmdb_dir" 2>/dev/null && log "  txpool_meta cleared" || true
}

# ---- Main ----

STARTED_PROD_DAEMON=false
mkdir -p "$CHAIN_DATA_DIR"

# Determine which heights to create
REQUESTED_HEIGHTS=()
if [[ $# -gt 0 ]]; then
  for arg in "$@"; do
    if [[ "$arg" == "current" ]]; then
      REQUESTED_HEIGHTS+=("current")
    else
      REQUESTED_HEIGHTS+=("$arg")
    fi
  done
else
  REQUESTED_HEIGHTS=("${ALL_HEIGHTS[@]}" "current")
fi

# Compile mdb_drop if needed (do it once up front)
if ! command -v mdb_drop &>/dev/null && [[ ! -x /tmp/mdb_drop ]]; then
  local_src="${ZEPHYR_LMDB_SRC:-}"
  if [[ -f "$local_src/mdb_drop.c" ]]; then
    log "Compiling mdb_drop..."
    gcc -o /tmp/mdb_drop "$local_src/mdb_drop.c" "$local_src/mdb.c" "$local_src/midl.c" \
      -lpthread -I"$local_src" 2>/dev/null && log "mdb_drop ready" || log "WARNING: mdb_drop compilation failed"
  fi
fi

# Ensure production daemon is running (needed as sync source)
ensure_production_daemon

# Find the best existing snapshot to use as a base for a given target height.
# Searches ALL_HEIGHTS for the highest existing snapshot below the target.
find_base_snapshot() {
  local target=$1
  local best=""
  for h in "${ALL_HEIGHTS[@]}"; do
    if [[ "$h" -lt "$target" && -d "$CHAIN_DATA_DIR/chain_${h}/lmdb" ]]; then
      best="$CHAIN_DATA_DIR/chain_${h}"
    fi
  done
  echo "$best"
}

# Create each requested snapshot, using nearest existing lower snapshot as base
for height in "${REQUESTED_HEIGHTS[@]}"; do
  if [[ "$height" == "current" ]]; then
    create_current_snapshot
  else
    base_dir=$(find_base_snapshot "$height")
    sync_to_height "$height" "$base_dir"
  fi
done

# If we started the production daemon, offer to stop it
if [[ "$STARTED_PROD_DAEMON" == "true" ]]; then
  log "Note: Production daemon was started by this script (PID file: $PROD_DATA_DIR/zephyrd.pid)"
  log "Stop it with: ./scripts/stop-test-daemon.sh $PROD_DATA_DIR"
fi

log ""
log "=== All snapshots created ==="
log "Snapshots:"
for dir in "$CHAIN_DATA_DIR"/chain_*; do
  if [[ -d "$dir/lmdb" ]]; then
    name=$(basename "$dir")
    size=$(du -sh "$dir/lmdb" 2>/dev/null | cut -f1)
    echo "  $name — $size"
  fi
done
