# Chain Verification System

Tooling for creating LMDB chain snapshots at specific block heights and running the scanner against them to compare scanner-computed state against on-chain truth. Used to isolate where reserve drift is introduced across the chain history.

## Quick Start

```bash
# 1. Create a snapshot at height 89,400 (100 blocks after HF)
./scripts/create-chain-snapshots.sh 89400

# 2. Run the chain verification test
./scripts/run-chain-verify.sh 89400

# 3. Create all snapshots
./scripts/create-chain-snapshots.sh
```

## How It Works

1. **Snapshot creation** syncs a fresh zephyrd instance from the local production daemon up to the target height using `--test-drop-download-height`. If it overshoots, it pops back to the exact target.
2. **Chain verification** starts a test daemon on an isolated port (18767), runs the full scanner pipeline (pricing records, transactions, aggregation), then compares protocol stats at the chain tip against the daemon's `get_reserve_info` and `get_circulating_supply` RPCs.

## Target Heights

| Height  | Label            | Rationale                                      |
|---------|------------------|-------------------------------------------------|
| 100     | `chain_100`      | Pre-V1 genesis scan                             |
| 89,400  | `chain_89400`    | +100 blocks after HF V1 start                   |
| 90,300  | `chain_90300`    | +1,000 — early conversion activity               |
| 94,300  | `chain_94300`    | +5,000                                           |
| 99,300  | `chain_99300`    | +10,000                                          |
| 139,300 | `chain_139300`   | +50,000                                          |
| 189,300 | `chain_189300`   | +100,000                                         |
| 295,100 | `chain_295100`   | ARTEMIS V5 boundary (conversion fee changes)     |
| 360,100 | `chain_360100`   | V6/YIELD boundary (yield starts)                 |
| 481,600 | `chain_481600`   | AUDIT V8 boundary (asset audit txs start)        |
| 536,000 | `chain_536000`   | Pre-V11 (blocks 0..535,999)                      |
| 536,001 | `chain_536001`   | V11 fork block (asset renames)                   |
| 536,002 | `chain_536002`   | V11 circ reset applied                           |
| 536,100 | `chain_536100`   | V11 +100                                         |
| current | `chain_current`  | Full chain tip                                   |

## Scripts

### `scripts/create-chain-snapshots.sh [height ...]`

Creates LMDB snapshots under `chain-data/`. Automatically starts the production daemon if it isn't running.

- No arguments: creates all 7 snapshots
- With arguments: creates only the specified heights (e.g., `89400 90300`)
- Skips snapshots that already exist (resumable)
- Clears the txpool from each snapshot to ensure fast daemon startup

**Prerequisites:**
- Production chain data at `~/.zephyr/` (~12GB LMDB)
- ~600MB–12GB free disk per snapshot (smaller chains = smaller snapshots)

**Timing:** ~12 minutes per 89K blocks of sync on NVMe.

### `scripts/start-test-daemon.sh <data-dir> [rpc-port] [max-wait-secs]`

Starts a zephyrd daemon in `--no-sync --offline` mode with isolated ports. Waits for RPC to become responsive before returning.

- Default RPC port: 18767 (P2P: 18766)
- Writes PID to `<data-dir>/zephyrd.pid`
- Clears stale LMDB lock files automatically

### `scripts/stop-test-daemon.sh <data-dir>`

Stops a daemon by reading its PID file. Sends SIGTERM, waits 30s, falls back to SIGKILL.

### `scripts/run-chain-verify.sh [height]`

Convenience wrapper that sets environment variables and runs `bun test tests/chain-verify.test.ts`.

- With height: `./scripts/run-chain-verify.sh 89400`
- Without: runs verification for all available snapshots

## Test File

### `tests/chain-verify.test.ts`

For each target snapshot:

1. **Setup**: starts test daemon, verifies chain height, resets test DB
2. **scan pricing records**: scans blocks 89,300 to chain tip
3. **scan transactions**: scans block rewards and conversion txs
4. **run aggregator**: computes protocol stats for each block
5. **compare scanner vs on-chain state**: queries `get_reserve_info` and `get_circulating_supply`, diffs against scanner's protocol stats

Comparison fields:
- `zeph_in_reserve` — ZEPH locked in the Djed reserve
- `zephusd_circ` — ZephUSD circulating supply
- `zephrsv_circ` — ZephRSV circulating supply
- `zyield_circ` — ZYield circulating supply
- `zsd_in_yield_reserve` — ZSD in yield reserve
- `reserve_ratio` — Djed reserve ratio

The final comparison is intentionally a soft assertion (logged, not failing) so you can iteratively fix the aggregator and re-run without the test blocking on known drift.

## Environment Variables

| Variable | Default | Purpose |
|---|---|---|
| `ZEPHYR_RPC_URL` | `http://127.0.0.1:17767` | Override daemon RPC endpoint (set by `run-chain-verify.sh`) |
| `CHAIN_VERIFY_HEIGHT` | (all) | Run verification for a specific height only |
| `CHAIN_VERIFY_RPC_PORT` | `18767` | RPC port for the test daemon |

## Directory Structure

```
chain-data/                    # gitignored
  chain_89400/lmdb/data.mdb    # ~600MB
  chain_90300/lmdb/data.mdb
  ...
  chain_current/lmdb/data.mdb  # ~12GB
scripts/
  create-chain-snapshots.sh
  start-test-daemon.sh
  stop-test-daemon.sh
  run-chain-verify.sh
tests/
  chain-verify.test.ts
```

## Patched Daemon

Chain verification requires a **patched zephyrd** for accurate ZEPH circulating supply comparisons. The stock daemon's `get_circulating_supply` RPC returns the DJED reserve LMDB tally for ZEPH/ZPH, not the true total supply. This makes the ZEPH circ comparison fail at every height.

The patch is on the `fix/circulating-supply-zeph-total` branch in the `../zephyr` repo (stashed):

```sh
cd ../zephyr
git checkout fix/circulating-supply-zeph-total
git stash pop
```

### What the patch does

It overrides the ZEPH/ZPH entry in the `get_circulating_supply` RPC response to return `already_generated_coins` (the authoritative cumulative emission from coinbase) instead of the DJED tally:

```cpp
// src/rpc/core_rpc_server.cpp — on_get_circulating_supply()

// Override ZEPH/ZPH value with true total supply from already_generated_coins.
// The LMDB tallies (V1 "ZEPH" and V2 "ZPH") only track DJED reserve flows,
// not total ZEPH supply. m_coinbase is the authoritative cumulative emission.
// This override is intentionally in the RPC handler (not the DB method) so that
// get_reserve_info — which also reads get_circulating_supply() — still sees the
// original DJED tally values for its reserve calculations.
uint64_t current_height = m_core.get_current_blockchain_height();
if (current_height > 0) {
  uint64_t coinbase = m_core.get_blockchain_storage().get_db().get_block_already_generated_coins(current_height - 1);
  for (auto& supply : amounts) {
    if (supply.first == "ZPH" || supply.first == "ZEPH") {
      supply.second = std::to_string(coinbase);
      break;
    }
  }
}
```

Key design decision: the override is in the RPC handler only, not in the underlying DB method. This means `get_reserve_info` (which internally calls `get_circulating_supply()` at the DB level) still sees the original DJED tally for its reserve ratio calculations — only the external RPC response is corrected.

Without this patch, the ZEPH circ test will show large discrepancies at every height. The other fields (ZSD, ZRS, ZYS, reserve, yield reserve, reserve ratio) are unaffected.

### Building

```sh
cd ../zephyr
mkdir -p build/Linux/master/release && cd build/Linux/master/release
cmake -DCMAKE_BUILD_TYPE=Release ../../../..
make -j$(nproc) daemon
```

The built binary path (`ZEPHYRD_BIN`) is configured in `.env.test`.

## Notes

- **Scanner `END_BLOCK` is exclusive**: chain at height N has blocks 0..N-1. The scanner processes `START_BLOCK` through `END_BLOCK-1`.
- **Txpool clearing**: after creating a snapshot, the txpool tables (`txpool_blob`, `txpool_meta`) are dropped from the LMDB using `mdb_drop` (compiled from Zephyr's bundled LMDB source). Without this, the daemon spends 90+ minutes re-validating 123K+ stale txpool entries on startup.
- **Production daemon**: `create-chain-snapshots.sh` starts the production daemon with `--no-sync` (accepts P2P connections but doesn't initiate sync). This allows the sync daemon to pull blocks from it.
- **`--test-drop-download-height`**: zephyrd flag that syncs normally up to the specified height, then discards all blocks after it. May overshoot by a few blocks; the script pops back to the exact target.
