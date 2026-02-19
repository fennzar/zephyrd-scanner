# Tests

## Quick Start

```sh
# Chain verification — the primary test (see below)
./scripts/run-chain-verify.sh 89400

# Unit and integration tests (~2s)
bun test
```

## Chain Verification (primary test)

The chain verification suite (`chain-verify.test.ts`) is the main test for the scanner. It runs a full scan cycle against real LMDB chain snapshots at specific block heights, then compares scanner-computed protocol state against on-chain truth from the daemon's `get_reserve_info` and `get_circulating_supply` RPCs.

This is how we know the scanner is correct — every hardfork boundary and edge case is covered by a snapshot.

**This test is not run by `bun test`** — it's driven by `scripts/run-chain-verify.sh` which manages the test daemon lifecycle and environment.

### How it works

For each chain snapshot:

1. **Setup**: starts a test daemon on port 18767, verifies chain height, resets test DB
2. **Scan pricing records**: blocks 89,300 to chain tip
3. **Scan transactions**: block rewards and conversion txs from block 0
4. **Run aggregator**: computes per-block protocol state
5. **Compare**: diffs scanner state against daemon RPC truth

### Snapshot targets

| Height | Label | Rationale |
|---|---|---|
| 100 | `chain_100` | Pre-V1 genesis scan |
| 89,400 | `chain_89400` | +100 blocks after HF V1 |
| 90,300 | `chain_90300` | +1,000 — early conversions |
| 94,300 | `chain_94300` | +5,000 |
| 99,300 | `chain_99300` | +10,000 |
| 139,300 | `chain_139300` | +50,000 |
| 189,300 | `chain_189300` | +100,000 |
| 295,100 | `chain_295100` | ARTEMIS V5 boundary |
| 360,100 | `chain_360100` | V6/YIELD boundary |
| 481,600 | `chain_481600` | AUDIT V8 boundary |
| 536,000 | `chain_536000` | Pre-V11 (blocks 0..535,999) |
| 536,001 | `chain_536001` | V11 fork block |
| 536,002 | `chain_536002` | V11 reset applied |
| 536,100 | `chain_536100` | V11 +100 |
| current | `chain_current` | Full chain tip |

### Comparison fields (15 tests per snapshot)

- `zeph_in_reserve` — ZEPH locked in the Djed reserve
- `zephusd_circ` / `zephrsv_circ` / `zyield_circ` — circulating supplies
- `zsd_in_yield_reserve` — ZSD in yield reserve
- `reserve_ratio` — Djed reserve ratio
- Net totals (ZSD, ZRS, ZYS circ) cross-checked against daemon
- Health table output for visual inspection

### Running

```sh
# Single snapshot
./scripts/run-chain-verify.sh 89400

# All available snapshots
./scripts/run-chain-verify.sh

# Delta mode — skip DB reset, scan only from a checkpoint
./scripts/run-chain-verify.sh 360100 --delta=295100
```

For full details on creating snapshots, the test daemon, and infrastructure, see [docs/chain-verification.md](../docs/chain-verification.md).

---

## Unit and Integration Tests

Run with `bun test` (~2 seconds). These don't require chain snapshots.

### Test Runner

Tests use [Bun's built-in test runner](https://bun.sh/docs/cli/test). Configuration is in `bunfig.toml`:

```toml
[test]
preload = ["./tests/setup/env.ts"]
```

The preload script loads `.env.test` **before** any `src/` imports. This is critical because `src/config.ts` caches `DATA_STORE` at module load time — without preloading, tests would pick up values from `.env` instead.

### Environment

Tests use a dedicated Postgres database configured in `.env.test`:

```
DATA_STORE=postgres
DATABASE_URL=postgresql://zephyrdscanner:zephyrdscanner@192.168.1.110:5432/zephyrdscanner_test
```

The test database is reset via `prisma db push --force-reset` before each test suite. This requires the `PRISMA_USER_CONSENT_FOR_DANGEROUS_AI_ACTION` env var (handled automatically by `setup/db.ts`).

### Test Setup

| File | Purpose |
|---|---|
| `setup/env.ts` | Preload: loads `.env.test` before `src/` modules import |
| `setup/db.ts` | `setupTestDatabase()`, `resetTestData()`, `teardownTestDatabase()` helpers |

### `config.test.ts` (6 tests)

Verifies that environment config functions return correct values under `DATA_STORE=postgres`:
- `getDataStoreMode()` returns `"postgres"`
- `useRedis()` returns `false`, `usePostgres()` returns `true`
- `getStartBlock()` / `getEndBlock()` parse env vars correctly

### `redis-gating.test.ts` (20 tests)

Ensures that when `DATA_STORE=postgres`, **zero Redis I/O occurs**. Every data access function (`getLatestProtocolStats`, `getPricingRecords`, `getTransactions`, etc.) is called and verified to return empty/null results from Postgres while `redis.status` stays at `"wait"` (the `lazyConnect` idle state). Covers functions from `utils.ts`, `yield.ts`, and `pr.ts`.

### `storage.test.ts` (8 tests)

Round-trip tests for the storage abstraction layer (`src/storage/factory.ts`):
- `scannerState`: set/get, missing keys, overwrites
- `pricing`: save/get, `getLatestHeight()`, missing records, empty table returns `-1`

All operations verify Redis stays idle.

### `scanner-integration.test.ts` (5 tests)

End-to-end integration test that runs the actual scan pipeline against a **live local daemon** (`127.0.0.1:17767`) over a small block range (blocks 89,300–89,304):

1. `scanPricingRecords()` — saves 5 pricing records to the test DB
2. Pricing record values are realistic (positive, non-zero)
3. `scanTransactions()` — saves block rewards and advances `height_txs`
4. `aggregate()` — creates `ProtocolStatsBlock` rows with correct heights
5. Redis stays idle throughout

Requires a running daemon. Timeout: 30s per test.

## Test Counts

| File | Tests |
|---|---|
| `config.test.ts` | 6 |
| `redis-gating.test.ts` | 20 |
| `storage.test.ts` | 8 |
| `scanner-integration.test.ts` | 5 |
| **Unit/integration total** | **39** |
| `chain-verify.test.ts` | 15 per snapshot |
