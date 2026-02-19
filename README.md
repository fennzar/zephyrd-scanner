# zephyrd-scanner

Scanner and API server for the Zephyr blockchain. Indexes pricing records, conversion transactions, block rewards, and protocol state (reserve, circulating supplies, yield) by scanning the chain from genesis.

## Getting Started

### Prerequisites

- Node.js 18+ (or [Bun](https://bun.sh) — required for running tests)
- PostgreSQL database
- A running `zephyrd` daemon (local or remote)

### Setup

```bash
npm install   # or: bun install

# Copy and configure environment
cp .env.example .env
# Edit .env — set DATABASE_URL and ZEPHYR_RPC_URL at minimum

# Apply database schema
npx prisma migrate deploy
```

### Running

```bash
# Start the scanner + HTTP API server
npm run start

# Scanner only (no HTTP server)
npm run scanner
```

`npm run` and `bun run` are interchangeable for all scripts — they use `npx tsx` under the hood.

The scanner polls the daemon every 30 seconds (configurable via `MAIN_SLEEP_MS`), scanning new blocks for pricing records and transactions, then running the aggregator to compute protocol state.

### Testing

```bash
# Unit/integration tests (~2s)
bun test

# Chain verification against daemon snapshots (see docs/chain-verification.md)
./scripts/run-chain-verify.sh 89400
```

See [tests/README.md](tests/README.md) for details on each test suite, setup, and chain verification.

## Configuration

All settings are in `.env`. Key variables:

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | — | PostgreSQL connection string |
| `DATA_STORE` | `postgres` | `postgres`, `redis`, or `hybrid` |
| `ZEPHYR_RPC_URL` | `http://127.0.0.1:17767` | Daemon RPC endpoint |
| `ONLINE` | `true` | Connect to daemon for live data |
| `ENABLE_SERVER` | `true` | Start the HTTP API server |
| `MAIN_SLEEP_MS` | `30000` | Poll interval between scan cycles |
| `START_BLOCK` / `END_BLOCK` | — | Limit scan range (for dev/testing) |
| `WALKTHROUGH_MODE` | `false` | Single cycle then exit |

See `.env.example` for the full list including Redis, reserve snapshot, and export settings.

## Data Store

PostgreSQL is the primary data store. The scanner writes all indexed data (pricing records, transactions, block rewards, protocol stats) to Postgres via Prisma.

### Store modes

- **`postgres`** — default; all reads and writes go to PostgreSQL.
- **`redis`** — legacy path; everything in Redis.
- **`hybrid`** — dual-writes to both stores for validation during migration.

### Postgres commands

| Command | Description |
|---|---|
| `npx prisma migrate deploy` | Apply migrations |
| `npx prisma studio` | Web UI for inspecting tables |
| `npm run db:reset` | Drop and recreate the database |
| `npm run db:backup` | `pg_dump` to `backups/` |
| `npm run db:restore-sql -- --file <path>` | Replay a backup |
| `npm run compare-stores` | Parity check between Redis and Postgres |
| `npm run db:migrate-from-redis` | Bulk import from Redis to Postgres |

### Redis configuration

When using `DATA_STORE=redis` or `hybrid`, configure Redis via:

- `REDIS_URL` — full connection string (overrides individual fields)
- `REDIS_HOST`, `REDIS_PORT`, `REDIS_PASSWORD`, `REDIS_DB`
- `REDIS_SOURCE_DB` — source DB index for the background preparation script

## API

The scanner exposes a public HTTP API (default port 4000).

### Public Routes

| Route | Description |
|---|---|
| `GET /` | Health check |
| `GET /stats` | Protocol statistics by `scale` (`block`, `hour`, `day`) |
| `GET /txs` | Conversion transactions with pagination and filtering |
| `GET /blockrewards` | Per-block reward breakdowns |
| `GET /pricingrecords` | Pricing records by height range |
| `GET /reservesnapshots` | Reserve snapshots by height |
| `GET /livestats` | Current protocol state (prices, supplies, reserve) |
| `GET /historicalreturns` | Historical yield returns |
| `GET /projectedreturns` | Projected yield returns |
| `GET /zyspricehistory` | ZYS price history |
| `GET /apyhistory` | APY history |

### Private Routes (localhost only)

| Route | Description |
|---|---|
| `GET /rollback?height=N` | Roll back to height N and rebuild |
| `GET /retallytotals` | Recompute cumulative totals |
| `GET /redetermineapyhistory` | Rebuild APY history |
| `GET /reservediff` | Compare daemon vs cached reserve state |
| `POST /reset?scope=aggregation` | Rebuild aggregation (`full` to rescan everything) |

### Query parameters

Most list endpoints support:

- **`from` / `to`** — height or timestamp range
- **`limit`** — max rows (default varies; `limit=all` for everything)
- **`offset`** — skip N rows
- **`order`** — `asc` (default) or `desc`
- **`fields`** — comma-separated field filter (for `/stats`)
- **`types`** — comma-separated conversion types (for `/txs`)
- **`page` / `pageSize`** — page-based pagination (for `/txs`)

See [docs/api-examples.md](docs/api-examples.md) for curl examples of every endpoint, and [docs/api.md](docs/api.md) for full JSON schemas.

### Generating API docs

```bash
npm run generate-api-docs    # Regenerate docs/api.md from TypeScript types
npm run generate-schemas     # Refresh raw JSON schemas only
```

Schema config is in `schema/schema.config.json`; schemas are generated from `src/api-types.ts`.

## Walkthrough Mode

Walkthrough mode runs a single full scan cycle then exits — useful for building a complete local database or debugging.

```bash
npm run walkthrough
```

Set `WALKTHROUGH_MODE=true` directly if running via `npm run start`. The scanner captures pricing records, conversion transactions, block rewards, totals, and protocol state for the entire chain in one pass.

## Background Scanning

Run a second scanner instance for re-scanning without disrupting production:

```bash
# Scanner only, no HTTP server
ENABLE_SERVER=false npm run scanner
```

For Redis-based setups, use separate DB indexes:

```bash
# Background rescan into DB 1
REDIS_DB=1 ENABLE_SERVER=false npm run bg-scan

# Seed target DB before scanning
npm run prepare-bg -- --target-db=1 --mode=soft   # Keep raw data, clear aggregates
npm run prepare-bg -- --target-db=1 --mode=hard   # Flush everything

# Swap when done
redis-cli SWAPDB 0 1
```

## Reserve Snapshots

The scanner periodically captures daemon `get_reserve_info` snapshots when the aggregated height matches the daemon height. Configuration:

| Variable | Default | Description |
|---|---|---|
| `RESERVE_SNAPSHOT_INTERVAL_BLOCKS` | `720` | Minimum block gap between snapshots |
| `RESERVE_SNAPSHOT_START_HEIGHT` | `89300` | First height for auto-snapshots |
| `RESERVE_SNAPSHOT_SOURCE` | `redis` | Snapshot store (`redis` or `file`) |

Scripts for managing snapshots:

```bash
# Export snapshots to JSON files
npx tsx src/scripts/exportReserveSnapshots.ts --dir reserve_snapshots

# Import JSON snapshots into Redis
npx tsx src/scripts/importReserveSnapshots.ts --dir reserve_snapshots

# Rapid live snapshot capture
npx tsx src/scripts/redisReserveSnapshotter.ts
```

## Chain Verification

The project includes a chain verification test suite that compares scanner-computed protocol state against on-chain daemon truth at specific block heights. This validates correctness across hardfork boundaries.

See [docs/chain-verification.md](docs/chain-verification.md) for full details on creating snapshots and running verification tests.

## Project Structure

```
src/
  index.ts              # Entry point (scanner + HTTP server)
  scanner-runner.ts     # Scanner-only entry point
  config.ts             # Environment and data store configuration
  aggregator.ts         # Per-block protocol state computation
  tx.ts                 # Transaction scanning and classification
  scan-unified.ts       # Unified scan pipeline
  constants.ts          # Protocol constants and hardfork heights
  logger.ts             # Health check and logging
  api-types.ts          # TypeScript types for API responses
  db/                   # Prisma client and database queries
  storage/              # Data store abstraction (postgres/redis)
  scripts/              # Utility scripts (backup, migration, export)
tests/
  chain-verify.test.ts  # Chain verification against daemon snapshots
  *.test.ts             # Unit and integration tests (39 total)
  setup/                # Test DB helpers and env preload
scripts/                # Shell scripts for daemon/snapshot management
schema/                 # JSON schema generation
docs/                   # API docs and chain verification guide
prisma/                 # Prisma schema and migrations
py/                     # Python prototype utilities
```

## Python Utilities

The `py/` directory contains the original Python prototype scripts that predate the Node.js scanner. They talk directly to a local daemon, produce CSV files, and generate graphs — useful for quick ad-hoc analysis. See [py/README.md](py/README.md) for details.
