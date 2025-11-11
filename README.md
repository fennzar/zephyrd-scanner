# zephyrdscanner

Tool for scanning the Zephyr Blockchain.
Powehouse for website data and all

IMPORTANT NOTE - THIS IS A WIP Project

## Node

`npm run start`

Uses redis to store information.
Polls every minute to check for new blocks/transactions.

### Redis configuration

The scanner reads Redis connection details from environment variables. Defaults are `localhost:6379`, database `0`.

- `REDIS_URL` – optional full connection string (overrides the fields below).
- `REDIS_HOST`, `REDIS_PORT`, `REDIS_PASSWORD`, `REDIS_DB` – individual connection properties.
- `REDIS_SOURCE_DB` – optional source DB index (used by the background preparation script, defaults to `0`).
- `AUTO_EXPORT_ENABLED`, `AUTO_EXPORT_INTERVAL`, `AUTO_EXPORT_DIR`, `AUTO_EXPORT_PRETTY` – control automatic milestone exports (see below).

### PostgreSQL configuration

All persistent scanner data can also live in PostgreSQL via Prisma. Set the following to enable it:

- `DATABASE_URL` – native Postgres connection string (example: `postgresql://zephyrdscanner:zephyrdscanner@localhost:5432/zephyrdscanner?schema=public`).
- `DATA_STORE` – `redis` (default), `postgres`, or `hybrid`. The scanner writes to the selected store(s) and the API reads from the same source.

#### Store selection

- `DATA_STORE=redis` – legacy behaviour; everything stays in Redis.
- `DATA_STORE=postgres` – scanner + API read/write Postgres only.
- `DATA_STORE=hybrid` – scanner dual-writes to Redis and Postgres so you can validate both before cutting over. The public HTTP API automatically targets whatever store the scanner instance declares via `DATA_STORE`.

You can also run two scanner processes (one per store) instead of hybrid mode. For example, keep production on Redis while running a background scanner with `DATA_STORE=postgres` so each API tracks its own backing store.

#### Tooling & commands

| Command | Description |
| --- | --- |
| `npm run prisma:migrate:deploy` | Apply Prisma migrations after configuring `DATABASE_URL`. |
| `npm run prisma:generate` | Regenerate the Prisma client if the schema changes. |
| `npx prisma studio` | Launch the Prisma web UI to inspect/edit tables. |
| `npm run db:reset` | Drop and recreate the Postgres database referenced by `DATABASE_URL` (useful before re-importing). |
| `npm run db:migrate-from-redis` | Bulk-import the current Redis dataset (10 000-row batches with progress logs). |
| `npm run compare-stores` | Randomly sample docs from both stores and emit a parity report (DATA_STORE is forced to `hybrid`). |
| `npm run db:export-sql` | Dump each Prisma model to JSON under `exports/sql/...` for lightweight snapshots. |
| `npm run db:backup` | Run `pg_dump` against `DATABASE_URL` and write `backups/postgres_backup_<timestamp>.sql`. |
| `DATA_STORE=postgres npm run scanner` | Run the scanner/server against Postgres only (set `DATA_STORE=hybrid` to dual-write during validation). |

#### Migrating from Redis

1. Ensure `DATABASE_URL` points at your Postgres instance (export it in the shell or place it in `.env`).
2. Start from a clean slate with `npm run db:reset`.
3. Apply the schema via `npx prisma migrate deploy`.
4. Run `npm run db:migrate-from-redis`. The script reads every Redis structure, writes Postgres in 10 k chunks, and logs `[migrate]` progress so you can see long-running sections.
5. Validate parity with `npm run compare-stores`. Fix any red lines, then re-run until the report is clean.
6. Switch the live scanner/API to Postgres by setting `DATA_STORE=postgres` (or keep Redis online and run a second Postgres-backed instance for shadow traffic).

The migrator is idempotent for existing rows (upserts) and can be rerun after partial imports. Reserve snapshot migration is included, so historical reserve data survives the move.

#### Exports, backups, and restores

- `npm run db:export-sql` produces structured JSON per table under `exports/sql/`. Useful for sharing subsets of data or re-importing with application scripts.
- `npm run db:backup` captures a raw SQL dump via `pg_dump` in `backups/` (the script strips Prisma’s `?schema=` suffix automatically). Restore with `psql -d <target_db> -f backups/postgres_backup_<timestamp>.sql`.

#### Resetting Postgres fast

`npm run db:reset` uses `dropdb`/`createdb` behind the scenes so you can wipe the database, rerun migrations, and import fresh data without manually shelling into Postgres.

You can run two scanner instances side by side by pointing each one at a different Redis DB index:

```sh
# production (HTTP + scanner on DB 0)
npm run start

# background rescan (scanner only, writes to DB 1)
REDIS_DB=1 npm run bg-scan
```

WIP: We currently make too many calls to the redis server which results in the connections dropping when there are 2 instances running.
The `bg-scan` script sets `ENABLE_SERVER=false`, so the background instance skips binding the HTTP port while it repopulates Redis.

#### Preparing the staging database

Before running a background scan you can seed the target DB using the `prepare-bg` helper:

```sh
# Copy DB 0 into DB 1, then clear aggregation keys (soft mode – default)
npm run prepare-bg -- --target-db=1 --mode=soft

# Flush DB 1 so the scanner starts from a blank slate (hard mode)
npm run prepare-bg -- --target-db=1 --mode=hard
```

Soft mode keeps pricing records, transactions, etc. intact while deleting `protocol_stats` and other derived aggregates so the scanner can rebuild them. Hard mode simply flushes the target DB. Omit `--source-db` to default to `REDIS_SOURCE_DB` (0) and `--target-db` to default to `REDIS_DB` (1).

#### Automatic milestone exports

During a scan the runner writes a Redis snapshot to disk whenever the aggregated block height crosses each `AUTO_EXPORT_INTERVAL` (default: 100 000 blocks). Configure the behaviour with:

- `AUTO_EXPORT_ENABLED` – set to `false` to disable automated exports.
- `AUTO_EXPORT_INTERVAL` – block interval between exports.
- `AUTO_EXPORT_DIR` – optional export root (passed to `--dir`).
- `AUTO_EXPORT_PRETTY` – set to `true` to pretty-print JSON output.

After the background run finishes you can atomically swap the databases and clear the old one:

```sh
redis-cli SWAPDB 0 1   # promote DB 1 to live
redis-cli SELECT 1
redis-cli FLUSHDB      # clear the staging data if desired
```

### Reserve snapshots

The scanner stores daemon `get_reserve_info` snapshots in Redis whenever the aggregated height matches the daemon height and the previous snapshot is at least `RESERVE_SNAPSHOT_INTERVAL_BLOCKS` (default 720) behind. Snapshots start at `RESERVE_SNAPSHOT_START_HEIGHT` (default 89300). This runs in normal mode and during walkthroughs using the following keys:

- `reserve_snapshots` (Redis hash) – JSON snapshot documents keyed by the previous block height.
- `reserve_snapshots:last_previous_height` (Redis string) – latest previous height for which a snapshot exists.
- `reserve_mismatch_heights` (Redis hash) – stores the full diff report whenever on-chain reserve data diverges from cached stats.

Useful commands:

```sh
# list the stored snapshot heights
redis-cli HKEYS reserve_snapshots

# inspect a specific snapshot
redis-cli HGET reserve_snapshots 89302 | jq

# check the most recent snapshot height
redis-cli GET reserve_snapshots:last_previous_height

# list heights where a reserve mismatch was recorded
redis-cli HKEYS reserve_mismatch_heights

# inspect the mismatch report for a height
redis-cli HGET reserve_mismatch_heights 89310 | jq

# fetch current reserve info directly from the daemon
curl "http://127.0.0.1:17767/json_rpc" \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":"0","method":"get_reserve_info"}' | jq
```

Exporting the stored snapshots to JSON files:

````sh
# write each snapshot to reserve_snapshots/HEIGHT.json (skips existing files)
npx tsx src/scripts/exportReserveSnapshots.ts --dir reserve_snapshots

# overwrite existing files and index by previous height instead of reserve height
npx tsx src/scripts/exportReserveSnapshots.ts --dir reserve_snapshots --force --index previous

# import JSON snapshots from disk into Redis
npx tsx src/scripts/importReserveSnapshots.ts --dir reserve_snapshots

# force overwriting existing Redis entries
npx tsx src/scripts/importReserveSnapshots.ts --dir reserve_snapshots --force

# rapidly capture live snapshots into Redis (standalone script)
npx tsx src/scripts/redisReserveSnapshotter.ts

# adjust poll rate (milliseconds) if the daemon can handle faster calls
RESERVE_SNAPSHOT_POLL_INTERVAL_MS=100 npx tsx src/scripts/redisReserveSnapshotter.ts

Exporting and importing full Redis data:

```sh
# dump all Redis keys and values into exports/<version>/redis_export_<version>_<height>_<timestamp>/
npx tsx src/scripts/exportRedisData.ts --pretty

# restore the dump directory (optionally flush first)
npx tsx src/scripts/importRedisData.ts --dir exports/1.0.0/redis_export_1.0.0_613410_2025-10-01T05-13-49-436Z --flush

# skip keys that already exist during import
npx tsx src/scripts/importRedisData.ts --dir exports/1.0.0/redis_export_1.0.0_613410_2025-10-01T05-13-49-436Z --skip-existing
````

For Postgres backups, run `npm run db:export-sql` for JSON snapshots under `exports/sql/...` or `npm run db:backup` to capture a raw `pg_dump` file in `backups/`.

````

Configuration knobs:

- `RESERVE_SNAPSHOT_INTERVAL_BLOCKS` (default `720`) – minimum block gap before persisting a new daemon snapshot.
- `RESERVE_SNAPSHOT_START_HEIGHT` (default `89300`) – first height from which auto-snapshots begin.
- `RESERVE_SNAPSHOT_SOURCE` (default `redis`) – primary snapshot store (`redis` or `file`).
- `WALKTHROUGH_SNAPSHOT_SOURCE` (default inherits) – snapshot source to use during walkthroughs.
- `RESERVE_DIFF_TOLERANCE` (default `1`) – maximum absolute reserve difference allowed before we record a mismatch (same units as the diff calculation).

### Walkthrough mode

Run `npm run walkthrough` to flush Redis, force `ONLINE=true`, enable walkthrough logging (`WALKTHROUGH_MODE=true`, default diff threshold `1`), and boot the scanner. Pair with a daemon started using `--block-sync-size=1` to find the first block where cached stats diverge from on-chain reserve info. During a walkthrough run the scanner mirrors the latest reserve snapshots saved in Redis (or falls back to the historical JSON files) so you can reconcile cached protocol stats against daemon truth block by block. You can point `WALKTHROUGH_SNAPSHOT_SOURCE` at `file` to replay the historical JSON dumps, or stay on the default `redis` source to inspect the live snapshots written during synced operation. Divergences detected by the walkthrough are emitted to the console and the same reports are persisted in `walkthrough_reports/` for later review.

When `DATA_STORE=postgres`, the walkthrough reset now truncates the Postgres tables as well so both stores remain in sync before replaying the chain.

Walkthrough runs capture:

- Pricing Records (Asset prices over time recorded in each block)
- Conversion Transactions
- Block Rewards (Miner Reward, Reserve Rewards and Old Governance Reward)
- Totals
  - Block Rewards
  - Number of Conversion Transactions (and of each type)
  - Amount of Zeph/ZSD/ZRS/ZYS converted (volume)
- Fees generated from conversions

### Partial aggregation guardrails

The scanner always runs the aggregator after each block update so dashboards receive near-real-time data. When we are still catching up (daemon height more than 30 blocks ahead for hourly or 720 blocks ahead for daily), stop-gap hourly or daily aggregates are skipped. As soon as the lag falls within those limits the temporary aggregates resume, so by the time the daemon is fully synced you have a single authoritative daily/hourly record.

## Available Routes

An instance of `zephyrdscanner` is running on `157.245.130.75:4000`. There is a rate limit, so please avoid excessive requests.

## Public Routes

### API Schemas
Run `npm run generate-api-docs` to regenerate the JSON Schemas and write a human-readable `docs/api.md` snapshot of every endpoint response. The underlying schema files live in `schema/` (for example, `schema/stats-block.schema.json`, `schema/transactions.schema.json`, `schema/live-stats.schema.json`) and are driven by the TypeScript exports in `src/api-types.ts`. Update `schema/schema.config.json` whenever you expose additional response types. Use `npm run generate-schemas` if you only need to refresh the raw JSON schema outputs.

### Route: `/`
**Description**: Root endpoint for testing server connection.

**Response**: Returns a simple confirmation message "zephyrdscanner reached".

```sh
curl "http://157.245.130.75:4000/"
````

### Route: `/stats`

**Description**: Retrieves statistics on the Zephyr Protocol over different time scales.

**Params**:

- **scale** (required): The granularity of data. Can be `block`, `hour`, or `day`.
- **from** (optional): Starting block number or timestamp (applicable for `hour` and `day` scales).
- **to** (optional): Ending block number or timestamp (applicable for `hour` and `day` scales).
- **fields** (optional): A comma-separated string of fields to request (e.g., `spot_open,spot_close`).

**Example**:

```sh
curl "157.245.130.75:4000/stats?scale=day&from=1729468800&to=1729598400&fields=spot_open,spot_close,moving_average_open,moving_average_close" | jq
```

Notes:

- When requesting data with block scale, the response structure and available fields differs compared to hour and day scales.
- There is a lot of data, avoid not passing in from and to params where possible.

```sh
curl "http://157.245.130.75:4000/stats?scale=block&from=500000&to=500100&fields=spot,moving_average"
```

### Route: `/historicalreturns`

Returns cached historical yield returns. Optional `?test=true` serves dummy data.

```sh
curl "http://157.245.130.75:4000/historicalreturns"
```

### Route: `/projectedreturns`

Returns projected yield returns. Optional `?test=true` serves dummy data.

```sh
curl "http://157.245.130.75:4000/projectedreturns"
```

### Route: `/blockrewards`

**Description**: Returns per-block reward breakdowns (miner, governance, reserve, yield) stored by the scanner.

**Params**:

- **from** / **to** (optional): Height range to include.
- **limit** (optional): Maximum rows to return (default `all` in range). Use `limit=all` to disable.
- **order** (optional): `asc` (default) or `desc`.

**Examples**:

```sh
# Latest 20 reward records
curl "http://157.245.130.75:4000/blockrewards?order=desc&limit=20" | jq
```

```sh
# Rewards around height 500000
curl "http://157.245.130.75:4000/blockrewards?from=499990&to=500010" | jq
```

### Route: `/pricingrecords`

**Description**: Returns cached pricing records (spot, reserve ratios, yield price) keyed by block height.

**Params**:

- **from** / **to** (optional): Height range to include.
- **limit** (optional): Maximum rows to return (default `all` in range). Use `limit=all` to disable.
- **order** (optional): `asc` (default) or `desc`.

**Examples**:

```sh
# First 5 pricing records after the v2 fork
curl "http://157.245.130.75:4000/pricingrecords?from=360000&limit=5" | jq
```

```sh
# Most recent pricing record
curl "http://157.245.130.75:4000/pricingrecords?order=desc&limit=1" | jq
```

### Route: `/reservesnapshots`

**Description**: Returns reserve snapshots captured by the scanner (indexed by `previous_height`).

**Params**:

- **height** (optional): Exact `previous_height` to fetch.
- **from** / **to** (optional): Range of `previous_height` values.
- **limit** (optional): Maximum rows to return (default `all` in range). Use `limit=all` to disable.
- **order** (optional): `asc` (default) or `desc`.

**Examples**:

```sh
# Specific snapshot by previous height
curl "http://157.245.130.75:4000/reservesnapshots?height=520000" | jq
```

```sh
# Latest 3 reserve snapshots
curl "http://157.245.130.75:4000/reservesnapshots?order=desc&limit=3" | jq
```

### Route: `/txs`

**Description**: Returns cached conversion transactions stored by the scanner. Supports pagination, timestamp windows, and filtering by conversion type.

**Params**:

- **from** (optional): Lower bound for `block_timestamp`. Accepts UNIX seconds or ISO-8601 date strings.
- **from_index** (optional): Skip the first N results when sorted from oldest to newest. Useful for polling incremental updates (e.g. `from_index=26963`). Also accepts the alias `from=idx:26963`.
- **to** (optional): Upper bound for `block_timestamp`. Accepts UNIX seconds or ISO-8601 date strings.
- **types** (optional): Comma separated list of conversion types to include (e.g. `mint_stable,redeem_stable,mint_reserve`).
- **limit** (optional): Maximum number of rows to return (default `1000`). Use `limit=all` or `limit=0` to retrieve the full result set.
- **offset** (optional): Number of rows to skip before returning results (default `0`). Useful for sequential pagination (e.g. page two = `offset=20&limit=20`).
- **page** and **pageSize** (optional): Alternative pagination controls. `page` is 1-based and uses `pageSize` (or the current `limit`) for sizing.
- **order** (optional): Sort direction by timestamp (`asc` or `desc`, default `desc`).

**Example**:

```sh
# Latest 25 stable mint/redemptions from the past day
curl "http://157.245.130.75:4000/txs?types=mint_stable,redeem_stable&from=$(date -d '1 day ago' +%s)&limit=25" | jq
```

```sh
# Oldest reserve conversions within a time window
curl "http://157.245.130.75:4000/txs?types=mint_reserve,redeem_reserve&from=1729468800&to=1729555200&order=asc&limit=10" | jq
```

```sh
# Grab every mint_yield event in a single response
curl "http://157.245.130.75:4000/txs?types=mint_yield&limit=all" | jq
```

```sh
# Page through the latest results (page 1 of 20-row slices)
curl "http://157.245.130.75:4000/txs?page=1&pageSize=20" | jq
```

```sh
# Grab all transactions in a single response (may be large)
curl "http://157.245.130.75:4000/txs" | jq
```

```sh
# Fetch only the transactions added after the initial 26 963 rows
curl "http://157.245.130.75:4000/txs?from_index=26963&limit=all" | jq
```

**Paginated response fields**:

- `total`: Total rows matching the filters.
- `limit`: Page size used for the current response (null when `limit=all`).
- `offset`: Starting index (relative to newest when `order=desc`).
- `next_offset` / `prev_offset`: Cursor-style hints for subsequent requests.
- `results`: Array of transaction objects (same shape as stored in Redis).

### Route: `/livestats`

**Description**: Retrieves (some) current live stats of Zephyr Protocol from Redis.

**Example**:

```sh
curl "157.245.130.75:4000/livestats" | jq
```

**Response**:

```sh
{
  "zeph_price": 3.8466,
  "zsd_rate": 0.26,
  "zsd_price": 1.0001,
  "zrs_rate": 1.5052,
  "zrs_price": 5.7899,
  "zys_price": 1.0352,
  "zeph_circ": 5815118.358150186,
  "zsd_circ": 761083.4546228138,
  "zrs_circ": 1092791.8177687959,
  "zys_circ": 525951.1647029163,
  "zeph_circ_daily_change": 8794.776697436348,
  "zsd_circ_daily_change": 57836.032324298634,
  "zrs_circ_daily_change": -6767.932300000219,
  "zys_circ_daily_change": 43992.33862813725,
  "zeph_in_reserve": 1842773.0392142392,
  "zeph_in_reserve_value": 7088410.772641493,
  "zeph_in_reserve_percent": 0.3168934707290177,
  "zsd_in_yield_reserve": 544445.5256842732,
  "zsd_in_yield_reserve_percent": 0.7153558816412525,
  "zys_current_variable_apy": 24.5123
}
```

### Route: `/zyspricehistory`

Returns the processed ZYS price history stored in Redis.

```sh
curl "http://157.245.130.75:4000/zyspricehistory"
```

### Route: `/apyhistory`

Returns the cached APY history.

```sh
curl "http://157.245.130.75:4000/apyhistory"
```

---

## Private Routes (localhost only)

### Route: `/rollback`

Roll the scanner back to a given `height` and rebuild forward.

```sh
curl "http://127.0.0.1:4000/rollback?height=500000"
```

### Route: `/retallytotals`

Recompute cumulative totals from existing aggregated data.

```sh
curl "http://127.0.0.1:4000/retallytotals"
```

### Route: `/redetermineapyhistory`

Force a full APY history rebuild.

```sh
curl "http://127.0.0.1:4000/redetermineapyhistory"
```

### Route: `/reservediff`

Compare daemon reserve info with cached stats (absolute differences).

```sh
curl "http://127.0.0.1:4000/reservediff"
```

### Route: `/reset`

**Method**: `POST`

**Description**: Rebuilds scanner state.

**Params**:

- **scope** (optional): `aggregation` (default) clears aggregation artefacts and recomputes derived data. Use `full` to flush the Redis database and rescan pricing records and transactions from scratch.

**Examples**:

```sh
# Rebuild only aggregate data
curl -X POST "http://127.0.0.1:4000/reset"

# Perform a full reset (flush Redis, rescan, reaggregate)
curl -X POST "http://127.0.0.1:4000/reset?scope=full"
```

---

## Python

Was used as a prototype for the node version, but has graphing functionality.
Uses CSV files as apposed to redis at the moment.

Run `prscan.py` first to generate the pricing records CSV file.
Run `graph.py` to generate graphs from the CSV files.

Run `txscan.py` to generate the transactions CSV file
Run `txstats.py` to generate stats from the transactions CSV file
