# zephyrdscanner

Tool for scanning the Zephyr Blockchain.
Powehouse for website data and all 

IMPORTANT NOTE - THIS IS A WIP Project

## Node

`npm run start`

Uses redis to store information.
Polls every minute to check for new blocks/transactions.

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

```sh
# write each snapshot to reserve_snapshots/HEIGHT.json (skips existing files)
npx tsx scripts/exportReserveSnapshots.ts --dir reserve_snapshots

# overwrite existing files and index by previous height instead of reserve height
npx tsx scripts/exportReserveSnapshots.ts --dir reserve_snapshots --force --index previous

# import JSON snapshots from disk into Redis
npx tsx scripts/importReserveSnapshots.ts --dir reserve_snapshots

# force overwriting existing Redis entries
npx tsx scripts/importReserveSnapshots.ts --dir reserve_snapshots --force

# rapidly capture live snapshots into Redis (standalone script)
npx tsx scripts/redisReserveSnapshotter.ts

# adjust poll rate (milliseconds) if the daemon can handle faster calls
RESERVE_SNAPSHOT_POLL_INTERVAL_MS=100 npx tsx scripts/redisReserveSnapshotter.ts

Exporting and importing full Redis data:

```sh
# dump all Redis keys and values into exports/<version>/redis_export_<version>_<height>_<timestamp>/
npx tsx scripts/exportRedisData.ts --pretty

# restore the dump directory (optionally flush first)
npx tsx scripts/importRedisData.ts --dir exports/1.0.0/redis_export_1.0.0_613410_2025-10-01T05-13-49-436Z --flush

# skip keys that already exist during import
npx tsx scripts/importRedisData.ts --dir exports/1.0.0/redis_export_1.0.0_613410_2025-10-01T05-13-49-436Z --skip-existing
```
```

Configuration knobs:

- `RESERVE_SNAPSHOT_INTERVAL_BLOCKS` (default `720`) – minimum block gap before persisting a new daemon snapshot.
- `RESERVE_SNAPSHOT_START_HEIGHT` (default `89300`) – first height from which auto-snapshots begin.
- `RESERVE_SNAPSHOT_SOURCE` (default `redis`) – primary snapshot store (`redis` or `file`).
- `WALKTHROUGH_SNAPSHOT_SOURCE` (default inherits) – snapshot source to use during walkthroughs.
- `RESERVE_DIFF_TOLERANCE` (default `1`) – maximum absolute reserve difference allowed before we record a mismatch (same units as the diff calculation).

### Walkthrough mode

Run `npm run walkthrough` to flush Redis, force `ONLINE=true`, enable walkthrough logging (`WALKTHROUGH_MODE=true`, default diff threshold `1`), and boot the scanner. Pair with a daemon started using `--block-sync-size=1` to find the first block where cached stats diverge from on-chain reserve info. During a walkthrough run the scanner mirrors the latest reserve snapshots saved in Redis (or falls back to the historical JSON files) so you can reconcile cached protocol stats against daemon truth block by block. You can point `WALKTHROUGH_SNAPSHOT_SOURCE` at `file` to replay the historical JSON dumps, or stay on the default `redis` source to inspect the live snapshots written during synced operation. Divergences detected by the walkthrough are emitted to the console and the same reports are persisted in `walkthrough_reports/` for later review.

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

### Route: `/`
**Description**: Root endpoint for testing server connection.

**Response**: Returns a simple confirmation message "zephyrdscanner reached".

```sh
curl "http://157.245.130.75:4000/"
```

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
  "zsd_in_yield_reserve_percent": 0.7153558816412525
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
