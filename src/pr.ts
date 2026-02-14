import { getCurrentBlockHeight, getBlock } from "./utils";
import redis from "./redis";
import { stores } from "./storage/factory";
import { usePostgres, getStartBlock, getEndBlock } from "./config";
import { appendZysPriceHistory, fetchZysPriceHistory as fetchZysPriceHistorySql } from "./db/yieldAnalytics";

const DEATOMIZE = 10 ** -12;
const VERSION_2_HF_V6_BLOCK_HEIGHT = 360000;

export interface ZysPriceHistoryEntry {
  timestamp: number;
  block_height: number;
  zys_price: number;
}

async function getPricingRecordFromBlock(height: number) {
  const blockData = await getBlock(height);
  if (!blockData) {
    return;
  }

  const pricingRecord = blockData.result.block_header.pricing_record;
  return pricingRecord;
}

async function getStoredHeight() {
  return stores.pricing.getLatestHeight();
}

export async function scanPricingRecords() {
  const hfHeight = 89300;
  const rpcHeight = await getCurrentBlockHeight();
  // const rpcHeight = 89303; // TEMP OVERRIDE FOR TESTING
  const redisHeight = await getStoredHeight();

  const configStartBlock = getStartBlock();
  const configEndBlock = getEndBlock();
  const effectiveHfHeight = configStartBlock > 0 ? configStartBlock : hfHeight;
  const effectiveEndHeight = configEndBlock > 0 ? Math.min(configEndBlock, rpcHeight - 1) : rpcHeight - 1;

  const startingHeight = Math.max(redisHeight + 1, effectiveHfHeight);

  console.log("Fired pricing record scanner...");
  console.log(`Starting height: ${startingHeight} | Ending height: ${effectiveEndHeight}${configEndBlock > 0 ? ` (capped by END_BLOCK)` : ''}`);

  // Compute interval for approx 1% of blocks (at least 1)
  const totalBlocks = effectiveEndHeight - startingHeight;
  const logInterval = Math.max(1, Math.floor(totalBlocks / 100));

  for (let height = startingHeight; height <= effectiveEndHeight; height++) {
    const block = await getBlock(height);
    if (!block) {
      console.log(`${height}/${effectiveEndHeight} - No block info found, exiting try later`);
      return;
    }
    await redis.hset("block_hashes", height, block.result.block_header.hash);
    const pricingRecord = block.result.block_header.pricing_record;
    if (!pricingRecord) {
      if (height === startingHeight || height === effectiveEndHeight || (height - startingHeight) % logInterval === 0) {
        const percentComplete = ((height - startingHeight) / totalBlocks) * 100;
        console.log(`PRs SCANNING BLOCK(s): ${height}/${effectiveEndHeight}  | ${percentComplete.toFixed(2)}%`);
      }
      await stores.pricing.save({
        blockHeight: height,
        timestamp: 0,
        spot: 0,
        movingAverage: 0,
        reserve: 0,
        reserveMa: 0,
        stable: 0,
        stableMa: 0,
        yieldPrice: 0,
      });
      continue;
    }

    if (height === startingHeight || height === effectiveEndHeight || (height - startingHeight) % logInterval === 0) {
      const percentComplete = ((height - startingHeight) / totalBlocks) * 100;
      console.log(`PRs SCANNING BLOCK: ${height}/${effectiveEndHeight}  | ${percentComplete.toFixed(2)}%`);
    }

    const timestamp = pricingRecord.timestamp;
    const spot = pricingRecord.spot * DEATOMIZE;
    const moving_average = pricingRecord.moving_average * DEATOMIZE;
    const reserve = pricingRecord.reserve * DEATOMIZE;
    const reserve_ma = pricingRecord.reserve_ma * DEATOMIZE;
    const stable = pricingRecord.stable * DEATOMIZE;
    const stable_ma = pricingRecord.stable_ma * DEATOMIZE;
    const yield_price = pricingRecord.yield_price ? pricingRecord.yield_price * DEATOMIZE : 0;

    let pr_to_save = {
      height: height,
      timestamp: timestamp,
      spot: spot,
      moving_average: moving_average,
      reserve: reserve,
      reserve_ma: reserve_ma,
      stable: stable,
      stable_ma: stable_ma,
      yield_price: yield_price,
    };

    await stores.pricing.save({
      blockHeight: pr_to_save.height,
      timestamp: pr_to_save.timestamp,
      spot: pr_to_save.spot,
      movingAverage: pr_to_save.moving_average,
      reserve: pr_to_save.reserve,
      reserveMa: pr_to_save.reserve_ma,
      stable: pr_to_save.stable,
      stableMa: pr_to_save.stable_ma,
      yieldPrice: pr_to_save.yield_price,
    });

    // (no longer log every block)
    // console.log(`Saved pricing record for height ${height}`);
  }
  return;
}

export async function processZYSPriceHistory() {
  console.log("Processing ZYS price history...");
  const BLOCK_INTERVAL = 30;
  const height_prs = await getStoredHeight();
  const most_recent_zys_price_block = await getMostRecentBlockHeightFromRedis();
  const start = Math.max(most_recent_zys_price_block + BLOCK_INTERVAL, VERSION_2_HF_V6_BLOCK_HEIGHT);
  const end = height_prs;

  console.log(`Most recent ZYS price block: ${most_recent_zys_price_block}`);
  console.log(`Starting ZYS price history processing from block ${start} to ${end}`);

  for (let height = start; height <= end; height += BLOCK_INTERVAL) {
    const pricing_record = await getPricingRecordFromBlock(height);

    if (!pricing_record || !pricing_record.yield_price) {
      continue;
    }

    const zys_price = pricing_record.yield_price;
    const timestamp = pricing_record.timestamp;

    // Store the ZYS price and block height as a JSON object in the sorted set
    const data = JSON.stringify({
      block_height: height,
      zys_price: zys_price,
    });

    console.log(`\t Block ${height} - ZYS Price: ${zys_price} - Timestamp: ${timestamp}`);

    // Store in a sorted set with the timestamp as the score and the data as the value
    await redis.zadd("zys_price_history", timestamp, data);
    if (usePostgres()) {
      await appendZysPriceHistory([
        {
          block_height: height,
          zys_price,
          timestamp,
        },
      ]);
    }
  }
}

export async function getZYSPriceHistoryFromRedis(): Promise<ZysPriceHistoryEntry[]> {
  if (usePostgres()) {
    const rows = await fetchZysPriceHistorySql();
    if (rows.length > 0) {
      return rows;
    }
  }
  // Retrieve all records from Redis along with their scores (timestamps)
  const result = await redis.zrangebyscore("zys_price_history", "-inf", "+inf", "WITHSCORES");

  const history: ZysPriceHistoryEntry[] = [];

  // Loop through the result and pair each entry with its corresponding score
  for (let i = 0; i < result.length; i += 2) {
    const entry = JSON.parse(result[i]) as { block_height: number; zys_price: number }; // Parse the JSON entry
    const timestamp = Number(result[i + 1]); // Get the corresponding timestamp (score)

    history.push({
      timestamp,
      block_height: entry.block_height,
      zys_price: entry.zys_price,
    });
  }

  return history;
}

// Function to retrieve the most recent block height from Redis
export async function getMostRecentBlockHeightFromRedis() {
  // Fetch the last (highest) scored element from the sorted set (which is the latest timestamp)
  const most_recent = await redis.zrevrange("zys_price_history", 0, 0);

  if (!most_recent || most_recent.length === 0) {
    // If there's no history, return 0 (i.e., no previous block recorded)
    return 0;
  }

  // Parse the JSON and return the block height
  const most_recent_data = JSON.parse(most_recent[0]);
  return most_recent_data.block_height;
}

// (async () => {
//   await scanPricingRecords();
//   return;
// })();
