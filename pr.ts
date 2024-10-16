import { getCurrentBlockHeight, getBlock } from "./utils";
import redis from "./redis";
import { get } from "http";

const DEATOMIZE = 10 ** -12;
const VERSION_2_HF_V6_BLOCK_HEIGHT = 360000;

async function getPricingRecordFromBlock(height: number) {
  const blockData = await getBlock(height);
  if (!blockData) {
    return;
  }

  const pricingRecord = blockData.result.block_header.pricing_record;
  return pricingRecord;
}

async function getRedisHeight() {
  const height = await redis.get("height_prs");
  if (!height) {
    return 0;
  }
  return parseInt(height);
}

export async function setRedisHeightPRs(height: number) {
  await redis.set("height_prs", height);
}

export async function scanPricingRecords() {
  const hfHeight = 89300;
  const rpcHeight = await getCurrentBlockHeight();
  const redisHeight = await getRedisHeight();
  const startingHeight = Math.max(redisHeight + 1, hfHeight);

  console.log("Fired pricing record scanner...");
  console.log(`Starting height: ${startingHeight} | Ending height: ${rpcHeight - 1}`);

  for (let height = startingHeight; height <= rpcHeight - 1; height++) {
    // const pricingRecord = await getPricingRecordFromBlock(height);
    const block = await getBlock(height);
    if (!block) {
      console.log(`${height}/${rpcHeight - 1} - No block`);
      continue;
    }
    // Save the block hash to redis to reference later to determine if there has been a rollback 
    await redis.hset("block_hashes", height, block.result.block_header.hash);
    const pricingRecord = block.result.block_header.pricing_record;
    if (!pricingRecord) {
      console.log(`${height}/${rpcHeight - 1} - No pricing record`);
      let pr_to_save = {
        height: height,
        timestamp: 0,
        spot: 0,
        moving_average: 0,
        reserve: 0,
        reserve_ma: 0,
        stable: 0,
        stable_ma: 0,
        yield_price: 0,
      };

      const pr_to_save_json = JSON.stringify(pr_to_save);
      await redis.hset("pricing_records", height, pr_to_save_json);
      await setRedisHeightPRs(height);
      continue;
    }
    const percentComplete = ((height - startingHeight) / (rpcHeight - startingHeight)) * 100;
    console.log(`PRs SCANNING BLOCK: ${height}/${rpcHeight - 1}  \t | ${percentComplete.toFixed(2)}%`);

    // const pricingRecordJson = JSON.stringify(pricingRecord);

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

    const pr_to_save_json = JSON.stringify(pr_to_save);
    await redis.hset("pricing_records", height, pr_to_save_json);
    await setRedisHeightPRs(height);

    console.log(`Saved pricing record for height ${height}`);

  }
  return;
}


export async function processZYSPriceHistory() {
  console.log("Processing ZYS price history...");
  const BLOCK_INTERVAL = 30;
  const height_prs = await getRedisHeight();
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
      zys_price: zys_price
    });

    console.log(`\t Block ${height} - ZYS Price: ${zys_price} - Timestamp: ${timestamp}`);

    // Store in a sorted set with the timestamp as the score and the data as the value
    await redis.zadd("zys_price_history", timestamp, data);
  }
}

export async function getZYSPriceHistoryFromRedis() {
  // Retrieve all records from Redis along with their scores (timestamps)
  const result = await redis.zrangebyscore("zys_price_history", '-inf', '+inf', 'WITHSCORES');

  const history = [];

  // Loop through the result and pair each entry with its corresponding score
  for (let i = 0; i < result.length; i += 2) {
    const entry = JSON.parse(result[i]);  // Parse the JSON entry
    const timestamp = result[i + 1];      // Get the corresponding timestamp (score)

    history.push({
      timestamp: Number(timestamp),  // Add the timestamp
      ...entry,          // Spread the block height and zys_price
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
