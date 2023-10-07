import { getCurrentBlockHeight, getBlock } from "./utils";
import redis from "./redis";

const DEATOMIZE = 10 ** -12;

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

async function setRedisHeight(height: number) {
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
    const pricingRecord = await getPricingRecordFromBlock(height);
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
      };

      const pr_to_save_json = JSON.stringify(pr_to_save);
      await redis.hset("pricing_records", height, pr_to_save_json);
      await setRedisHeight(height);
      continue;
    }
    console.log(`SCANNING BLOCK: ${height}/${rpcHeight - 1}`);

    // const pricingRecordJson = JSON.stringify(pricingRecord);

    const timestamp = pricingRecord.timestamp;

    const spot = pricingRecord.spot * DEATOMIZE;
    const moving_average = pricingRecord.moving_average * DEATOMIZE;
    const reserve = pricingRecord.reserve * DEATOMIZE;
    const reserve_ma = pricingRecord.reserve_ma * DEATOMIZE;
    const stable = pricingRecord.stable * DEATOMIZE;
    const stable_ma = pricingRecord.stable_ma * DEATOMIZE;

    let pr_to_save = {
      height: height,
      timestamp: timestamp,
      spot: spot,
      moving_average: moving_average,
      reserve: reserve,
      reserve_ma: reserve_ma,
      stable: stable,
      stable_ma: stable_ma,
    };

    const pr_to_save_json = JSON.stringify(pr_to_save);
    await redis.hset("pricing_records", height, pr_to_save_json);
    await setRedisHeight(height);

    console.log(`Saved pricing record for height ${height}`);
  }
  return;
}

// (async () => {
//   await scanPricingRecords();
//   return;
// })();
