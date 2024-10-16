// This file handles rolling the scanner back and removing all data from after the supplied height.
// This is not only for debugging purposes, but also for when the chain reorgs.
import redis from "./redis";
import { scanTransactions } from "./tx";
import { getBlock, getCurrentBlockHeight, getProtocolStatsFromRedis, getRedisBlockRewardInfo, getRedisHeight, getRedisPricingRecord } from "./utils";
const VERSION_2_HF_V6_BLOCK_HEIGHT = 360000;

export async function rollbackScanner(rollBackHeight: number) {
  const daemon_height = await getCurrentBlockHeight();
  const rollback_block_info = await getBlock(rollBackHeight);
  if (!rollback_block_info) {
    console.log(`Block at height ${rollBackHeight} not found`);
    return;
  }

  const rollback_timestamp = rollback_block_info.result.block_header.timestamp;

  console.log(`Rolling back scanner to height ${rollBackHeight}`);
  console.log(`\t Daemon height is ${daemon_height} - We are removing ${daemon_height - rollBackHeight} blocks`);

  // ---------------------------------------------------------------------------
  // -------------------- Remove data from "protocol_stats" --------------------
  // ---------------------------------------------------------------------------


  console.log(`\t Removing data from protocol_stats & protocol_stats_hourly & protocol_stats_daily...`);
  for (let height_to_process = rollBackHeight + 1; height_to_process <= daemon_height; height_to_process++) {
    await redis.hdel("protocol_stats", height_to_process.toString());
  }

  // Set "height_aggregator" to the rollBackHeight
  await redis.set("height_aggregator", rollBackHeight.toString());

  // Remove data from "protocol_stats_hourly"
  const hourlyResults = await redis.zrangebyscore("protocol_stats_hourly", rollback_timestamp.toString(), "+inf");
  console.log(`\t Removing ${hourlyResults.length} entries from protocol_stats_hourly...`);
  for (const score of hourlyResults) {
    await redis.zrem("protocol_stats_hourly", score);
  }

  // Set "timestamp_aggregator_hourly" to the rollBackTimestamp
  await redis.set("timestamp_aggregator_hourly", rollback_timestamp.toString());

  // Remove data from "protocol_stats_daily"
  const dailyResults = await redis.zrangebyscore("protocol_stats_daily", rollback_timestamp.toString(), "+inf");
  console.log(`\t Removing ${dailyResults.length} entries from protocol_stats_daily...`);
  for (const score of dailyResults) {
    await redis.zrem("protocol_stats_daily", score);
  }

  // Set "timestamp_aggregator_daily" to the rollBackTimestamp
  await redis.set("timestamp_aggregator_daily", rollback_timestamp.toString());

  // ---------------------------------------------------------
  // -------------------- Pricing Records --------------------
  // ---------------------------------------------------------

  console.log(`\t Removing data from pricing_records...`);
  for (let height_to_process = rollBackHeight + 1; height_to_process <= daemon_height; height_to_process++) {
    await redis.hdel("pricing_records", height_to_process.toString());
  }
  // Set "height_prs" to the rollBackHeight
  await redis.set("height_prs", rollBackHeight.toString());

  // ------------------------------------------------------
  // -------------------- Transactions --------------------
  // ------------------------------------------------------

  console.log(`\t Removing data from transactions...`);

  for (let height_to_process = rollBackHeight + 1; height_to_process <= daemon_height; height_to_process++) {
    // Remove block rewards
    await redis.hdel("block_rewards", height_to_process.toString());

    // Retrieve txs_by_block for the block being rolled back
    const txsByBlockHashes = await redis.hget("txs_by_block", height_to_process.toString());
    if (txsByBlockHashes) {
      const txs = JSON.parse(txsByBlockHashes);
      // Remove each transaction from "txs" key
      for (const tx_id of txs) {
        await redis.hdel("txs", tx_id);
      }
    }

    // Remove the block from "txs_by_block"
    await redis.hdel("txs_by_block", height_to_process.toString());
  }

  // Set "height_txs" to the rollBackHeight
  await redis.set("height_txs", rollBackHeight.toString());


  // ------------------------------------------------------
  // ----------------------- Totals -----------------------
  // ------------------------------------------------------
  console.log(`\t Recalculating totals by rescanning all transactions...`);
  await retallyTotals();


  // ------------------------------------------------------
  // -------------------- Block Hashes --------------------
  // ------------------------------------------------------s
  console.log(`\t Removing block hashes...`);
  for (let height_to_process = rollBackHeight + 1; height_to_process <= daemon_height; height_to_process++) {
    await redis.hdel("block_hashes", height_to_process.toString());
  }

  console.log(`Rollback to height ${rollBackHeight} completed successfully`);
}


export async function detectAndHandleReorg() {
  await setBlockHashesIfEmpty();

  // Start from the current scanner height
  let storedHeight = Number(await redis.get("height_aggregator"));
  let rollbackHeight = null;

  // Loop backwards through the stored block heights to compare hashes
  for (let height = storedHeight; height > 0; height--) {
    const storedBlockHash = await redis.hget("block_hashes", height.toString());
    const daemonBlockInfo = await getBlock(height);
    if (!daemonBlockInfo) {
      console.log(`Error detectAndHandleReorg - Block at height ${height} not found. Continuing...`);
      continue;
    }
    const daemonBlockHash = daemonBlockInfo.result.block_header.hash;

    // If the hashes match, we found the point of consistency
    if (storedBlockHash === daemonBlockHash) {
      console.log(`Matching block hash found at height ${height}. No rollback needed before this point.`);
      rollbackHeight = height;
      break;
    }
  }

  // If no rollback height is found, we're already at the correct point
  if (rollbackHeight === null) {
    console.log("No matching block hash found, scanner is up to date.");
    return;
  }

  // Rollback to the height after the matching hash (if necessary)
  if (rollbackHeight < storedHeight) {
    console.log(`Rollback required to height ${rollbackHeight}. Initiating rollback...`);
    await rollbackScanner(rollbackHeight);
  } else {
    console.log("No rollback required, the scanner is at the correct state.");
  }
}

async function setBlockHashesIfEmpty() {
  // check if block_hashes exists
  const blockHashesExists = await redis.exists("block_hashes");
  if (blockHashesExists) {
    console.log("block_hashes already exists, skipping...");
    return;
  }

  console.log("block_hashes does not exist, setting block hashes...");
  const starting_height = 89300;
  const ending_height = Number(await redis.get("height_prs"));
  if (!ending_height) {
    console.log("height_prs not found, skipping...");
    return;
  }

  for (let height = starting_height; height <= ending_height; height++) {
    console.log(`Setting block hash for height ${height} / ${ending_height} (${(height / ending_height) * 100})%`);
    const block = await getBlock(height);
    if (!block) {
      console.log(`Block at height ${height} not found, skipping...`);
      continue;
    }

    const block_hash = block.result.block_header.hash;
    await redis.hset("block_hashes", height.toString(), block_hash);
  }

  console.log("block_hashes set successfully.");
}

export async function retallyTotals() {
  console.log(`Recalculating totals...`);
  // we can do this from loking at the day aggregation records
  // get all the day aggregation records
  const relevantFields = [
    "conversion_transactions_count",
    "yield_conversion_transactions_count",
    "mint_reserve_count",
    "mint_reserve_volume",
    "fees_zephrsv",
    "redeem_reserve_count",
    "redeem_reserve_volume",
    "fees_zephusd",
    "mint_stable_count",
    "mint_stable_volume",
    "redeem_stable_count",
    "redeem_stable_volume",
    "fees_zeph",
    "mint_yield_count",
    "mint_yield_volume",
    "fees_zyield",
    "redeem_yield_count",
    "redeem_yield_volume",
    "fees_zephusd_yield",
  ]
  const aggregatedData = await getProtocolStatsFromRedis("hour", undefined, undefined, relevantFields)

  // calculate the totals
  const totals = {
    conversion_transactions: 0,
    yield_conversion_transactions: 0,
    mint_reserve_count: 0,
    mint_reserve_volume: 0,
    fees_zephrsv: 0,
    redeem_reserve_count: 0,
    redeem_reserve_volume: 0,
    fees_zephusd: 0,
    mint_stable_count: 0,
    mint_stable_volume: 0,
    redeem_stable_count: 0,
    redeem_stable_volume: 0,
    fees_zeph: 0,
    mint_yield_count: 0,
    mint_yield_volume: 0,
    fees_zyield: 0,
    redeem_yield_count: 0,
    redeem_yield_volume: 0,
    fees_zephusd_yield: 0,
    miner_reward: 0,
    governance_reward: 0,
    reserve_reward: 0,
    yield_reward: 0,
  }

  for (const record of aggregatedData) {
    totals.conversion_transactions += record.data.conversion_transactions_count;
    totals.yield_conversion_transactions += record.data.yield_conversion_transactions_count;
    totals.mint_reserve_count += record.data.mint_reserve_count;
    totals.mint_reserve_volume += record.data.mint_reserve_volume;
    totals.fees_zephrsv += record.data.fees_zephrsv;
    totals.redeem_reserve_count += record.data.redeem_reserve_count;
    totals.redeem_reserve_volume += record.data.redeem_reserve_volume;
    totals.fees_zephusd += record.data.fees_zephusd;
    totals.mint_stable_count += record.data.mint_stable_count;
    totals.mint_stable_volume += record.data.mint_stable_volume;
    totals.redeem_stable_count += record.data.redeem_stable_count;
    totals.redeem_stable_volume += record.data.redeem_stable_volume;
    totals.fees_zeph += record.data.fees_zeph;
    totals.mint_yield_count += record.data.mint_yield_count;
    totals.mint_yield_volume += record.data.mint_yield_volume;
    totals.fees_zyield += record.data.fees_zyield;
    totals.redeem_yield_count += record.data.redeem_yield_count;
    totals.redeem_yield_volume += record.data.redeem_yield_volume;
    totals.fees_zephusd_yield += record.data.fees_zephusd_yield;
  }


  const lastEntry = aggregatedData[aggregatedData.length - 1];

  // Check if the last entry contains 'timestamp'
  const lastTimestamp = 'timestamp' in lastEntry ? lastEntry.timestamp : undefined;

  // now we need to figure out the the block hieght we need to process from, from the last availble timestamp.
  // it's only going to be max 720 blocks back

  // get the current block height from aggreagtor
  const currentBlockHeight = await getRedisHeight()

  const protocolStats = await getProtocolStatsFromRedis("block", (currentBlockHeight - 720).toString(), undefined, relevantFields);

  for (const record of protocolStats) {
    if (record.data.timestamp < lastTimestamp) {
      continue;
    }
    totals.conversion_transactions += record.data.conversion_transactions_count;
    totals.yield_conversion_transactions += record.data.yield_conversion_transactions_count;
    totals.mint_reserve_count += record.data.mint_reserve_count;
    totals.mint_reserve_volume += record.data.mint_reserve_volume;
    totals.fees_zephrsv += record.data.fees_zephrsv;
    totals.redeem_reserve_count += record.data.redeem_reserve_count;
    totals.redeem_reserve_volume += record.data.redeem_reserve_volume;
    totals.fees_zephusd += record.data.fees_zephusd;
    totals.mint_stable_count += record.data.mint_stable_count;
    totals.mint_stable_volume += record.data.mint_stable_volume;
    totals.redeem_stable_count += record.data.redeem_stable_count;
    totals.redeem_stable_volume += record.data.redeem_stable_volume;
    totals.fees_zeph += record.data.fees_zeph;
    totals.mint_yield_count += record.data.mint_yield_count;
    totals.mint_yield_volume += record.data.mint_yield_volume;
    totals.fees_zyield += record.data.fees_zyield;
    totals.redeem_yield_count += record.data.redeem_yield_count;
    totals.redeem_yield_volume += record.data.redeem_yield_volume;
    totals.fees_zephusd_yield += record.data.fees_zephusd_yield;

  }

  // now populate the totals with the rewards
  const starting_height = 89300;
  for (let height = starting_height; height <= currentBlockHeight; height++) {
    const bri = await getRedisBlockRewardInfo(height);
    totals.miner_reward += bri.miner_reward;
    totals.governance_reward += bri.governance_reward;
    totals.reserve_reward += bri.reserve_reward;
    totals.yield_reward += bri.yield_reward;
  }

  // delete the totals key
  await redis.del("totals");

  // set all the totals to redis
  for (const [key, value] of Object.entries(totals)) {
    // check if value is NaN
    if (isNaN(value)) {
      console.log(`Value for ${key} is NaN, skipping...`);
      continue
    }
    await redis.hincrbyfloat("totals", key, value);
  }

  console.log(`Totals recalculated successfully.`);
  console.log(totals);

}






