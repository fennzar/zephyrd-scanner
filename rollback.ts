// This file handles rolling the scanner back and removing all data from after the supplied height.
// This is not only for debugging purposes, but also for when the chain reorgs.
import { aggregate } from "./aggregator";
import { scanPricingRecords } from "./pr";
import redis from "./redis";
import { scanTransactions } from "./tx";
import { AggregatedData, ProtocolStats, getAggregatedProtocolStatsFromRedis, getBlock, getBlockProtocolStatsFromRedis, getCurrentBlockHeight, getRedisBlockRewardInfo, getRedisHeight, getRedisPricingRecord } from "./utils";
import * as fs from 'fs';

// Function to append log information to a file
function writeLogToFile(logContent: string) {
  fs.appendFileSync('totals_log.txt', logContent + '\n', 'utf8');
}

const VERSION_2_HF_V6_BLOCK_HEIGHT = 360000;

export async function rollbackScanner(rollBackHeight: number) {
  const daemon_height = await getCurrentBlockHeight();
  const rollback_block_info = await getBlock(rollBackHeight);
  if (!rollback_block_info) {
    console.log(`Block at height ${rollBackHeight} not found`);
    return;
  }

  // ------------------------------------------------------
  // -------------------- Block Hashes --------------------
  // ------------------------------------------------------s
  console.log(`\t Removing block hashes that we are rolling back: (${rollBackHeight} - ${daemon_height})...`);
  for (let height_to_process = rollBackHeight + 1; height_to_process <= daemon_height; height_to_process++) {
    await redis.hdel("block_hashes", height_to_process.toString());
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

  console.log(`\t Rescanning Pricing Records...`);
  // Refire scanPricingRecords to repopulate the pricing records
  await scanPricingRecords();

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

  console.log(`\t Rescanning Transactions...`);

  // Refire scanTransactions to repopulate the transactions
  await scanTransactions();

  // ------------------------------------------------------
  // ------------------- Aggregator -----------------------
  // ------------------------------------------------------

  console.log(`\t Firing aggregator to repop protocol_stats...`);
  await aggregate();


  // ------------------------------------------------------
  // ----------------------- Totals -----------------------
  // ------------------------------------------------------
  console.log(`\t Recalculating totals by rescanning all transactions...`);
  await retallyTotals();


  console.log(`Rollback to height ${rollBackHeight} and reset completed successfully`);
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
      console.log(`Error detectAndHandleReorg - Block at height ${height} not found. Exiting...`);
      return;
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
      console.log(`Block at height ${height} not found, exiting`);
      return;
    }

    const block_hash = block.result.block_header.hash;
    await redis.hset("block_hashes", height.toString(), block_hash);
  }

  console.log("block_hashes set successfully.");
}

export async function retallyTotals() {

  // delete the previous log file
  try {
    fs.unlinkSync('totals_log.txt');
  } catch (err) {
    console.error(err);
  }

  console.log(`Recalculating totals...`);

  // Define relevant fields to request from aggregated data
  const relevantAggregatedFields: (keyof AggregatedData)[] = [
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
  ];

  // Get all the day aggregation records
  const aggregatedData = await getAggregatedProtocolStatsFromRedis("day", undefined, undefined, relevantAggregatedFields);

  // Calculate the totals
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
  };

  console.log(`
    _______________________________________________________
    Recalculating totals from day-level data... 
    _______________________________________________________
    `);
  writeLogToFile(`
    _______________________________________________________
    Recalculating totals from day-level data... 
    _______________________________________________________
    `);

  // Aggregate data from the retrieved records
  for (const record of aggregatedData) {
    // Convert timestamp to human-readable format (assuming record.timestamp is in seconds)
    const timestampHR = new Date(record.timestamp * 1000).toISOString().replace("T", " ").substring(0, 19);

    // Calculate changes
    const changeConversionTransactions = record.data.conversion_transactions_count ?? 0;
    const changeYieldConversionTransactions = record.data.yield_conversion_transactions_count ?? 0;
    const changeMintReserveCount = record.data.mint_reserve_count ?? 0;
    const changeMintReserveVolume = record.data.mint_reserve_volume ?? 0;
    const changeFeesZephrsv = record.data.fees_zephrsv ?? 0;
    const changeRedeemReserveCount = record.data.redeem_reserve_count ?? 0;
    const changeRedeemReserveVolume = record.data.redeem_reserve_volume ?? 0;
    const changeFeesZephusd = record.data.fees_zephusd ?? 0;
    const changeMintStableCount = record.data.mint_stable_count ?? 0;
    const changeMintStableVolume = record.data.mint_stable_volume ?? 0;
    const changeRedeemStableCount = record.data.redeem_stable_count ?? 0;
    const changeRedeemStableVolume = record.data.redeem_stable_volume ?? 0;
    const changeFeesZeph = record.data.fees_zeph ?? 0;
    const changeMintYieldCount = record.data.mint_yield_count ?? 0;
    const changeMintYieldVolume = record.data.mint_yield_volume ?? 0;
    const changeFeesZyield = record.data.fees_zyield ?? 0;
    const changeRedeemYieldCount = record.data.redeem_yield_count ?? 0;
    const changeRedeemYieldVolume = record.data.redeem_yield_volume ?? 0;
    const changeFeesZephusdYield = record.data.fees_zephusd_yield ?? 0;

    // Update totals
    totals.conversion_transactions += changeConversionTransactions;
    totals.yield_conversion_transactions += changeYieldConversionTransactions;
    totals.mint_reserve_count += changeMintReserveCount;
    totals.mint_reserve_volume += changeMintReserveVolume;
    totals.fees_zephrsv += changeFeesZephrsv;
    totals.redeem_reserve_count += changeRedeemReserveCount;
    totals.redeem_reserve_volume += changeRedeemReserveVolume;
    totals.fees_zephusd += changeFeesZephusd;
    totals.mint_stable_count += changeMintStableCount;
    totals.mint_stable_volume += changeMintStableVolume;
    totals.redeem_stable_count += changeRedeemStableCount;
    totals.redeem_stable_volume += changeRedeemStableVolume;
    totals.fees_zeph += changeFeesZeph;
    totals.mint_yield_count += changeMintYieldCount;
    totals.mint_yield_volume += changeMintYieldVolume;
    totals.fees_zyield += changeFeesZyield;
    totals.redeem_yield_count += changeRedeemYieldCount;
    totals.redeem_yield_volume += changeRedeemYieldVolume;
    totals.fees_zephusd_yield += changeFeesZephusdYield;

    // Log the timestamp and changes with the updated totals
    writeLogToFile(
      `\nTimestamp: ${record.timestamp} (${timestampHR})\n` +
      `Totals:\n` +
      `  Conversion Transactions: ${totals.conversion_transactions} (+${changeConversionTransactions})\n` +
      `  Yield Conversion Transactions: ${totals.yield_conversion_transactions} (+${changeYieldConversionTransactions})\n` +
      `  Mint Reserve Count: ${totals.mint_reserve_count} (+${changeMintReserveCount})\n` +
      `  Mint Reserve Volume: ${totals.mint_reserve_volume} (+${changeMintReserveVolume})\n` +
      `  Fees Zephrsv: ${totals.fees_zephrsv} (+${changeFeesZephrsv})\n` +
      `  Redeem Reserve Count: ${totals.redeem_reserve_count} (+${changeRedeemReserveCount})\n` +
      `  Redeem Reserve Volume: ${totals.redeem_reserve_volume} (+${changeRedeemReserveVolume})\n` +
      `  Fees Zephusd: ${totals.fees_zephusd} (+${changeFeesZephusd})\n` +
      `  Mint Stable Count: ${totals.mint_stable_count} (+${changeMintStableCount})\n` +
      `  Mint Stable Volume: ${totals.mint_stable_volume} (+${changeMintStableVolume})\n` +
      `  Redeem Stable Count: ${totals.redeem_stable_count} (+${changeRedeemStableCount})\n` +
      `  Redeem Stable Volume: ${totals.redeem_stable_volume} (+${changeRedeemStableVolume})\n` +
      `  Fees Zeph: ${totals.fees_zeph} (+${changeFeesZeph})\n` +
      `  Mint Yield Count: ${totals.mint_yield_count} (+${changeMintYieldCount})\n` +
      `  Mint Yield Volume: ${totals.mint_yield_volume} (+${changeMintYieldVolume})\n` +
      `  Fees Zyield: ${totals.fees_zyield} (+${changeFeesZyield})\n` +
      `  Redeem Yield Count: ${totals.redeem_yield_count} (+${changeRedeemYieldCount})\n` +
      `  Redeem Yield Volume: ${totals.redeem_yield_volume} (+${changeRedeemYieldVolume})\n` +
      `  Fees Zephusd Yield: ${totals.fees_zephusd_yield} (+${changeFeesZephusdYield})`
    );
  }


  // Get the current block height from aggregator
  const currentBlockHeight = await getRedisHeight();

  // Define relevant fields for block-level data
  const relevantBlockFields: (keyof ProtocolStats)[] = [
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
  ];

  console.log(`
  _______________________________________________________
  Recalculating totals for the last 720 blocks... (from block ${currentBlockHeight - 720} to ${currentBlockHeight})
  This is to account for any data not get aggregated into the daily stats.
  _______________________________________________________
  `);
  writeLogToFile(`
  _______________________________________________________
  Recalculating totals for the last 720 blocks... (from block ${currentBlockHeight - 720} to ${currentBlockHeight})
  This is to account for any data not get aggregated into the daily stats.
  _______________________________________________________
  `);

  // Fetch block-level data for the last 720 blocks
  const protocolStats = await getBlockProtocolStatsFromRedis((currentBlockHeight - 720).toString(), undefined, relevantBlockFields);

  let reachedFirstBlock = false;
  for (const record of protocolStats) {
    if (record.data.block_timestamp < (aggregatedData[aggregatedData.length - 1].timestamp || 0)) {
      continue;
    }

    if (!reachedFirstBlock) {
      console.log(`Skipped until block ${record.block_height} as blocks ${(currentBlockHeight - 720)} - ${record.block_height - 1} already accounted for in daily stats.`);
      console.log(`Reached first block not accounted for in daily stats: ${record.block_height}. Processing from here...`);
      writeLogToFile(`Skipped until block ${record.block_height} as blocks ${(currentBlockHeight - 720)} - ${record.block_height - 1} already accounted for in daily stats.`);
      writeLogToFile(`Reached first block not accounted for in daily stats: ${record.block_height}. Processing from here...`);
      reachedFirstBlock = true;
    }

    // Calculate changes
    const changeConversionTransactions = record.data.conversion_transactions_count ?? 0;
    const changeYieldConversionTransactions = record.data.yield_conversion_transactions_count ?? 0;
    const changeMintReserveCount = record.data.mint_reserve_count ?? 0;
    const changeMintReserveVolume = record.data.mint_reserve_volume ?? 0;
    const changeFeesZephrsv = record.data.fees_zephrsv ?? 0;
    const changeRedeemReserveCount = record.data.redeem_reserve_count ?? 0;
    const changeRedeemReserveVolume = record.data.redeem_reserve_volume ?? 0;
    const changeFeesZephusd = record.data.fees_zephusd ?? 0;
    const changeMintStableCount = record.data.mint_stable_count ?? 0;
    const changeMintStableVolume = record.data.mint_stable_volume ?? 0;
    const changeRedeemStableCount = record.data.redeem_stable_count ?? 0;
    const changeRedeemStableVolume = record.data.redeem_stable_volume ?? 0;
    const changeFeesZeph = record.data.fees_zeph ?? 0;
    const changeMintYieldCount = record.data.mint_yield_count ?? 0;
    const changeMintYieldVolume = record.data.mint_yield_volume ?? 0;
    const changeFeesZyield = record.data.fees_zyield ?? 0;
    const changeRedeemYieldCount = record.data.redeem_yield_count ?? 0;
    const changeRedeemYieldVolume = record.data.redeem_yield_volume ?? 0;
    const changeFeesZephusdYield = record.data.fees_zephusd_yield ?? 0;

    // Update totals
    totals.conversion_transactions += changeConversionTransactions;
    totals.yield_conversion_transactions += changeYieldConversionTransactions;
    totals.mint_reserve_count += changeMintReserveCount;
    totals.mint_reserve_volume += changeMintReserveVolume;
    totals.fees_zephrsv += changeFeesZephrsv;
    totals.redeem_reserve_count += changeRedeemReserveCount;
    totals.redeem_reserve_volume += changeRedeemReserveVolume;
    totals.fees_zephusd += changeFeesZephusd;
    totals.mint_stable_count += changeMintStableCount;
    totals.mint_stable_volume += changeMintStableVolume;
    totals.redeem_stable_count += changeRedeemStableCount;
    totals.redeem_stable_volume += changeRedeemStableVolume;
    totals.fees_zeph += changeFeesZeph;
    totals.mint_yield_count += changeMintYieldCount;
    totals.mint_yield_volume += changeMintYieldVolume;
    totals.fees_zyield += changeFeesZyield;
    totals.redeem_yield_count += changeRedeemYieldCount;
    totals.redeem_yield_volume += changeRedeemYieldVolume;
    totals.fees_zephusd_yield += changeFeesZephusdYield;

    // Log the block height and changes with the updated totals
    writeLogToFile(
      `Block Height: ${record.block_height}\n` +
      `Totals:\n` +
      `  Conversion Transactions: ${totals.conversion_transactions} (+${changeConversionTransactions})\n` +
      `  Yield Conversion Transactions: ${totals.yield_conversion_transactions} (+${changeYieldConversionTransactions})\n` +
      `  Mint Reserve Count: ${totals.mint_reserve_count} (+${changeMintReserveCount})\n` +
      `  Mint Reserve Volume: ${totals.mint_reserve_volume} (+${changeMintReserveVolume})\n` +
      `  Fees Zephrsv: ${totals.fees_zephrsv} (+${changeFeesZephrsv})\n` +
      `  Redeem Reserve Count: ${totals.redeem_reserve_count} (+${changeRedeemReserveCount})\n` +
      `  Redeem Reserve Volume: ${totals.redeem_reserve_volume} (+${changeRedeemReserveVolume})\n` +
      `  Fees Zephusd: ${totals.fees_zephusd} (+${changeFeesZephusd})\n` +
      `  Mint Stable Count: ${totals.mint_stable_count} (+${changeMintStableCount})\n` +
      `  Mint Stable Volume: ${totals.mint_stable_volume} (+${changeMintStableVolume})\n` +
      `  Redeem Stable Count: ${totals.redeem_stable_count} (+${changeRedeemStableCount})\n` +
      `  Redeem Stable Volume: ${totals.redeem_stable_volume} (+${changeRedeemStableVolume})\n` +
      `  Fees Zeph: ${totals.fees_zeph} (+${changeFeesZeph})\n` +
      `  Mint Yield Count: ${totals.mint_yield_count} (+${changeMintYieldCount})\n` +
      `  Mint Yield Volume: ${totals.mint_yield_volume} (+${changeMintYieldVolume})\n` +
      `  Fees Zyield: ${totals.fees_zyield} (+${changeFeesZyield})\n` +
      `  Redeem Yield Count: ${totals.redeem_yield_count} (+${changeRedeemYieldCount})\n` +
      `  Redeem Yield Volume: ${totals.redeem_yield_volume} (+${changeRedeemYieldVolume})\n` +
      `  Fees Zephusd Yield: ${totals.fees_zephusd_yield} (+${changeFeesZephusdYield})`
    );
  }

  console.log(`
    _______________________________________________________
    Recalculating totals for block reward info...
    _______________________________________________________`)
  writeLogToFile(`
    _______________________________________________________
    Recalculating totals for block reward info...
    _______________________________________________________`)

  // As this scanner only processes from v1, we can add in the accm rewards before then
  totals.miner_reward = 1391857.1317692809
  totals.governance_reward = 73255.6385141733

  // Populate the totals with the rewards from block reward info
  const starting_height = 89300;
  for (let height = starting_height; height <= currentBlockHeight; height++) {
    const bri = await getRedisBlockRewardInfo(height);

    // Calculate changes
    const changeMinerReward = bri.miner_reward ?? 0;
    const changeGovernanceReward = bri.governance_reward ?? 0;
    const changeReserveReward = bri.reserve_reward ?? 0;
    const changeYieldReward = bri.yield_reward ?? 0;

    // Update totals
    totals.miner_reward += changeMinerReward;
    totals.governance_reward += changeGovernanceReward;
    totals.reserve_reward += changeReserveReward;
    totals.yield_reward += changeYieldReward;

    // Log the block height and changes with the updated totals
    // writeLogToFile(
    //   `Block Height: ${height}\n` +
    //   `Totals:\n` +
    //   `  Miner Reward: ${totals.miner_reward} (+${changeMinerReward})\n` +
    //   `  Governance Reward: ${totals.governance_reward} (+${changeGovernanceReward})\n` +
    //   `  Reserve Reward: ${totals.reserve_reward} (+${changeReserveReward})\n` +
    //   `  Yield Reward: ${totals.yield_reward} (+${changeYieldReward})`
    // );
  }

  // Delete the previous totals key in Redis
  await redis.del("totals");

  // Set all the recalculated totals back in Redis
  for (const [key, value] of Object.entries(totals)) {
    if (isNaN(value)) {
      console.log(`Value for ${key} is NaN, skipping...`);
      continue;
    }
    await redis.hincrbyfloat("totals", key, value);
  }

  console.log(`Totals recalculated successfully.`);
  console.log(totals);
}







