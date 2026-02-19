// This file handles rolling the scanner back and removing all data from after the supplied height.
// This is not only for debugging purposes, but also for when the chain reorgs.
import * as fs from "fs";

import { aggregate } from "./aggregator";
import { processZYSPriceHistory, scanPricingRecords } from "./pr";
import redis from "./redis";
import { scanTransactions } from "./tx";
import {
  AggregatedData,
  ProtocolStats,
  getAggregatedProtocolStatsFromRedis,
  getBlock,
  getBlockProtocolStatsFromRedis,
  getCurrentBlockHeight,
  getRedisBlockRewardInfo,
  getScannerHeight,
} from "./utils";
import { determineAPYHistory, determineHistoricalReturns, determineProjectedReturns } from "./yield";
import { usePostgres, useRedis } from "./config";
import { stores } from "./storage/factory";
import { deleteTransactionsAboveHeight } from "./db/transactions";
import { deleteBlockRewardsAboveHeight } from "./db/blockRewards";
import { deleteBlockStatsAboveHeight, deleteAggregatesFromTimestamp } from "./db/protocolStats";
import { getPrismaClient } from "./db";
import { clearPostgresAggregationState, truncatePostgresData } from "./db/admin";
import {
  calculateTotalsFromPostgres,
  setTotals as setSqlTotals,
  TotalsRecord,
} from "./db/totals";

// Function to append log information to a file
function writeLogToFile(logContent: string) {
  fs.appendFileSync('totals_log.txt', logContent + '\n', 'utf8');
}

interface RedisTotals {
  conversion_transactions: number;
  yield_conversion_transactions: number;
  mint_reserve_count: number;
  mint_reserve_volume: number;
  fees_zephrsv: number;
  redeem_reserve_count: number;
  redeem_reserve_volume: number;
  fees_zephusd: number;
  mint_stable_count: number;
  mint_stable_volume: number;
  redeem_stable_count: number;
  redeem_stable_volume: number;
  fees_zeph: number;
  mint_yield_count: number;
  mint_yield_volume: number;
  fees_zyield: number;
  redeem_yield_count: number;
  redeem_yield_volume: number;
  fees_zephusd_yield: number;
  miner_reward: number;
  governance_reward: number;
  reserve_reward: number;
  yield_reward: number;
}

function redisTotalsFromRecord(record: TotalsRecord): RedisTotals {
  return {
    conversion_transactions: record.conversionTransactions,
    yield_conversion_transactions: record.yieldConversionTransactions,
    mint_reserve_count: record.mintReserveCount,
    mint_reserve_volume: record.mintReserveVolume,
    fees_zephrsv: record.feesZephrsv,
    redeem_reserve_count: record.redeemReserveCount,
    redeem_reserve_volume: record.redeemReserveVolume,
    fees_zephusd: record.feesZephusd,
    mint_stable_count: record.mintStableCount,
    mint_stable_volume: record.mintStableVolume,
    redeem_stable_count: record.redeemStableCount,
    redeem_stable_volume: record.redeemStableVolume,
    fees_zeph: record.feesZeph,
    mint_yield_count: record.mintYieldCount,
    mint_yield_volume: record.mintYieldVolume,
    fees_zyield: record.feesZyield,
    redeem_yield_count: record.redeemYieldCount,
    redeem_yield_volume: record.redeemYieldVolume,
    fees_zephusd_yield: record.feesZephusdYield,
    miner_reward: record.minerReward,
    governance_reward: record.governanceReward,
    reserve_reward: record.reserveReward,
    yield_reward: record.yieldReward,
  };
}

function totalsRecordFromRedis(totals: RedisTotals): TotalsRecord {
  return {
    conversionTransactions: totals.conversion_transactions,
    yieldConversionTransactions: totals.yield_conversion_transactions,
    mintReserveCount: totals.mint_reserve_count,
    mintReserveVolume: totals.mint_reserve_volume,
    feesZephrsv: totals.fees_zephrsv,
    redeemReserveCount: totals.redeem_reserve_count,
    redeemReserveVolume: totals.redeem_reserve_volume,
    feesZephusd: totals.fees_zephusd,
    mintStableCount: totals.mint_stable_count,
    mintStableVolume: totals.mint_stable_volume,
    redeemStableCount: totals.redeem_stable_count,
    redeemStableVolume: totals.redeem_stable_volume,
    feesZeph: totals.fees_zeph,
    mintYieldCount: totals.mint_yield_count,
    mintYieldVolume: totals.mint_yield_volume,
    feesZyield: totals.fees_zyield,
    redeemYieldCount: totals.redeem_yield_count,
    redeemYieldVolume: totals.redeem_yield_volume,
    feesZephusdYield: totals.fees_zephusd_yield,
    minerReward: totals.miner_reward,
    governanceReward: totals.governance_reward,
    reserveReward: totals.reserve_reward,
    yieldReward: totals.yield_reward,
  };
}

async function writeRedisTotals(totals: RedisTotals): Promise<void> {
  await redis.del("totals");
  const pipeline = redis.pipeline();
  for (const [key, value] of Object.entries(totals)) {
    if (!Number.isFinite(value)) {
      console.log(`Value for ${key} is NaN, skipping...`);
      continue;
    }
    pipeline.hincrbyfloat("totals", key, value);
  }
  await pipeline.exec();
}

async function retallyTotalsViaPostgres({ redisEnabled, postgresEnabled }: { redisEnabled: boolean; postgresEnabled: boolean }): Promise<void> {
  console.log("[retallyTotals] Rebuilding totals from Postgres aggregates...");
  const totalsRecord = await calculateTotalsFromPostgres();
  if (redisEnabled) {
    await writeRedisTotals(redisTotalsFromRecord(totalsRecord));
  } else {
    console.log("[retallyTotals] Redis disabled – skipping totals hash update");
  }
  if (postgresEnabled) {
    await setSqlTotals(totalsRecord);
  }
  console.log("[retallyTotals] Totals recalculated successfully.");
}

export async function rollbackScanner(rollBackHeight: number) {
  const redisEnabled = useRedis();
  const postgresEnabled = usePostgres();
  const prisma = postgresEnabled ? getPrismaClient() : null;

  // Set rolling-back flag
  if (redisEnabled) {
    await redis.set("scanner_rolling_back", "true");
  }
  if (postgresEnabled) {
    await stores.scannerState.set("scanner_rolling_back", "true");
  }

  const daemon_height = await getCurrentBlockHeight();
  const rollback_block_info = await getBlock(rollBackHeight);
  if (!rollback_block_info) {
    console.log(`Block at height ${rollBackHeight} not found`);
    return;
  }

  const rollback_timestamp = rollback_block_info.result.block_header.timestamp;

  console.log(`Rolling back scanner to height ${rollBackHeight}`);
  console.log(`\t Daemon height is ${daemon_height} - We are removing ${daemon_height - rollBackHeight} blocks`);

  // --- Redis cleanup ---
  if (redisEnabled) {
    console.log(`\t Removing block hashes (${rollBackHeight} - ${daemon_height})...`);
    let pipeline = redis.pipeline();
    for (let h = rollBackHeight + 1; h <= daemon_height; h++) {
      pipeline.hdel("block_hashes", h.toString());
    }
    await pipeline.exec();

    console.log(`\t Removing protocol_stats, hourly & daily from Redis...`);
    pipeline = redis.pipeline();
    for (let h = rollBackHeight + 1; h <= daemon_height; h++) {
      pipeline.hdel("protocol_stats", h.toString());
    }
    await pipeline.exec();
    await redis.set("height_aggregator", rollBackHeight.toString());

    const hourlyResults = await redis.zrangebyscore("protocol_stats_hourly", rollback_timestamp.toString(), "+inf");
    pipeline = redis.pipeline();
    for (const score of hourlyResults) { pipeline.zrem("protocol_stats_hourly", score); }
    await pipeline.exec();
    await redis.set("timestamp_aggregator_hourly", rollback_timestamp.toString());

    const dailyResults = await redis.zrangebyscore("protocol_stats_daily", rollback_timestamp.toString(), "+inf");
    pipeline = redis.pipeline();
    for (const score of dailyResults) { pipeline.zrem("protocol_stats_daily", score); }
    await pipeline.exec();
    await redis.set("timestamp_aggregator_daily", rollback_timestamp.toString());

    console.log(`\t Removing pricing_records from Redis...`);
    pipeline = redis.pipeline();
    for (let h = rollBackHeight + 1; h <= daemon_height; h++) {
      pipeline.hdel("pricing_records", h.toString());
    }
    await pipeline.exec();
    await redis.set("height_prs", rollBackHeight.toString());

    console.log(`\t Removing transactions from Redis...`);
    pipeline = redis.pipeline();
    for (let h = rollBackHeight + 1; h <= daemon_height; h++) {
      pipeline.hdel("block_rewards", h.toString());
      const txsByBlockHashes = await redis.hget("txs_by_block", h.toString());
      if (txsByBlockHashes) {
        for (const tx_id of JSON.parse(txsByBlockHashes)) {
          pipeline.hdel("txs", tx_id);
        }
      }
      pipeline.hdel("txs_by_block", h.toString());
    }
    await pipeline.exec();
    await redis.set("height_txs", rollBackHeight.toString());
  }

  // --- Postgres cleanup ---
  if (postgresEnabled) {
    console.log(`\t Removing data from Postgres above height ${rollBackHeight}...`);
    await deleteBlockStatsAboveHeight(rollBackHeight);
    await deleteAggregatesFromTimestamp("hour", rollback_timestamp);
    await deleteAggregatesFromTimestamp("day", rollback_timestamp);
    if (prisma) {
      await prisma.pricingRecord.deleteMany({
        where: { blockHeight: { gt: rollBackHeight } },
      });
    }
    await deleteBlockRewardsAboveHeight(rollBackHeight);
    await deleteTransactionsAboveHeight(rollBackHeight);
    await stores.scannerState.set("height_aggregator", rollBackHeight.toString());
    await stores.scannerState.set("timestamp_aggregator_hourly", rollback_timestamp.toString());
    await stores.scannerState.set("timestamp_aggregator_daily", rollback_timestamp.toString());
    await stores.scannerState.set("height_prs", rollBackHeight.toString());
    await stores.scannerState.set("height_txs", rollBackHeight.toString());
  }

  console.log(`\t Rescanning Pricing Records...`);
  await scanPricingRecords();

  console.log(`\t Rescanning Transactions...`);
  await scanTransactions();

  console.log(`\t Firing aggregator to repop protocol_stats...`);
  await aggregate();

  console.log(`\t Recalculating APY History...`);
  await determineAPYHistory(true);

  console.log(`\t Recalculating totals...`);
  await retallyTotals();

  console.log(`Rollback to height ${rollBackHeight} and reset completed successfully`);

  // Unset the scanner_rolling_back flag
  if (redisEnabled) {
    await redis.del("scanner_rolling_back");
  }
  if (postgresEnabled) {
    await stores.scannerState.set("scanner_rolling_back", "false");
  }
}


export async function detectAndHandleReorg() {
  if (!useRedis()) {
    // Reorg detection relies on block_hashes stored in Redis.
    // In postgres-only mode, skip until a Postgres-backed implementation exists.
    return;
  }

  await setBlockHashesIfEmpty();

  // Start from the current scanner height
  let storedHeight = await getScannerHeight();
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
  const starting_height = 0;
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
  const postgresEnabled = usePostgres();
  const redisEnabled = useRedis();
  const forceRedisRetally = process.env.RETALLY_FORCE_REDIS === "1";

  if (postgresEnabled && !forceRedisRetally) {
    await retallyTotalsViaPostgres({ redisEnabled, postgresEnabled });
    return;
  }

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
  const totals: RedisTotals = {
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
  const currentBlockHeight = await getScannerHeight();

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

  // Populate the totals with the rewards from block reward info
  const starting_height = 0;
  for (let height = starting_height; height <= currentBlockHeight; height++) {
    try {
      const bri = await getRedisBlockRewardInfo(height);
      if (!bri) {
        console.log(`Block reward info at height ${height} not found, skipping...`);
        continue;
      }
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
    } catch (error) {
      console.error(`Error: reTallyTotals - block reward info at block height ${height}:`, error);
    }
  }

  if (redisEnabled) {
    await writeRedisTotals(totals);
  } else {
    console.log("[retallyTotals] Redis disabled – skipping totals hash update");
  }

  console.log(`Totals recalculated successfully.`);
  console.log(totals);

  if (postgresEnabled) {
    await setSqlTotals(totalsRecordFromRedis(totals));
  }
}


const AGGREGATION_KEYS = [
  "protocol_stats",
  "protocol_stats_hourly",
  "protocol_stats_daily",
  "height_aggregator",
  "timestamp_aggregator_hourly",
  "timestamp_aggregator_daily",
  "historical_returns",
  "projected_returns",
  "apy_history",
];

async function clearAggregationArtifacts() {
  if (useRedis()) {
    const pipeline = redis.pipeline();
    for (const key of AGGREGATION_KEYS) {
      pipeline.del(key);
    }
    await pipeline.exec();
  }
  await clearPostgresAggregationState();
}

type ResetScope = "full" | "aggregation";

export async function resetScanner(scope: ResetScope = "aggregation") {
  const redisEnabled = useRedis();
  const postgresEnabled = usePostgres();

  console.log(`[resetScanner] Starting ${scope} reset`);
  if (redisEnabled) {
    await redis.set("scanner_rolling_back", "true");
  }
  if (postgresEnabled) {
    await stores.scannerState.set("scanner_rolling_back", "true");
  }

  try {
    if (scope === "full") {
      if (redisEnabled) {
        console.log("[resetScanner] Flushing Redis database");
        await redis.flushdb();
      }
      await truncatePostgresData();

      console.log("[resetScanner] Rescanning pricing records");
      await scanPricingRecords();

      console.log("[resetScanner] Rescanning transactions");
      await scanTransactions(true);
    } else {
      console.log("[resetScanner] Clearing aggregation artefacts");
      await clearAggregationArtifacts();
    }

    console.log("[resetScanner] Running aggregation");
    await aggregate();

    console.log("[resetScanner] Rebuilding ZYS price history");
    if (redisEnabled) {
      await redis.del("zys_price_history");
    }
    await processZYSPriceHistory();

    console.log("[resetScanner] Rebuilding yield analytics");
    await determineHistoricalReturns();
    await determineProjectedReturns();
    await determineAPYHistory(true);

    console.log("[resetScanner] Retallying totals");
    await retallyTotals();

    console.log(`[resetScanner] ${scope} reset completed`);
  } catch (error) {
    console.error(`[resetScanner] ${scope} reset failed`, error);
    throw error;
  } finally {
    if (redisEnabled) {
      await redis.del("scanner_rolling_back");
    }
    if (postgresEnabled) {
      await stores.scannerState.set("scanner_rolling_back", "false");
    }
  }
}
