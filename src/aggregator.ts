// Take all data and aggregate into a single redis key done by block, hourly and daily.

import fs from "node:fs/promises";
import path from "node:path";

import redis from "./redis";
import {
  AggregatedData,
  ProtocolStats,
  getScannerHeight,
  getPricingRecordFromStore,
  getScannerTimestampDaily,
  getScannerTimestampHourly,
  getReserveDiffs,
  getReserveInfo,
  getLastReserveSnapshotPreviousHeight,
  recordReserveMismatch,
  clearReserveMismatch,
  saveReserveSnapshotToRedis,
  getLatestProtocolStats,
  getPricingRecordHeight,
  getRedisBlockRewardInfo,
  setProtocolStats,
  ReserveDiffReport,
  RESERVE_SNAPSHOT_INTERVAL_BLOCKS,
  RESERVE_SNAPSHOT_START_HEIGHT,
  WALKTHROUGH_SNAPSHOT_SOURCE,
  RESERVE_DIFF_TOLERANCE,
  HOURLY_PENDING_KEY,
  DAILY_PENDING_KEY,
} from "./utils";
import { UNAUDITABLE_ZEPH_MINT } from "./constants";
import { logAggregatedSummary, logReserveDiffReport, logReserveHeights, logReserveSnapshotStatus } from "./logger";
import { usePostgres, useRedis } from "./config";
import {
  saveBlockProtocolStats,
  saveAggregatedProtocolStats,
  fetchBlockProtocolStatsByTimestampRange,
  getProtocolStatsBlock,
} from "./db/protocolStats";
import {
  getTransactionsByBlock,
  getTransactionsByHashes,
  type ConversionTransactionRecord,
} from "./db/transactions";
import { setAggregatorHeight, setDailyTimestamp, setHourlyTimestamp, getTransactionHeight } from "./scannerState";
// const DEATOMIZE = 10 ** -12;
const HF_VERSION_1_HEIGHT = 89300;
const HF_VERSION_1_TIMESTAMP = 1696152427;

const ARTEMIS_HF_V5_BLOCK_HEIGHT = 295000;

const VERSION_2_HF_V6_BLOCK_HEIGHT = 360000;
const VERSION_2_HF_V6_TIMESTAMP = 1728817200; // ESTIMATED. TO BE UPDATED?
const VERSION_2_3_0_HF_V11_BLOCK_HEIGHT = 536000; // Post Audit, asset type changes.

const RESERVE_SNAPSHOT_INTERVAL = RESERVE_SNAPSHOT_INTERVAL_BLOCKS;
const ATOMIC_UNITS = 1_000_000_000_000n; // 1 ZEPH/ZSD in atomic units
const ATOMIC_UNITS_NUMBER = Number(ATOMIC_UNITS);

const BLOCK_COUNTER_DEFAULTS = {
  zsd_minted_for_yield: 0,
  conversion_transactions_count: 0,
  yield_conversion_transactions_count: 0,
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
  redeem_yield_count: 0,
  redeem_yield_volume: 0,
  fees_zephusd_yield: 0,
  fees_zyield: 0,
};

const WALKTHROUGH_MODE = process.env.WALKTHROUGH_MODE === "true";
const WALKTHROUGH_DIFF_THRESHOLD = Number(process.env.WALKTHROUGH_DIFF_THRESHOLD ?? "1");
const WALKTHROUGH_REPORT_PATH = process.env.WALKTHROUGH_REPORT_PATH;
const WALKTHROUGH_REPORT_DIR = process.env.WALKTHROUGH_REPORT_DIR ?? "walkthrough_reports";
const WALKTHROUGH_RESERVE_LOG = process.env.WALKTHROUGH_RESERVE_LOG ?? "walkthrough_reserve.log";
const RESERVE_FORCE_RECONCILE_LOG = process.env.RESERVE_FORCE_RECONCILE_LOG ?? "reserve_force_reconcile.log";

interface WalkthroughDiffRecord {
  blockHeight: number;
  report: ReserveDiffReport;
  conversionTransactions: number;
}

const walkthroughDiffHistory: WalkthroughDiffRecord[] = [];

function toAtoms(value: number): bigint {
  if (!Number.isFinite(value)) {
    return 0n;
  }
  return BigInt(Math.round(value * ATOMIC_UNITS_NUMBER));
}

function atomsToNumber(atoms: bigint): number {
  const integerPart = atoms / ATOMIC_UNITS;
  const fractionalPart = atoms % ATOMIC_UNITS;
  return Number(integerPart) + Number(fractionalPart) / ATOMIC_UNITS_NUMBER;
}

interface PricingRecord {
  height: number;
  timestamp: number;
  spot: number;
  moving_average: number;
  reserve: number;
  reserve_ma: number;
  stable: number;
  stable_ma: number;
  yield_price: number;
}

interface BlockRewardInfo {
  height: number;
  miner_reward: number;
  governance_reward: number;
  reserve_reward: number;
  yield_reward: number;
  miner_reward_atoms?: string;
  governance_reward_atoms?: string;
  reserve_reward_atoms?: string;
  yield_reward_atoms?: string;
  base_reward_atoms?: string;
  fee_adjustment_atoms?: string;
}

function convertZephToZsd(amountZeph: number, stable: number, stableMA: number, blockHeight: number): number {
  if (!amountZeph || amountZeph <= 0) {
    return 0;
  }

  const exchangeRate = Math.max(stable, stableMA);
  if (!exchangeRate || exchangeRate <= 0) {
    return 0;
  }

  const amountAtoms = BigInt(Math.round(amountZeph * ATOMIC_UNITS_NUMBER));
  const exchangeAtoms = BigInt(Math.round(exchangeRate * ATOMIC_UNITS_NUMBER));
  if (exchangeAtoms === 0n) {
    return 0;
  }

  let rate = (ATOMIC_UNITS * ATOMIC_UNITS) / exchangeAtoms;
  const feeDivisor = blockHeight >= ARTEMIS_HF_V5_BLOCK_HEIGHT ? 1000n : 50n; // 0.1% post Artemis, 2% before
  rate -= rate / feeDivisor;
  rate -= rate % 10000n; // mimic daemon truncation for determinism

  const stableAtoms = (amountAtoms * rate) / ATOMIC_UNITS;
  return Number(stableAtoms) / ATOMIC_UNITS_NUMBER;
}

interface Transaction {
  hash: string;
  block_height: number;
  block_timestamp: number;
  conversion_type: string;
  conversion_rate: number;
  from_asset: string;
  from_amount: number;
  from_amount_atoms?: string;
  to_asset: string;
  to_amount: number;
  to_amount_atoms?: string;
  conversion_fee_asset: string;
  conversion_fee_amount: number;
  conversion_fee_atoms?: string;
  tx_fee_asset: string;
  tx_fee_amount: number;
  tx_fee_atoms?: string;
}

const transactionCache = new Map<number, Transaction[]>();

function mapDbTransaction(record: ConversionTransactionRecord): Transaction {
  return {
    hash: record.hash,
    block_height: record.blockHeight,
    block_timestamp: record.blockTimestamp,
    conversion_type: record.conversionType,
    conversion_rate: record.conversionRate,
    from_asset: record.fromAsset,
    from_amount: record.fromAmount,
    from_amount_atoms: record.fromAmountAtoms,
    to_asset: record.toAsset,
    to_amount: record.toAmount,
    to_amount_atoms: record.toAmountAtoms,
    conversion_fee_asset: record.conversionFeeAsset,
    conversion_fee_amount: record.conversionFeeAmount,
    conversion_fee_atoms: undefined,
    tx_fee_asset: record.txFeeAsset,
    tx_fee_amount: record.txFeeAmount,
    tx_fee_atoms: record.txFeeAtoms,
  };
}

async function getProtocolStatsRecord(height: number): Promise<ProtocolStats | null> {
  if (height < 0) {
    return null;
  }
  if (usePostgres()) {
    return getProtocolStatsBlock(height);
  }
  if (!useRedis()) return null;
  const json = await redis.hget("protocol_stats", height.toString());
  if (!json) {
    return null;
  }
  try {
    return JSON.parse(json) as ProtocolStats;
  } catch (error) {
    console.error(`Error parsing protocol stats for height ${height}:`, error);
    return null;
  }
}

async function getTransactionHashesForBlock(height: number): Promise<string[]> {
  if (usePostgres()) {
    const rows = await getTransactionsByBlock(height);
    const mapped = rows.map(mapDbTransaction);
    transactionCache.set(height, mapped);
    return mapped.map((tx) => tx.hash);
  }
  if (!useRedis()) return [];
  const json = await redis.hget("txs_by_block", height.toString());
  if (!json) {
    return [];
  }
  try {
    return JSON.parse(json) as string[];
  } catch (error) {
    console.error(`Error parsing txs_by_block entry for height ${height}:`, error);
    return [];
  }
}

async function loadProtocolStatsForRange(startTimestamp: number, endTimestamp: number): Promise<ProtocolStats[]> {
  if (usePostgres()) {
    return fetchBlockProtocolStatsByTimestampRange(startTimestamp, endTimestamp);
  }
  if (!useRedis()) return [];
  const raw = await redis.hgetall("protocol_stats");
  if (!raw) {
    return [];
  }
  const records: ProtocolStats[] = [];
  for (const blockDataJson of Object.values(raw)) {
    try {
      const parsed = JSON.parse(blockDataJson) as ProtocolStats;
      if (
        parsed.block_timestamp >= startTimestamp &&
        parsed.block_timestamp < endTimestamp
      ) {
        records.push(parsed);
      }
    } catch (error) {
      console.error("Failed to parse protocol stats entry:", error);
    }
  }
  records.sort((a, b) => a.block_timestamp - b.block_timestamp);
  return records;
}

function bucketProtocolStatsByWindow(
  stats: ProtocolStats[],
  startTimestamp: number,
  windowSize: number
): Map<number, ProtocolStats[]> {
  const buckets = new Map<number, ProtocolStats[]>();
  for (const record of stats) {
    if (record.block_timestamp < startTimestamp) {
      continue;
    }
    const index = Math.floor((record.block_timestamp - startTimestamp) / windowSize);
    const bucketStart = startTimestamp + index * windowSize;
    if (bucketStart >= startTimestamp) {
      const bucket = buckets.get(bucketStart);
      if (bucket) {
        bucket.push(record);
      } else {
        buckets.set(bucketStart, [record]);
      }
    }
  }
  return buckets;
}

// once off function to process all conversion txs and populate the txs by block key
async function populateTxsByBlock(): Promise<void> {
  try {
    console.log("Starting to populate txs by block...");

    // Fetch all transactions
    const txs = await redis.hgetall("txs");

    // Check if transactions exist
    if (!txs) {
      console.log("No transactions found.");
      return;
    }

    const txsByBlock: Record<number, string[]> = {};

    for (const [hash, txJson] of Object.entries(txs)) {
      const tx: Transaction = JSON.parse(txJson);
      const block_height = tx.block_height;

      if (!txsByBlock[block_height]) {
        txsByBlock[block_height] = [];
      }
      txsByBlock[block_height].push(hash);
    }

    // Store the aggregated transactions by block
    for (const [blockHeight, hashes] of Object.entries(txsByBlock)) {
      await redis.hset("txs_by_block", blockHeight, JSON.stringify(hashes));
    }

    console.log("Finished populating txs by block");
  } catch (error) {
    console.error("Error populating txs by block:", error);
  }
}

export async function aggregate() {
  const redisEnabled = useRedis();
  if (redisEnabled) {
    // hangover fix from old implementation
    const txsByBlockExists = await redis.exists("txs_by_block");
    if (!txsByBlockExists) {
      console.log("No txs by block found, populating...");
      await populateTxsByBlock();
    }
  }

  console.log(`Starting aggregation...`);

  const current_height_prs = await getPricingRecordHeight();
  const current_height_txs = await getTransactionHeight();

  if (!current_height_prs || !current_height_txs) {
    console.log("No current height found for pricing records or transactions — skipping aggregation");
    return;
  }

  // by block
  const height_by_block = await getScannerHeight(); // where we are at in the data aggregation
  const height_to_process = Math.max(height_by_block + 1, HF_VERSION_1_HEIGHT); // only process from HF_VERSION_1_HEIGHT

  // Aggregate only up to the minimum of pricing and tx heights — both inputs are required
  const lastBlockToProcess = Math.min(current_height_prs, current_height_txs);
  console.log(`\tAggregating from block: ${height_to_process} to ${lastBlockToProcess}`);
  // const lastBlockToProcess = 89303; // TEMP OVERRIDE FOR TESTING
  if (lastBlockToProcess < height_to_process) {
    console.log("\tNo new blocks to aggregate.");
  } else {
    if (WALKTHROUGH_MODE) {
      walkthroughDiffHistory.length = 0;
      await fs.writeFile(WALKTHROUGH_RESERVE_LOG, "");
    }
    const totalBlocks = lastBlockToProcess - height_to_process + 1;
    const progressInterval = Math.max(1, Math.floor(totalBlocks / 20));

    let aggregationAborted = false;
    for (let i = height_to_process; i <= lastBlockToProcess; i++) {
      const shouldLog = i === lastBlockToProcess || (i - height_to_process) % progressInterval === 0;
      const ok = await aggregateBlock(i, shouldLog);
      if (!ok) {
        console.error(`[aggregate] Block aggregation halted at height ${i}. Skipping remaining blocks.`);
        aggregationAborted = true;
        break;
      }
    }

    if (aggregationAborted) {
      console.log(`Finished aggregation (aborted early)`);
      return;
    }
  }

  // get pr for current_height_prs
  const current_pr = await getPricingRecordFromStore(current_height_prs);
  const timestamp_hourly = await getScannerTimestampHourly();
  const timestamp_daily = await getScannerTimestampDaily();

  await aggregateByTimestamp(Math.max(timestamp_hourly, HF_VERSION_1_TIMESTAMP), current_pr.timestamp, "hourly");
  await aggregateByTimestamp(Math.max(timestamp_daily, HF_VERSION_1_TIMESTAMP), current_pr.timestamp, "daily");

  if (WALKTHROUGH_MODE) {
    await outputWalkthroughDiffReport();
  }

  const latestAggregatedHeight = await getScannerHeight();
  await handleReserveIntegrity(latestAggregatedHeight);

  console.log(`Finished aggregation`);
}

const MAX_RECOVERY_DEPTH = 100;

async function loadBlockInputs(height: number, recoveryDepth = 0) {
  const [pr, bri, prevBlockData, txHashes] = await Promise.all([
    getPricingRecordFromStore(height),
    getRedisBlockRewardInfo(height),
    getProtocolStatsRecord(height - 1),
    getTransactionHashesForBlock(height),
  ]);

  // At HFv1 start, missing prevBlockData is expected — there's no predecessor
  if (height <= HF_VERSION_1_HEIGHT + 1) {
    return { pr, bri, prevBlockData: prevBlockData ?? null, txHashes };
  }

  // For later heights, missing prevBlockData is a real problem — attempt recovery
  if (!prevBlockData) {
    if (recoveryDepth >= MAX_RECOVERY_DEPTH) {
      console.error(`[loadBlockInputs] FATAL: Recovery depth limit (${MAX_RECOVERY_DEPTH}) reached at height ${height - 1}. Too many consecutive missing blocks.`);
      return { pr, bri, prevBlockData: null, txHashes };
    }

    console.warn(`[loadBlockInputs] Missing prevBlockData for height ${height - 1}. Attempting re-aggregation (depth ${recoveryDepth + 1}/${MAX_RECOVERY_DEPTH})...`);

    await aggregateBlock(height - 1, false, recoveryDepth + 1);

    const retryData = await getProtocolStatsRecord(height - 1);

    if (!retryData) {
      console.error(`[loadBlockInputs] FATAL: Re-aggregation failed for height ${height - 1}. Cannot proceed.`);
      return { pr, bri, prevBlockData: null, txHashes };
    }

    console.log(`[loadBlockInputs] Successfully recovered prevBlockData for height ${height - 1}`);
    return { pr, bri, prevBlockData: retryData, txHashes };
  }

  return { pr, bri, prevBlockData, txHashes };
}

async function fetchTransactions(blockHeight: number, hashes: string[]): Promise<Map<string, Transaction>> {
  const txMap = new Map<string, Transaction>();
  if (hashes.length === 0) {
    return txMap;
  }

  if (usePostgres()) {
    let cached = transactionCache.get(blockHeight);
    if (!cached) {
      const rows = await getTransactionsByBlock(blockHeight);
      cached = rows.map(mapDbTransaction);
    }
    transactionCache.delete(blockHeight);
    cached.forEach((tx) => {
      txMap.set(tx.hash, tx);
    });
    return txMap;
  }

  if (!useRedis()) return txMap;
  const pipeline = redis.pipeline();
  for (const hash of hashes) {
    pipeline.hget("txs", hash);
  }

  const results = await pipeline.exec();
  if (!results) {
    return txMap;
  }

  results.forEach((result, index) => {
    const [err, json] = result;
    const hash = hashes[index];
    if (err) {
      console.error(`Error fetching transaction ${hash}:`, err);
      return;
    }
    if (!json) {
      return;
    }
    try {
      const jsonString = typeof json === "string" ? json : json?.toString?.();
      if (!jsonString) {
        return;
      }
      txMap.set(hash, JSON.parse(jsonString) as Transaction);
    } catch (error) {
      console.error(`Error parsing transaction ${hash}:`, error);
    }
  });

  return txMap;
}

async function aggregateBlock(height_to_process: number, logProgress = false, recoveryDepth = 0): Promise<boolean> {
  if (logProgress) {
    console.log(`\tAggregating block: ${height_to_process}`);
  }

  const { pr, bri, prevBlockData, txHashes } = await loadBlockInputs(height_to_process, recoveryDepth);

  if (!pr) {
    console.log("No pricing record found for height: ", height_to_process);
    return false;
  }
  if (!bri) {
    console.log("No block reward info found for height: ", height_to_process);
    return false;
  }

  // Abort if prevBlockData is missing and recovery failed (post-HFv1)
  if (!prevBlockData && height_to_process > HF_VERSION_1_HEIGHT + 1) {
    console.error(`[aggregate] ABORT: Cannot aggregate block ${height_to_process} — missing prevBlockData and recovery failed. Height will NOT advance.`);
    return false;
  }

  const transactionsByHash = await fetchTransactions(height_to_process, txHashes);

  // Seed running-tally fields from the previous block.
  // Post-HFv1: the abort guard above guarantees prevBlockData is present — use it directly.
  // At HFv1 start: no predecessor exists, use static known state at v1.
  const runningTally = prevBlockData
    ? {
        zeph_in_reserve: prevBlockData.zeph_in_reserve,
        zeph_in_reserve_atoms: prevBlockData.zeph_in_reserve_atoms,
        zsd_in_yield_reserve: prevBlockData.zsd_in_yield_reserve,
        zeph_circ: prevBlockData.zeph_circ,
        zephusd_circ: prevBlockData.zephusd_circ,
        zephrsv_circ: prevBlockData.zephrsv_circ,
        zyield_circ: prevBlockData.zyield_circ,
        assets: prevBlockData.assets,
        assets_ma: prevBlockData.assets_ma,
        liabilities: prevBlockData.liabilities,
        equity: prevBlockData.equity,
        equity_ma: prevBlockData.equity_ma,
        reserve_ratio: prevBlockData.reserve_ratio,
        reserve_ratio_ma: prevBlockData.reserve_ratio_ma,
        zsd_accrued_in_yield_reserve_from_yield_reward: prevBlockData.zsd_accrued_in_yield_reserve_from_yield_reward,
      }
    : {
        zeph_in_reserve: 0,
        zeph_in_reserve_atoms: undefined as string | undefined,
        zsd_in_yield_reserve: 0,
        zeph_circ: 1965112.77028345, // circulating supply at HF_VERSION_1_HEIGHT - 1
        zephusd_circ: 0,
        zephrsv_circ: 0,
        zyield_circ: 0,
        assets: 0,
        assets_ma: 0,
        liabilities: 0,
        equity: 0,
        equity_ma: 0,
        reserve_ratio: 0 as number | null,
        reserve_ratio_ma: 0 as number | null,
        zsd_accrued_in_yield_reserve_from_yield_reward: 0,
      };

  let blockData: ProtocolStats = {
    // Block identity + pricing (from current block's pricing record)
    block_height: height_to_process,
    block_timestamp: pr.timestamp,
    spot: pr.spot,
    moving_average: pr.moving_average,
    reserve: pr.reserve,
    reserve_ma: pr.reserve_ma,
    stable: pr.stable,
    stable_ma: pr.stable_ma,
    yield_price: pr.yield_price,
    // Running tally (carried forward from previous block)
    ...runningTally,
    // Per-block counters (reset each block)
    ...BLOCK_COUNTER_DEFAULTS,
  };

  let reserveAtoms = blockData.zeph_in_reserve_atoms
    ? BigInt(blockData.zeph_in_reserve_atoms)
    : toAtoms(blockData.zeph_in_reserve);
  blockData.zeph_in_reserve_atoms = reserveAtoms.toString();

  const prevReserveAtoms = reserveAtoms;
  let conversionReserveAtoms = 0n;

  const applyReserveDeltaAtoms = (deltaAtoms: bigint, trackConversion = false) => {
    reserveAtoms += deltaAtoms;
    if (trackConversion) {
      conversionReserveAtoms += deltaAtoms;
    }
    blockData.zeph_in_reserve = atomsToNumber(reserveAtoms);
    blockData.zeph_in_reserve_atoms = reserveAtoms.toString();
  };

  const applyReserveDelta = (delta: number) => {
    if (!Number.isFinite(delta) || delta === 0) {
      return;
    }
    applyReserveDeltaAtoms(toAtoms(delta), true);
  };

  // console.log(`pr`);
  // console.log(pr);
  // console.log(`\n\n`);
  // console.log(`bri`);
  // console.log(bri);
  // console.log(`\n\n`);

  const block_txs = txHashes;
  // console.log(`block_txs`);
  // console.log(block_txs);
  const reserveIncrementAtoms = bri.reserve_reward_atoms
    ? BigInt(bri.reserve_reward_atoms)
    : toAtoms(bri.reserve_reward || 0);
  applyReserveDeltaAtoms(reserveIncrementAtoms);
  const rewardAtoms = reserveIncrementAtoms;

  const getFromAtoms = (tx: any) => {
    if (typeof tx.from_amount_atoms === "string") {
      return BigInt(tx.from_amount_atoms);
    }
    return toAtoms(tx.from_amount || 0);
  };

  const getToAtoms = (tx: any) => {
    if (typeof tx.to_amount_atoms === "string") {
      return BigInt(tx.to_amount_atoms);
    }
    return toAtoms(tx.to_amount || 0);
  };

  const getTxFeeAtoms = (_tx: any) => 0n;

  // We need to reset circulating supply values to the audited amounts on HFv11
  if (blockData.block_height === VERSION_2_3_0_HF_V11_BLOCK_HEIGHT + 1) {
    const audited_zeph_amount = 7_828_285.273529857474;
    blockData.zeph_circ = audited_zeph_amount + UNAUDITABLE_ZEPH_MINT; // Include post-audit mint
    blockData.zephusd_circ = 370722.218621489316; // Audited amount at HFv11
    blockData.zephrsv_circ = 1023512.020210500202; // Audited amount at HFv11
    blockData.zyield_circ = 185474.354977384066; // Audited amount at HFv11
  }
  // should instead capture the total_reward! This is so that we don't have redo "saveBlockRewardInfo"
  blockData.zeph_circ +=
    (bri?.miner_reward ?? 0) + (bri?.governance_reward ?? 0) + (bri?.reserve_reward ?? 0) + (bri?.yield_reward ?? 0);

  if (block_txs.length !== 0) {
    if (logProgress) {
      console.log(
        `\tFound Conversion Transactions (${block_txs.length}) in block: ${height_to_process} - Processing...`
      );
    }
    let failureCount = 0;
    let failureTxs: string[] = [];

    for (const tx_hash of block_txs) {
      const tx = transactionsByHash.get(tx_hash);
      if (!tx) {
        failureCount++;
        failureTxs.push(tx_hash);
        continue;
      }
      try {
        switch (tx.conversion_type) {
          case "mint_stable":
            blockData.conversion_transactions_count += 1;
            // to = ZEPHUSD (ZSD)
            // from = ZEPH
            blockData.mint_stable_count += 1;
            blockData.mint_stable_volume += tx.to_amount;
            blockData.fees_zephusd += tx.conversion_fee_amount;
            blockData.zephusd_circ += tx.to_amount;
            {
              let deltaAtoms = getFromAtoms(tx);
              if (tx.tx_fee_asset === "ZEPH") {
                deltaAtoms -= getTxFeeAtoms(tx);
              }
              applyReserveDeltaAtoms(deltaAtoms, true);
            }
            break;
          case "redeem_stable":
            blockData.conversion_transactions_count += 1;
            // to = ZEPH
            // from = ZEPHUSD (ZSD)
            blockData.redeem_stable_count += 1;
            blockData.redeem_stable_volume += tx.from_amount;
            blockData.fees_zeph += tx.conversion_fee_amount;
            {
              let deltaAtoms = getToAtoms(tx);
              if (tx.tx_fee_asset === "ZEPH") {
                deltaAtoms += getTxFeeAtoms(tx);
              }
              applyReserveDeltaAtoms(-deltaAtoms, true);
            }
            // Floor guard: prevent negative circulation from redemption
            const prevZephusdCirc = blockData.zephusd_circ;
            blockData.zephusd_circ = Math.max(0, blockData.zephusd_circ - tx.from_amount);
            if (prevZephusdCirc - tx.from_amount < 0) {
              console.warn(`[aggregate] ANOMALY: zephusd_circ would go negative at height ${height_to_process}: ${prevZephusdCirc} - ${tx.from_amount} = ${prevZephusdCirc - tx.from_amount}`);
            }
            break;
          case "mint_reserve":
            blockData.conversion_transactions_count += 1;
            // to = ZEPHRSV (ZRS)
            // from = ZEPH
            blockData.mint_reserve_count += 1;
            blockData.mint_reserve_volume += tx.to_amount;
            {
              let deltaAtoms = getFromAtoms(tx);
              if (tx.tx_fee_asset === "ZEPH") {
                deltaAtoms -= getTxFeeAtoms(tx);
              }
              applyReserveDeltaAtoms(deltaAtoms, true);
            }
            blockData.zephrsv_circ += tx.to_amount;
            blockData.fees_zephrsv += tx.conversion_fee_amount;
            break;
          case "redeem_reserve":
            blockData.conversion_transactions_count += 1;
            // to = ZEPH
            // from = ZEPHRSV (ZRS)
            blockData.redeem_reserve_count += 1;
            blockData.redeem_reserve_volume += tx.from_amount;
            {
              let deltaAtoms = getToAtoms(tx);
              if (tx.tx_fee_asset === "ZEPH") {
                deltaAtoms += getTxFeeAtoms(tx);
              }
              applyReserveDeltaAtoms(-deltaAtoms, true);
            }
            // Floor guard: prevent negative circulation from redemption
            const prevZephrsvCirc = blockData.zephrsv_circ;
            blockData.zephrsv_circ = Math.max(0, blockData.zephrsv_circ - tx.from_amount);
            if (prevZephrsvCirc - tx.from_amount < 0) {
              console.warn(`[aggregate] ANOMALY: zephrsv_circ would go negative at height ${height_to_process}: ${prevZephrsvCirc} - ${tx.from_amount} = ${prevZephrsvCirc - tx.from_amount}`);
            }
            blockData.fees_zeph += tx.conversion_fee_amount;
            break;
          case "mint_yield":
            blockData.yield_conversion_transactions_count += 1;
            // to = ZYIELD (ZYS)
            // from = ZEPHUSD (ZSD)
            blockData.mint_yield_count += 1;
            blockData.mint_yield_volume += tx.to_amount;
            blockData.fees_zyield += tx.conversion_fee_amount;
            blockData.zyield_circ += tx.to_amount;
            blockData.zsd_in_yield_reserve += tx.from_amount;
            break;
          case "redeem_yield":
            blockData.yield_conversion_transactions_count += 1;
            // to = ZEPHUSD (ZSD)
            // from = ZYIELD (ZYS)
            blockData.redeem_yield_count += 1;
            blockData.redeem_yield_volume += tx.from_amount;
            blockData.fees_zephusd_yield += tx.conversion_fee_amount;
            // Floor guards: prevent negative values from yield redemption
            const prevZyieldCirc = blockData.zyield_circ;
            const prevZsdInYieldReserve = blockData.zsd_in_yield_reserve;
            blockData.zyield_circ = Math.max(0, blockData.zyield_circ - tx.from_amount);
            blockData.zsd_in_yield_reserve = Math.max(0, blockData.zsd_in_yield_reserve - tx.to_amount);
            if (prevZyieldCirc - tx.from_amount < 0) {
              console.warn(`[aggregate] ANOMALY: zyield_circ would go negative at height ${height_to_process}: ${prevZyieldCirc} - ${tx.from_amount}`);
            }
            if (prevZsdInYieldReserve - tx.to_amount < 0) {
              console.warn(`[aggregate] ANOMALY: zsd_in_yield_reserve would go negative at height ${height_to_process}: ${prevZsdInYieldReserve} - ${tx.to_amount}`);
            }
            break;
          default:
            console.log(`Unknown conversion type: ${tx.conversion_type}`);
            console.log(tx);
            break;
        }
      } catch (error) {
        console.log(`[Error] Error processing conversion ${tx_hash} in block ${height_to_process}:`, error);
        failureCount++;
        failureTxs.push(tx_hash);
      }
    }

    if (failureCount > 0) {
      console.log(
        `Failed to process ${failureCount} conversion transactions for block ${height_to_process}:`,
        failureTxs
      );
    }
  }

  // Calculate additional stats
  blockData.assets = blockData.zeph_in_reserve * blockData.spot;
  blockData.assets_ma = blockData.zeph_in_reserve * blockData.moving_average;
  blockData.liabilities = blockData.zephusd_circ;
  blockData.equity = blockData.assets - blockData.liabilities;
  blockData.equity_ma = blockData.assets_ma - blockData.liabilities;

  // Calculate reserve ratio
  blockData.reserve_ratio = blockData.liabilities > 0 ? blockData.assets / blockData.liabilities : Number.NaN;
  blockData.reserve_ratio_ma = blockData.liabilities > 0 ? blockData.assets_ma / blockData.liabilities : Number.NaN;

  // Calculate ZSD Yield Reserve Accrual and ZSD Minted this block
  if (height_to_process >= VERSION_2_HF_V6_BLOCK_HEIGHT) {
    if (blockData.reserve_ratio >= 2 && blockData.reserve_ratio_ma >= 2) {
      const yield_reward_zeph = bri.yield_reward;
      const zsd_auto_minted = convertZephToZsd(yield_reward_zeph, pr.stable, pr.stable_ma, height_to_process);
      blockData.zsd_minted_for_yield = zsd_auto_minted;
      blockData.zsd_accrued_in_yield_reserve_from_yield_reward += zsd_auto_minted;
      blockData.zsd_in_yield_reserve += zsd_auto_minted;
      //add to circ
      blockData.zephusd_circ += zsd_auto_minted;
    }
  }

  // Pre-save validation: abort on clearly invalid state to prevent corrupt data from persisting.
  // reserve_ratio/reserve_ratio_ma are excluded — NaN is legitimate when liabilities === 0.
  const criticalFields = [
    { name: 'zephusd_circ', value: blockData.zephusd_circ },
    { name: 'zephrsv_circ', value: blockData.zephrsv_circ },
    { name: 'zyield_circ', value: blockData.zyield_circ },
    { name: 'zeph_in_reserve', value: blockData.zeph_in_reserve },
    { name: 'assets', value: blockData.assets },
    { name: 'zsd_in_yield_reserve', value: blockData.zsd_in_yield_reserve },
  ];

  const validationErrors: string[] = [];
  for (const { name, value } of criticalFields) {
    if (!Number.isFinite(value)) {
      validationErrors.push(`${name} is not finite (${value})`);
    }
    if (value < 0) {
      validationErrors.push(`${name} is negative (${value})`);
    }
  }

  if (validationErrors.length > 0) {
    console.error(`[aggregate] ABORT: Invalid block data at height ${height_to_process}. Height will NOT advance.`);
    for (const err of validationErrors) {
      console.error(`  - ${err}`);
    }
    return false;
  }

  if (usePostgres()) {
    await saveBlockProtocolStats(blockData as ProtocolStats);
  }

  if (useRedis()) {
    await redis
      .pipeline()
      .hset("protocol_stats", height_to_process.toString(), JSON.stringify(blockData))
      .set("height_aggregator", height_to_process.toString())
      .exec();
  }
  await setAggregatorHeight(height_to_process);

  if (WALKTHROUGH_MODE) {
    await verifyReserveDiffs(height_to_process, {
      conversionTransactions: blockData.conversion_transactions_count,
      reserveDebug: {
        prevAtoms: prevReserveAtoms,
        rewardAtoms,
        conversionAtoms: conversionReserveAtoms,
        finalAtoms: reserveAtoms,
      },
    });
  }

  return true;
}

interface WalkthroughBlockContext {
  conversionTransactions: number;
  reserveDebug?: {
    prevAtoms: bigint;
    rewardAtoms: bigint;
    conversionAtoms: bigint;
    finalAtoms: bigint;
  };
}

async function verifyReserveDiffs(blockHeight: number, context?: WalkthroughBlockContext) {
  try {
    const diffReport = await getReserveDiffs({
      targetHeight: blockHeight,
      allowSnapshots: WALKTHROUGH_MODE,
      snapshotSource: WALKTHROUGH_SNAPSHOT_SOURCE as "redis" | "file",
    });

    if (WALKTHROUGH_MODE) {
      const logLine = {
        block_height: blockHeight,
        conversion_transactions: context?.conversionTransactions ?? 0,
        diff_report: diffReport,
      };
      console.log(`[walkthrough] ${JSON.stringify(logLine)}`);
      const reserveEntry: Record<string, unknown> = {
        block_height: blockHeight,
        conversion_transactions: context?.conversionTransactions ?? 0,
        reserve_debug: context?.reserveDebug
          ? {
            prev_atoms: context.reserveDebug.prevAtoms.toString(),
            reward_atoms: context.reserveDebug.rewardAtoms.toString(),
            conversion_atoms: context.reserveDebug.conversionAtoms.toString(),
            final_atoms: context.reserveDebug.finalAtoms.toString(),
          }
          : undefined,
      };
      const reserveDiff = diffReport.diffs.find((entry) => entry.field === "zeph_in_reserve");
      if (reserveDiff) {
        reserveEntry.zeph_in_reserve_diff = reserveDiff;
        reserveEntry.zeph_in_reserve_on_chain_atoms = toAtoms(reserveDiff.on_chain ?? 0).toString();
        reserveEntry.zeph_in_reserve_cached_atoms = toAtoms(reserveDiff.cached ?? 0).toString();
      }
      await fs.appendFile(WALKTHROUGH_RESERVE_LOG, `${JSON.stringify(reserveEntry)}\n`);
      await fs.appendFile("walkthrough_console.log", `${JSON.stringify(logLine)}\n`);
      if (!diffReport.mismatch) {
        walkthroughDiffHistory.push({
          blockHeight,
          report: diffReport,
          conversionTransactions: context?.conversionTransactions ?? 0,
        });
      } else {
        console.warn(
          `[walkthrough] reserve snapshot mismatch at block ${blockHeight} (source height: ${diffReport.source_height ?? "unknown"
          })`
        );
      }
    }

    if (diffReport.mismatch) {
      return;
    }

    let maxDiff = 0;
    if (diffReport.diffs.length > 0) {
      maxDiff = Math.max(...diffReport.diffs.map((entry) => entry.difference));
    }

    if (maxDiff > WALKTHROUGH_DIFF_THRESHOLD) {
      const message = `Walkthrough diff exceeded threshold ${WALKTHROUGH_DIFF_THRESHOLD} at block ${blockHeight}`;
      if (WALKTHROUGH_MODE) {
        console.warn(`[walkthrough] ${message}`);
      } else {
        throw new Error(message);
      }
    }
  } catch (error) {
    console.error("[walkthrough] reserve diff check failed", error);
    throw error;
  }
}

async function outputWalkthroughDiffReport() {
  if (!WALKTHROUGH_MODE) {
    return;
  }

  if (walkthroughDiffHistory.length === 0) {
    console.log("[walkthrough] No reserve diff records captured this run.");
    return;
  }

  let totalConversions = 0;
  let blocksWithConversions = 0;
  let netZephDiffAtoms = 0;
  let totalAbsZephDiffAtoms = 0;
  const perBlockSummaries: string[] = [];
  const driftWithoutConversionsRecords: { block_height: number; diff_atoms: number }[] = [];

  for (const record of walkthroughDiffHistory) {
    const { blockHeight, report, conversionTransactions } = record;
    totalConversions += conversionTransactions;
    if (conversionTransactions > 0) {
      blocksWithConversions += 1;
    }

    const zephEntry = report.diffs.find((entry) => entry.field === "zeph_in_reserve");
    const diffAtoms = zephEntry?.difference_atoms ?? 0;
    const diffValue = zephEntry?.difference ?? 0;
    netZephDiffAtoms += diffAtoms;
    totalAbsZephDiffAtoms += Math.abs(diffAtoms);

    if (conversionTransactions === 0 && Math.abs(diffAtoms) > 0) {
      driftWithoutConversionsRecords.push({ block_height: blockHeight, diff_atoms: diffAtoms });
    }

    const sourceLabel = report.source === "snapshot" ? "snapshot" : "rpc";
    const formattedDiff = diffValue ? diffValue.toFixed(12) : "0";
    perBlockSummaries.push(
      `[walkthrough] h=${blockHeight} | src=${sourceLabel} | conv=${conversionTransactions} | zeph_atoms_diff=${diffAtoms} | zeph_diff=${formattedDiff}`
    );
  }

  console.log("[walkthrough] Reserve drift report");
  perBlockSummaries.forEach((line) => console.log(line));

  const blockCount = walkthroughDiffHistory.length;
  const averageAbsZephDiffAtoms = blockCount > 0 ? totalAbsZephDiffAtoms / blockCount : 0;

  console.log(`[walkthrough] Blocks analysed: ${blockCount}`);
  console.log(
    `[walkthrough] Total conversion transactions: ${totalConversions} (across ${blocksWithConversions} blocks)`
  );
  const netZephDiff = netZephDiffAtoms / ATOMIC_UNITS_NUMBER;
  const avgAbsZephDiff = averageAbsZephDiffAtoms / ATOMIC_UNITS_NUMBER;

  console.log(`[walkthrough] Net zeph reserve drift: ${netZephDiffAtoms} atoms (${netZephDiff.toFixed(12)} ZEPH)`);
  console.log(
    `[walkthrough] Avg |zeph drift| per block: ${Math.round(averageAbsZephDiffAtoms)} atoms (${avgAbsZephDiff.toFixed(
      12
    )} ZEPH)`
  );

  if (driftWithoutConversionsRecords.length > 0) {
    const driftStrings = driftWithoutConversionsRecords.map((entry) => `${entry.block_height} (${entry.diff_atoms})`);
    console.log(`[walkthrough] Drift detected on conversion-free blocks: ${driftStrings.join(", ")}`);
  } else {
    console.log("[walkthrough] No drift detected on conversion-free blocks.");
  }

  const firstBlock = walkthroughDiffHistory[0];
  const lastBlock = walkthroughDiffHistory[walkthroughDiffHistory.length - 1];
  const generatedAt = new Date().toISOString();

  const reportPayload = {
    generated_at: generatedAt,
    block_range: {
      start: firstBlock.blockHeight,
      end: lastBlock.blockHeight,
      total: walkthroughDiffHistory.length,
    },
    summary: {
      total_conversion_transactions: totalConversions,
      blocks_with_conversions: blocksWithConversions,
      net_zeph_drift_atoms: netZephDiffAtoms,
      net_zeph_drift: netZephDiff,
      average_abs_zeph_drift_atoms: averageAbsZephDiffAtoms,
      average_abs_zeph_drift: avgAbsZephDiff,
      drift_without_conversions: driftWithoutConversionsRecords,
    },
    blocks: walkthroughDiffHistory.map(({ blockHeight, report, conversionTransactions }) => ({
      block_height: blockHeight,
      conversion_transactions: conversionTransactions,
      reserve_height: report.reserve_height,
      source: report.source,
      source_height: report.source_height,
      snapshot_path: report.snapshot_path,
      diffs: report.diffs,
    })),
  };

  try {
    const reportPath = await persistWalkthroughReport(reportPayload);
    if (reportPath) {
      console.log(`[walkthrough] Report saved to ${reportPath}`);
    }
  } catch (error) {
    console.error("[walkthrough] Failed to save walkthrough report:", error);
  }
}

async function handleReserveIntegrity(latestHeight: number) {
  if (!latestHeight || latestHeight < HF_VERSION_1_HEIGHT) {
    return;
  }

  try {
    const reserveInfo = await getReserveInfo();
    const result = reserveInfo?.result;
    if (!result || typeof result.height !== "number") {
      console.log("[reserve] Skipping snapshot – daemon reserve info missing height");
      return;
    }

    const daemonPreviousHeight = result.height - 1;
    logReserveHeights({ aggregated: latestHeight, daemon: result.height, daemonPrevious: daemonPreviousHeight });

    if (daemonPreviousHeight !== latestHeight) {
      console.log(`[reserve] Skipping snapshot – latest aggregated height does not match daemon previous height`);
      return;
    }

    if (latestHeight >= RESERVE_SNAPSHOT_START_HEIGHT) {
      const lastSnapshotHeight = await getLastReserveSnapshotPreviousHeight();
      const heightGap = lastSnapshotHeight === null ? Infinity : latestHeight - lastSnapshotHeight;
      if (!lastSnapshotHeight) {
        logReserveSnapshotStatus({ action: "initial", aggregatedHeight: latestHeight });
      } else {
        logReserveSnapshotStatus({
          action: "gap-check",
          lastSnapshotHeight,
          gap: heightGap,
          required: RESERVE_SNAPSHOT_INTERVAL,
        });
      }

      if (!lastSnapshotHeight || heightGap >= RESERVE_SNAPSHOT_INTERVAL) {
        const stored = await saveReserveSnapshotToRedis(reserveInfo);
        if (stored) {
          logReserveSnapshotStatus({ action: "store", storedPreviousHeight: stored.previous_height });
        }
      } else {
        logReserveSnapshotStatus({ action: "skip", gap: heightGap, required: RESERVE_SNAPSHOT_INTERVAL });
      }
    }

    const diffReport = await getReserveDiffs({
      targetHeight: latestHeight,
      allowSnapshots: true,
      snapshotSource: "redis",
    });
    const toleranceValue = RESERVE_DIFF_TOLERANCE;
    const passedTolerance = logReserveDiffReport(diffReport, toleranceValue);

    if (!passedTolerance || diffReport.mismatch) {
      await recordReserveMismatch(diffReport.block_height, diffReport);
      await forceReconcileReserves(diffReport, reserveInfo);
    } else {
      await clearReserveMismatch(diffReport.block_height);
    }
  } catch (error) {
    console.error("[reserve] Failed to reconcile reserve snapshot:", error);
  }
}

async function forceReconcileReserves(diffReport: ReserveDiffReport, reserveInfo: any) {
  try {
    const latestStats = await getLatestProtocolStats();
    if (!latestStats) {
      console.warn("[reserve] Cannot force reconcile – no protocol stats in cache");
      return;
    }

    const reconciledStats: ProtocolStats = { ...latestStats };
    const adjustments: Record<string, { cached: number; on_chain: number }> = {};

    diffReport.diffs.forEach((entry) => {
      const { field, on_chain, cached, difference } = entry;
      if (!Number.isFinite(difference) || Math.abs(difference ?? 0) <= RESERVE_DIFF_TOLERANCE) {
        return;
      }

      switch (field) {
        case "zeph_in_reserve":
          reconciledStats.zeph_in_reserve = on_chain;
          reconciledStats.zeph_in_reserve_atoms = toAtoms(on_chain).toString();
          adjustments[field] = { cached, on_chain };
          break;
        case "zephusd_circ":
          reconciledStats.zephusd_circ = on_chain;
          adjustments[field] = { cached, on_chain };
          break;
        case "zephrsv_circ":
          reconciledStats.zephrsv_circ = on_chain;
          adjustments[field] = { cached, on_chain };
          break;
        case "zyield_circ":
          reconciledStats.zyield_circ = on_chain;
          adjustments[field] = { cached, on_chain };
          break;
        case "zsd_in_yield_reserve":
          reconciledStats.zsd_in_yield_reserve = on_chain;
          adjustments[field] = { cached, on_chain };
          break;
        case "reserve_ratio":
          reconciledStats.reserve_ratio = on_chain;
          adjustments[field] = { cached, on_chain };
          break;
        default:
          break;
      }
    });

    if (Object.keys(adjustments).length === 0) {
      return;
    }

    await setProtocolStats(reconciledStats.block_height, reconciledStats);

    const logLine = {
      block_height: reconciledStats.block_height,
      adjustments,
      diff_report: diffReport,
      reserve_info: reserveInfo?.result ?? null,
      timestamp: new Date().toISOString(),
    };

    await fs.appendFile(RESERVE_FORCE_RECONCILE_LOG, `${JSON.stringify(logLine)}\n`);
    console.warn(
      `[reserve] Forced reconcile at ${reconciledStats.block_height}; fields adjusted: ${Object.keys(adjustments).join(", ")}`
    );
  } catch (error) {
    console.error("[reserve] Failed to force reconcile reserves:", error);
  }
}

interface WalkthroughReportPayload {
  generated_at: string;
  block_range: {
    start: number;
    end: number;
    total: number;
  };
  summary: {
    total_conversion_transactions: number;
    blocks_with_conversions: number;
    net_zeph_drift_atoms: number;
    net_zeph_drift: number;
    average_abs_zeph_drift_atoms: number;
    average_abs_zeph_drift: number;
    drift_without_conversions: { block_height: number; diff_atoms: number }[];
  };
  blocks: Array<{
    block_height: number;
    conversion_transactions: number;
    reserve_height: number;
    source: "rpc" | "snapshot";
    source_height?: number;
    snapshot_path?: string;
    diffs: ReserveDiffReport["diffs"];
  }>;
}

async function persistWalkthroughReport(report: WalkthroughReportPayload): Promise<string | null> {
  const timestamp = Date.now();
  const defaultFileName = `walkthrough-report-${report.block_range.start}-${report.block_range.end}-${timestamp}.json`;
  const resolvedPath = WALKTHROUGH_REPORT_PATH
    ? path.resolve(process.cwd(), WALKTHROUGH_REPORT_PATH)
    : path.resolve(process.cwd(), WALKTHROUGH_REPORT_DIR, defaultFileName);

  await fs.mkdir(path.dirname(resolvedPath), { recursive: true });
  await fs.writeFile(resolvedPath, JSON.stringify(report, null, 2), "utf8");
  return resolvedPath;
}

async function aggregateByTimestamp(
  startTimestamp: number,
  endingTimestamp: number,
  windowType: "hourly" | "daily" = "hourly"
) {
  console.log(`\tAggregating by timestamp: ${startTimestamp} to ${endingTimestamp} for ${windowType}`);
  const timestampWindow = windowType === "hourly" ? 3600 : 86400;
  const windowsPerChunk = windowType === "daily" ? 30 : 168; // 30 days or 7 days worth of hours
  const chunkSize = windowsPerChunk * timestampWindow;
  const diff = Math.max(endingTimestamp - startTimestamp, 0);
  const totalWindows = Math.max(1, Math.ceil(diff / timestampWindow));

  let windowIndex = 0;

  for (let chunkStart = startTimestamp; chunkStart < endingTimestamp; chunkStart += chunkSize) {
    const chunkEnd = Math.min(chunkStart + chunkSize, endingTimestamp);
    const protocolStats = await loadProtocolStatsForRange(chunkStart, chunkEnd);

    if (protocolStats.length === 0) {
      const windowsInChunk = Math.ceil((chunkEnd - chunkStart) / timestampWindow);
      windowIndex += windowsInChunk;
      console.log(`No protocol stats in chunk ${chunkStart}–${chunkEnd}, skipping ${windowsInChunk} windows`);
      continue;
    }

    const groupedStats = bucketProtocolStatsByWindow(protocolStats, chunkStart, timestampWindow);

    for (let windowStart = chunkStart; windowStart < chunkEnd; windowStart += timestampWindow) {
    const expectedEnd = windowStart + timestampWindow;
    const windowEnd = Math.min(expectedEnd, endingTimestamp);
    const windowComplete = windowEnd === expectedEnd;
    // Increment the window index
    windowIndex++;
    let aggregatedData: AggregatedData = {
      // Prices
      spot_open: 0,
      spot_close: 0,
      spot_high: 0,
      spot_low: Infinity,
      moving_average_open: 0,
      moving_average_close: 0,
      moving_average_high: 0,
      moving_average_low: Infinity,
      reserve_open: 0,
      reserve_close: 0,
      reserve_high: 0,
      reserve_low: Infinity,
      reserve_ma_open: 0,
      reserve_ma_close: 0,
      reserve_ma_high: 0,
      reserve_ma_low: Infinity,
      stable_open: 0,
      stable_close: 0,
      stable_high: 0,
      stable_low: Infinity,
      stable_ma_open: 0,
      stable_ma_close: 0,
      stable_ma_high: 0,
      stable_ma_low: Infinity,
      zyield_price_open: 0,
      zyield_price_close: 0,
      zyield_price_high: 0,
      zyield_price_low: Infinity,
      // Circulating Reserve Amounts
      // DJED Reserve
      zeph_in_reserve_open: 0,
      zeph_in_reserve_close: 0,
      zeph_in_reserve_high: 0,
      zeph_in_reserve_low: Infinity,
      // Yield Reserve
      zsd_in_yield_reserve_open: 0,
      zsd_in_yield_reserve_close: 0,
      zsd_in_yield_reserve_high: 0,
      zsd_in_yield_reserve_low: Infinity,
      // Circulating Supply
      zeph_circ_open: 0,
      zeph_circ_close: 0,
      zeph_circ_high: 0,
      zeph_circ_low: Infinity,
      zephusd_circ_open: 0,
      zephusd_circ_close: 0,
      zephusd_circ_high: 0,
      zephusd_circ_low: Infinity,
      zephrsv_circ_open: 0,
      zephrsv_circ_close: 0,
      zephrsv_circ_high: 0,
      zephrsv_circ_low: Infinity,
      zyield_circ_open: 0,
      zyield_circ_close: 0,
      zyield_circ_high: 0,
      zyield_circ_low: Infinity,
      // Djed Mechanics Stats
      assets_open: 0,
      assets_close: 0,
      assets_high: 0,
      assets_low: Infinity,
      assets_ma_open: 0,
      assets_ma_close: 0,
      assets_ma_high: 0,
      assets_ma_low: Infinity,
      liabilities_open: 0,
      liabilities_close: 0,
      liabilities_high: 0,
      liabilities_low: Infinity,
      equity_open: 0,
      equity_close: 0,
      equity_high: 0,
      equity_low: Infinity,
      equity_ma_open: 0,
      equity_ma_close: 0,
      equity_ma_high: 0,
      equity_ma_low: Infinity,
      reserve_ratio_open: 0,
      reserve_ratio_close: 0,
      reserve_ratio_high: 0,
      reserve_ratio_low: Infinity,
      reserve_ratio_ma_open: 0,
      reserve_ratio_ma_close: 0,
      reserve_ratio_ma_high: 0,
      reserve_ratio_ma_low: Infinity,
      // Conversion Stats
      conversion_transactions_count: 0,
      yield_conversion_transactions_count: 0,
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
      pending: !windowComplete,
      window_start: windowStart,
      window_end: windowEnd,
    };

    const protocolStatsWindow = groupedStats.get(windowStart) ?? [];

    console.log(
      `window: ${windowStart} => ${windowEnd} protocolStatsWindow length (relevant blocks) ${protocolStatsWindow.length} \n`
    );

    if (protocolStatsWindow.length === 0) {
      const progress = ((windowIndex / totalWindows) * 100).toFixed(2);
      console.log(
        `No relevant blocks found for window starting at ${windowStart} | window ${windowIndex} of ${totalWindows} (${progress}%)`
      );
      continue;
    }
    try {
      // console.log(protocolStatsWindow);

      // wait for 10 secs
      // await new Promise((resolve) => setTimeout(resolve, 10000));

      // sort protocolStatsWindow by timestamp
      protocolStatsWindow.sort((a, b) => a.block_timestamp - b.block_timestamp);
      // add all values to the aggregatedData

      // spot
      aggregatedData.spot_open = protocolStatsWindow[0].spot ?? 0;
      aggregatedData.spot_high = protocolStatsWindow[0].spot ?? 0;
      aggregatedData.spot_low = protocolStatsWindow[0].spot ?? 0;
      aggregatedData.spot_close = protocolStatsWindow[protocolStatsWindow.length - 1].spot ?? 0;

      // moving_average
      aggregatedData.moving_average_open = protocolStatsWindow[0].moving_average ?? 0;
      aggregatedData.moving_average_high = protocolStatsWindow[0].moving_average ?? 0;
      aggregatedData.moving_average_low = protocolStatsWindow[0].moving_average ?? 0;
      aggregatedData.moving_average_close = protocolStatsWindow[protocolStatsWindow.length - 1].moving_average ?? 0;

      // reserve (price)
      aggregatedData.reserve_open = protocolStatsWindow[0].reserve ?? 0;
      aggregatedData.reserve_high = protocolStatsWindow[0].reserve ?? 0;
      aggregatedData.reserve_low = protocolStatsWindow[0].reserve ?? 0;
      aggregatedData.reserve_close = protocolStatsWindow[protocolStatsWindow.length - 1].reserve ?? 0;

      // reserve_ma
      aggregatedData.reserve_ma_open = protocolStatsWindow[0].reserve_ma ?? 0;
      aggregatedData.reserve_ma_high = protocolStatsWindow[0].reserve_ma ?? 0;
      aggregatedData.reserve_ma_low = protocolStatsWindow[0].reserve_ma ?? 0;
      aggregatedData.reserve_ma_close = protocolStatsWindow[protocolStatsWindow.length - 1].reserve_ma ?? 0;

      // stable (price)
      aggregatedData.stable_open = protocolStatsWindow[0].stable ?? 0;
      aggregatedData.stable_high = protocolStatsWindow[0].stable ?? 0;
      aggregatedData.stable_low = protocolStatsWindow[0].stable ?? 0;
      aggregatedData.stable_close = protocolStatsWindow[protocolStatsWindow.length - 1].stable ?? 0;

      // stable_ma
      aggregatedData.stable_ma_open = protocolStatsWindow[0].stable_ma ?? 0;
      aggregatedData.stable_ma_high = protocolStatsWindow[0].stable_ma ?? 0;
      aggregatedData.stable_ma_low = protocolStatsWindow[0].stable_ma ?? 0;
      aggregatedData.stable_ma_close = protocolStatsWindow[protocolStatsWindow.length - 1].stable_ma ?? 0;

      // zyield_price
      aggregatedData.zyield_price_open = protocolStatsWindow[0].yield_price ?? 0;
      aggregatedData.zyield_price_high = protocolStatsWindow[0].yield_price ?? 0;
      aggregatedData.zyield_price_low = protocolStatsWindow[0].yield_price ?? 0;
      aggregatedData.zyield_price_close = protocolStatsWindow[protocolStatsWindow.length - 1].yield_price ?? 0;

      // zeph_in_reserve
      aggregatedData.zeph_in_reserve_open = protocolStatsWindow[0].zeph_in_reserve ?? 0;
      aggregatedData.zeph_in_reserve_high = protocolStatsWindow[0].zeph_in_reserve ?? 0;
      aggregatedData.zeph_in_reserve_low = protocolStatsWindow[0].zeph_in_reserve ?? 0;
      aggregatedData.zeph_in_reserve_close = protocolStatsWindow[protocolStatsWindow.length - 1].zeph_in_reserve ?? 0;

      // zsd_in_yield_reserve
      aggregatedData.zsd_in_yield_reserve_open = protocolStatsWindow[0].zsd_in_yield_reserve ?? 0;
      aggregatedData.zsd_in_yield_reserve_high = protocolStatsWindow[0].zsd_in_yield_reserve ?? 0;
      aggregatedData.zsd_in_yield_reserve_low = protocolStatsWindow[0].zsd_in_yield_reserve ?? 0;
      aggregatedData.zsd_in_yield_reserve_close =
        protocolStatsWindow[protocolStatsWindow.length - 1].zsd_in_yield_reserve ?? 0;

      // zeph_circ
      aggregatedData.zeph_circ_open = protocolStatsWindow[0].zeph_circ ?? 0;
      aggregatedData.zeph_circ_high = protocolStatsWindow[0].zeph_circ ?? 0;
      aggregatedData.zeph_circ_low = protocolStatsWindow[0].zeph_circ ?? 0;
      aggregatedData.zeph_circ_close = protocolStatsWindow[protocolStatsWindow.length - 1].zeph_circ ?? 0;

      // zephusd_circ
      aggregatedData.zephusd_circ_open = protocolStatsWindow[0].zephusd_circ ?? 0;
      aggregatedData.zephusd_circ_high = protocolStatsWindow[0].zephusd_circ ?? 0;
      aggregatedData.zephusd_circ_low = protocolStatsWindow[0].zephusd_circ ?? 0;
      aggregatedData.zephusd_circ_close = protocolStatsWindow[protocolStatsWindow.length - 1].zephusd_circ ?? 0;

      // zephrsv_circ
      aggregatedData.zephrsv_circ_open = protocolStatsWindow[0].zephrsv_circ ?? 0;
      aggregatedData.zephrsv_circ_high = protocolStatsWindow[0].zephrsv_circ ?? 0;
      aggregatedData.zephrsv_circ_low = protocolStatsWindow[0].zephrsv_circ ?? 0;
      aggregatedData.zephrsv_circ_close = protocolStatsWindow[protocolStatsWindow.length - 1].zephrsv_circ ?? 0;

      // zyield_circ
      aggregatedData.zyield_circ_open = protocolStatsWindow[0].zyield_circ ?? 0;
      aggregatedData.zyield_circ_high = protocolStatsWindow[0].zyield_circ ?? 0;
      aggregatedData.zyield_circ_low = protocolStatsWindow[0].zyield_circ ?? 0;
      aggregatedData.zyield_circ_close = protocolStatsWindow[protocolStatsWindow.length - 1].zyield_circ ?? 0;

      // assets
      aggregatedData.assets_open = protocolStatsWindow[0].assets ?? 0;
      aggregatedData.assets_high = protocolStatsWindow[0].assets ?? 0;
      aggregatedData.assets_low = protocolStatsWindow[0].assets ?? 0;
      aggregatedData.assets_close = protocolStatsWindow[protocolStatsWindow.length - 1].assets ?? 0;

      // assets_ma
      aggregatedData.assets_ma_open = protocolStatsWindow[0].assets_ma ?? 0;
      aggregatedData.assets_ma_high = protocolStatsWindow[0].assets_ma ?? 0;
      aggregatedData.assets_ma_low = protocolStatsWindow[0].assets_ma ?? 0;
      aggregatedData.assets_ma_close = protocolStatsWindow[protocolStatsWindow.length - 1].assets_ma ?? 0;

      // liabilities
      aggregatedData.liabilities_open = protocolStatsWindow[0].liabilities ?? 0;
      aggregatedData.liabilities_high = protocolStatsWindow[0].liabilities ?? 0;
      aggregatedData.liabilities_low = protocolStatsWindow[0].liabilities ?? 0;
      aggregatedData.liabilities_close = protocolStatsWindow[protocolStatsWindow.length - 1].liabilities ?? 0;

      // equity
      aggregatedData.equity_open = protocolStatsWindow[0].equity ?? 0;
      aggregatedData.equity_high = protocolStatsWindow[0].equity ?? 0;
      aggregatedData.equity_low = protocolStatsWindow[0].equity ?? 0;
      aggregatedData.equity_close = protocolStatsWindow[protocolStatsWindow.length - 1].equity ?? 0;

      // equity_ma
      aggregatedData.equity_ma_open = protocolStatsWindow[0].equity_ma ?? 0;
      aggregatedData.equity_ma_high = protocolStatsWindow[0].equity_ma ?? 0;
      aggregatedData.equity_ma_low = protocolStatsWindow[0].equity_ma ?? 0;
      aggregatedData.equity_ma_close = protocolStatsWindow[protocolStatsWindow.length - 1].equity_ma ?? 0;

      // reserve_ratio
      aggregatedData.reserve_ratio_open = protocolStatsWindow[0].reserve_ratio ?? 0;
      aggregatedData.reserve_ratio_high = protocolStatsWindow[0].reserve_ratio ?? 0;
      aggregatedData.reserve_ratio_low = protocolStatsWindow[0].reserve_ratio ?? 0;
      aggregatedData.reserve_ratio_close = protocolStatsWindow[protocolStatsWindow.length - 1].reserve_ratio ?? 0;

      // reserve_ratio_ma
      aggregatedData.reserve_ratio_ma_open = protocolStatsWindow[0].reserve_ratio_ma ?? 0;
      aggregatedData.reserve_ratio_ma_high = protocolStatsWindow[0].reserve_ratio_ma ?? 0;
      aggregatedData.reserve_ratio_ma_low = protocolStatsWindow[0].reserve_ratio_ma ?? 0;
      aggregatedData.reserve_ratio_ma_close = protocolStatsWindow[protocolStatsWindow.length - 1].reserve_ratio_ma ?? 0;

      protocolStatsWindow.forEach((blockData) => {
        // high and low
        aggregatedData.spot_high = Math.max(aggregatedData.spot_high, blockData.spot);
        aggregatedData.spot_low = Math.min(aggregatedData.spot_low, blockData.spot);

        aggregatedData.moving_average_high = Math.max(aggregatedData.moving_average_high, blockData.moving_average);
        aggregatedData.moving_average_low = Math.min(aggregatedData.moving_average_low, blockData.moving_average);

        aggregatedData.reserve_high = Math.max(aggregatedData.reserve_high, blockData.reserve);
        aggregatedData.reserve_low = Math.min(aggregatedData.reserve_low, blockData.reserve);

        aggregatedData.reserve_ma_high = Math.max(aggregatedData.reserve_ma_high, blockData.reserve_ma);
        aggregatedData.reserve_ma_low = Math.min(aggregatedData.reserve_ma_low, blockData.reserve_ma);

        aggregatedData.stable_high = Math.max(aggregatedData.stable_high, blockData.stable);
        aggregatedData.stable_low = Math.min(aggregatedData.stable_low, blockData.stable);

        aggregatedData.stable_ma_high = Math.max(aggregatedData.stable_ma_high, blockData.stable_ma);
        aggregatedData.stable_ma_low = Math.min(aggregatedData.stable_ma_low, blockData.stable_ma);

        aggregatedData.zyield_price_high = Math.max(aggregatedData.zyield_price_high, blockData.yield_price);
        aggregatedData.zyield_price_low = Math.min(aggregatedData.zyield_price_low, blockData.yield_price);

        aggregatedData.zeph_in_reserve_high = Math.max(aggregatedData.zeph_in_reserve_high, blockData.zeph_in_reserve);
        aggregatedData.zeph_in_reserve_low = Math.min(aggregatedData.zeph_in_reserve_low, blockData.zeph_in_reserve);

        aggregatedData.zsd_in_yield_reserve_high = Math.max(
          aggregatedData.zsd_in_yield_reserve_high,
          blockData.zsd_in_yield_reserve
        );
        aggregatedData.zsd_in_yield_reserve_low = Math.min(
          aggregatedData.zsd_in_yield_reserve_low,
          blockData.zsd_in_yield_reserve
        );

        aggregatedData.zeph_circ_high = Math.max(aggregatedData.zeph_circ_high, blockData.zeph_circ);
        aggregatedData.zeph_circ_low = Math.min(aggregatedData.zeph_circ_low, blockData.zeph_circ);

        aggregatedData.zephusd_circ_high = Math.max(aggregatedData.zephusd_circ_high, blockData.zephusd_circ);
        aggregatedData.zephusd_circ_low = Math.min(aggregatedData.zephusd_circ_low, blockData.zephusd_circ);

        aggregatedData.zephrsv_circ_high = Math.max(aggregatedData.zephrsv_circ_high, blockData.zephrsv_circ);
        aggregatedData.zephrsv_circ_low = Math.min(aggregatedData.zephrsv_circ_low, blockData.zephrsv_circ);

        aggregatedData.zyield_circ_high = Math.max(aggregatedData.zyield_circ_high, blockData.zyield_circ);
        aggregatedData.zyield_circ_low = Math.min(aggregatedData.zyield_circ_low, blockData.zyield_circ);

        aggregatedData.assets_high = Math.max(aggregatedData.assets_high, blockData.assets);
        aggregatedData.assets_low = Math.min(aggregatedData.assets_low, blockData.assets);

        aggregatedData.assets_ma_high = Math.max(aggregatedData.assets_ma_high, blockData.assets_ma);
        aggregatedData.assets_ma_low = Math.min(aggregatedData.assets_ma_low, blockData.assets_ma);

        aggregatedData.liabilities_high = Math.max(aggregatedData.liabilities_high, blockData.liabilities);
        aggregatedData.liabilities_low = Math.min(aggregatedData.liabilities_low, blockData.liabilities);

        aggregatedData.equity_high = Math.max(aggregatedData.equity_high, blockData.equity);
        aggregatedData.equity_low = Math.min(aggregatedData.equity_low, blockData.equity);

        aggregatedData.equity_ma_high = Math.max(aggregatedData.equity_ma_high, blockData.equity_ma);
        aggregatedData.equity_ma_low = Math.min(aggregatedData.equity_ma_low, blockData.equity_ma);

        const reserveRatio = blockData.reserve_ratio ?? 0;
        aggregatedData.reserve_ratio_high = Math.max(aggregatedData.reserve_ratio_high, reserveRatio);
        aggregatedData.reserve_ratio_low = Math.min(aggregatedData.reserve_ratio_low, reserveRatio);

        const reserveRatioMa = blockData.reserve_ratio_ma ?? 0;
        aggregatedData.reserve_ratio_ma_high = Math.max(aggregatedData.reserve_ratio_ma_high, reserveRatioMa);
        aggregatedData.reserve_ratio_ma_low = Math.min(aggregatedData.reserve_ratio_ma_low, reserveRatioMa);

        // counters
        aggregatedData.conversion_transactions_count += blockData.conversion_transactions_count;
        aggregatedData.yield_conversion_transactions_count += blockData.yield_conversion_transactions_count;
        aggregatedData.mint_reserve_count += blockData.mint_reserve_count;
        aggregatedData.mint_reserve_volume += blockData.mint_reserve_volume;
        aggregatedData.fees_zephrsv += blockData.fees_zephrsv;
        aggregatedData.redeem_reserve_count += blockData.redeem_reserve_count;
        aggregatedData.redeem_reserve_volume += blockData.redeem_reserve_volume;
        aggregatedData.fees_zephusd += blockData.fees_zephusd;
        aggregatedData.mint_stable_count += blockData.mint_stable_count;
        aggregatedData.mint_stable_volume += blockData.mint_stable_volume;
        aggregatedData.redeem_stable_count += blockData.redeem_stable_count;
        aggregatedData.redeem_stable_volume += blockData.redeem_stable_volume;
        aggregatedData.fees_zeph += blockData.fees_zeph;
        aggregatedData.mint_yield_count += blockData.mint_yield_count;
        aggregatedData.mint_yield_volume += blockData.mint_yield_volume;
        aggregatedData.fees_zyield += blockData.fees_zyield;
        aggregatedData.redeem_yield_count += blockData.redeem_yield_count;
        aggregatedData.redeem_yield_volume += blockData.redeem_yield_volume;
        aggregatedData.fees_zephusd_yield += blockData.fees_zephusd_yield;
      });

      // Store the aggregated data for the hour
      const zsetKey = windowType === "hourly" ? "protocol_stats_hourly" : "protocol_stats_daily";
      const pendingKey = windowType === "hourly" ? HOURLY_PENDING_KEY : DAILY_PENDING_KEY;

      if (usePostgres()) {
        await saveAggregatedProtocolStats(
          windowType === "hourly" ? "hour" : "day",
          windowStart,
          windowEnd,
          aggregatedData,
          !windowComplete
        );
      }

      if (windowComplete) {
        if (useRedis()) {
          const pipeline = redis.pipeline();
          pipeline.zremrangebyscore(zsetKey, windowStart, windowStart);
          pipeline.zadd(zsetKey, windowStart, JSON.stringify(aggregatedData));
          pipeline.del(pendingKey);
          if (windowType === "hourly") {
            pipeline.set("timestamp_aggregator_hourly", windowEnd);
          } else {
            pipeline.set("timestamp_aggregator_daily", windowEnd);
          }
          await pipeline.exec();
        }

        if (windowEnd) {
          if (windowType === "hourly") {
            await setHourlyTimestamp(windowEnd);
            console.log(`Hourly stats aggregated for window starting at ${windowStart}`);
          } else {
            await setDailyTimestamp(windowEnd);
            console.log(`Daily stats aggregated for window starting at ${windowStart}`);
          }
        }
      } else if (useRedis()) {
        await redis.set(pendingKey, JSON.stringify(aggregatedData));
        console.log(`[aggregation-${windowType}] pending window updated at ${windowStart}`);
      }

      logAggregatedSummary(windowType, windowStart, aggregatedData);

      //show some progress
      // Calculate and log the progress
      const progress = ((windowIndex / totalWindows) * 100).toFixed(2);
      console.log(`\tProcessing window ${windowIndex} of ${totalWindows} (${progress}%)`);
    } catch (error) {
      console.error("Error aggregating by timestamp:", error);
      console.log(`\n\nprotocolStatsWindow:`);
      protocolStatsWindow[0];
    }
  }
  }
}
