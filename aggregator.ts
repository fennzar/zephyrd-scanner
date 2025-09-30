// Take all data and aggregate into a single redis key done by block, hourly and daily.

import fs from "node:fs/promises";
import path from "node:path";

import redis from "./redis";
import {
  AggregatedData,
  ProtocolStats,
  getRedisHeight,
  getRedisPricingRecord,
  getRedisTimestampDaily,
  getRedisTimestampHourly,
  getReserveDiffs,
  getReserveInfo,
  getLastReserveSnapshotPreviousHeight,
  recordReserveMismatch,
  clearReserveMismatch,
  saveReserveSnapshotToRedis,
  getCurrentBlockHeight,
  getLatestProtocolStats,
  setProtocolStats,
  ReserveDiffReport,
  RESERVE_SNAPSHOT_INTERVAL_BLOCKS,
  RESERVE_SNAPSHOT_START_HEIGHT,
  WALKTHROUGH_SNAPSHOT_SOURCE,
  RESERVE_DIFF_TOLERANCE,
} from "./utils";
// const DEATOMIZE = 10 ** -12;
const HF_VERSION_1_HEIGHT = 89300;
const HF_VERSION_1_TIMESTAMP = 1696152427;

const ARTEMIS_HF_V5_BLOCK_HEIGHT = 295000;

const VERSION_2_HF_V6_BLOCK_HEIGHT = 360000;
const VERSION_2_HF_V6_TIMESTAMP = 1728817200; // ESTIMATED. TO BE UPDATED?
const VERSION_2_3_0_HF_V11_BLOCK_HEIGHT = 536000; // Post Audit, asset type changes.

const RESERVE_SNAPSHOT_INTERVAL = RESERVE_SNAPSHOT_INTERVAL_BLOCKS;
const TEMP_HOURLY_MAX_LAG = 30;
const TEMP_DAILY_MAX_LAG = 720;

const ATOMIC_UNITS = 1_000_000_000_000n; // 1 ZEPH/ZSD in atomic units
const ATOMIC_UNITS_NUMBER = Number(ATOMIC_UNITS);

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
  to_asset: string;
  to_amount: number;
  conversion_fee_asset: string;
  conversion_fee_amount: number;
  tx_fee_asset: string;
  tx_fee_amount: number;
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
  // hangover fix from old implementation
  const txsByBlockExists = await redis.exists("txs_by_block");
  if (!txsByBlockExists) {
    console.log("No txs by block found, populating...");
    await populateTxsByBlock();
  }

  console.log(`Starting aggregation...`);

  const current_height_prs = Number(await redis.get("height_prs"));

  if (!current_height_prs) {
    console.log("No current height found for pricing records");
    return;
  }

  // by block
  const height_by_block = await getRedisHeight(); // where we are at in the data aggregation
  const height_to_process = Math.max(height_by_block + 1, HF_VERSION_1_HEIGHT); // only process from HF_VERSION_1_HEIGHT

  console.log(`\tAggregating from block: ${height_to_process} to ${current_height_prs}`);

  const lastBlockToProcess = current_height_prs;
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

    for (let i = height_to_process; i <= lastBlockToProcess; i++) {
      const shouldLog = i === lastBlockToProcess || (i - height_to_process) % progressInterval === 0;
      await aggregateBlock(i, shouldLog);
    }
  }

  // get pr for current_height_prs
  const current_pr = await getRedisPricingRecord(current_height_prs);
  const timestamp_hourly = await getRedisTimestampHourly();
  const timestamp_daily = await getRedisTimestampDaily();

  const priorAggregatedHeight = await getRedisHeight();
  let allowHourlyAggregation = true;
  let allowDailyAggregation = true;

  if (priorAggregatedHeight && priorAggregatedHeight > 0) {
    try {
      const daemonHeight = await getCurrentBlockHeight();
      if (daemonHeight && daemonHeight > priorAggregatedHeight) {
        const lag = daemonHeight - priorAggregatedHeight;
        if (lag > TEMP_HOURLY_MAX_LAG) {
          allowHourlyAggregation = false;
          console.log(`Skipping hourly aggregation – daemon ahead by ${lag} blocks (threshold ${TEMP_HOURLY_MAX_LAG})`);
        }
        if (lag > TEMP_DAILY_MAX_LAG) {
          allowDailyAggregation = false;
          console.log(`Skipping daily aggregation – daemon ahead by ${lag} blocks (threshold ${TEMP_DAILY_MAX_LAG})`);
        }
      }
    } catch (error) {
      console.error("Failed to determine daemon height for temp aggregation check:", error);
    }
  }

  if (allowHourlyAggregation) {
    await aggregateByTimestamp(Math.max(timestamp_hourly, HF_VERSION_1_TIMESTAMP), current_pr.timestamp, "hourly");
  }
  if (allowDailyAggregation) {
    await aggregateByTimestamp(Math.max(timestamp_daily, HF_VERSION_1_TIMESTAMP), current_pr.timestamp, "daily");
  }

  if (WALKTHROUGH_MODE) {
    await outputWalkthroughDiffReport();
  }

  const latestAggregatedHeight = await getRedisHeight();
  await handleReserveIntegrity(latestAggregatedHeight);

  console.log(`Finished aggregation`);
}

async function loadBlockInputs(height: number) {
  const pipelineResults = await redis
    .pipeline()
    .hget("pricing_records", height.toString())
    .hget("block_rewards", height.toString())
    .hget("protocol_stats", (height - 1).toString())
    .hget("txs_by_block", height.toString())
    .exec();

  if (!pipelineResults) {
    throw new Error(`Failed to load Redis inputs for block ${height}`);
  }

  const [pricingRecordResult, blockRewardResult, prevStatsResult, txHashesResult] = pipelineResults;

  const prJson = pricingRecordResult?.[1] as string | null;
  const briJson = blockRewardResult?.[1] as string | null;
  const prevBlockDataJson = prevStatsResult?.[1] as string | null;
  const txHashesJson = txHashesResult?.[1] as string | null;

  let pr: PricingRecord | null = null;
  let bri: BlockRewardInfo | null = null;
  let prevBlockData: Partial<ProtocolStats> = {};
  let txHashes: string[] = [];

  try {
    pr = prJson ? (JSON.parse(prJson) as PricingRecord) : null;
  } catch (error) {
    console.error(`Error parsing pricing record for block ${height}:`, error);
  }

  try {
    bri = briJson ? (JSON.parse(briJson) as BlockRewardInfo) : null;
  } catch (error) {
    console.error(`Error parsing block reward info for block ${height}:`, error);
  }

  try {
    prevBlockData = prevBlockDataJson ? JSON.parse(prevBlockDataJson) : {};
  } catch (error) {
    console.error(`Error parsing previous block stats for block ${height}:`, error);
    prevBlockData = {};
  }

  try {
    txHashes = txHashesJson ? JSON.parse(txHashesJson) : [];
  } catch (error) {
    console.error(`Error parsing transaction hashes for block ${height}:`, error);
    txHashes = [];
  }

  return { pr, bri, prevBlockData, txHashes };
}

async function fetchTransactions(hashes: string[]): Promise<Map<string, Transaction>> {
  const txMap = new Map<string, Transaction>();
  if (hashes.length === 0) {
    return txMap;
  }

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

async function aggregateBlock(height_to_process: number, logProgress = false) {
  if (logProgress) {
    console.log(`\tAggregating block: ${height_to_process}`);
  }

  const { pr, bri, prevBlockData, txHashes } = await loadBlockInputs(height_to_process);

  if (!pr) {
    console.log("No pricing record found for height: ", height_to_process);
    return;
  }
  if (!bri) {
    console.log("No block reward info found for height: ", height_to_process);
    return;
  }

  const transactionsByHash = await fetchTransactions(txHashes);

  // initialize the block data
  let blockData: ProtocolStats = {
    block_height: height_to_process,
    block_timestamp: pr.timestamp, // Get timestamp from pricing record
    spot: pr.spot, // Get spot from pricing record
    moving_average: pr.moving_average, // Get moving average from pricing record
    reserve: pr.reserve, // Get reserve from pricing record
    reserve_ma: pr.reserve_ma, // Get reserve moving average from pricing record
    stable: pr.stable, // Get stable from pricing record
    stable_ma: pr.stable_ma, // Get stable moving average from pricing record
    yield_price: pr.yield_price, // Get yield price from pricing record
    zeph_in_reserve: prevBlockData.zeph_in_reserve || 0, // Initialize from previous block or 0
    zeph_in_reserve_atoms: prevBlockData.zeph_in_reserve_atoms,
    zsd_in_yield_reserve: prevBlockData.zsd_in_yield_reserve || 0, // Initialize from previous block or 0
    zeph_circ: prevBlockData.zeph_circ || 1965112.77028345, // Initialize from previous block or circulating supply at HF_VERSION_1_HEIGHT - 1
    zephusd_circ: prevBlockData.zephusd_circ || 0, // Initialize from previous block or 0
    zephrsv_circ: prevBlockData.zephrsv_circ || 0, // Initialize from previous block or 0
    zyield_circ: prevBlockData.zyield_circ || 0, // Initialize from previous block or 0
    assets: prevBlockData.assets || 0, // Initialize from previous block or 0
    assets_ma: prevBlockData.assets_ma || 0, // Initialize from previous block or 0
    liabilities: prevBlockData.liabilities || 0, // Initialize from previous block or 0
    equity: prevBlockData.equity || 0, // Initialize from previous block or 0
    equity_ma: prevBlockData.equity_ma || 0, // Initialize from previous block or 0
    reserve_ratio: prevBlockData.reserve_ratio || 0, // Initialize from previous block or 0
    reserve_ratio_ma: prevBlockData.reserve_ratio_ma || 0, // Initialize from previous block or 0
    zsd_accrued_in_yield_reserve_from_yield_reward: prevBlockData.zsd_accrued_in_yield_reserve_from_yield_reward || 0, // Initialize from previous block or 0
    zsd_minted_for_yield: 0,
    conversion_transactions_count: 0,
    yield_conversion_transactions_count: 0,
    mint_reserve_count: 0,
    mint_reserve_volume: 0,
    fees_zephrsv: 0, // conversion fees from minting zeph -> zrs
    redeem_reserve_count: 0,
    redeem_reserve_volume: 0,
    fees_zephusd: 0, // conversion fees from minting zeph -> zsd
    mint_stable_count: 0,
    mint_stable_volume: 0,
    redeem_stable_count: 0,
    redeem_stable_volume: 0,
    fees_zeph: 0, // conversion fees from redeeming zsd -> zeph && redeeming zrs -> zeph
    mint_yield_count: 0,
    mint_yield_volume: 0,
    redeem_yield_count: 0,
    redeem_yield_volume: 0,
    fees_zephusd_yield: 0, // conversion fees from redeeming zys -> zsd
    fees_zyield: 0, // conversion fees from minting zsd -> zys
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
    const audited_zeph_amount = 7828285.273529857474;
    blockData.zeph_circ = audited_zeph_amount; // Audited amount at HFv11
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
            blockData.zephusd_circ -= tx.from_amount;
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
            blockData.zephrsv_circ -= tx.from_amount;
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
            blockData.zyield_circ -= tx.from_amount;
            blockData.zsd_in_yield_reserve -= tx.to_amount;
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

  await redis
    .pipeline()
    .hset("protocol_stats", height_to_process.toString(), JSON.stringify(blockData))
    .set("height_aggregator", height_to_process.toString())
    .exec();

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
          `[walkthrough] reserve snapshot mismatch at block ${blockHeight} (source height: ${
            diffReport.source_height ?? "unknown"
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
    console.log(
      `[reserve] Heights | aggregated=${latestHeight} | daemon_height=${result.height} | daemon_previous=${daemonPreviousHeight}`
    );

    if (daemonPreviousHeight !== latestHeight) {
      console.log(`[reserve] Skipping snapshot – latest aggregated height does not match daemon previous height`);
      return;
    }

    if (latestHeight >= RESERVE_SNAPSHOT_START_HEIGHT) {
      const lastSnapshotHeight = await getLastReserveSnapshotPreviousHeight();
      const heightGap = lastSnapshotHeight === null ? Infinity : latestHeight - lastSnapshotHeight;
      if (!lastSnapshotHeight) {
        console.log(`[reserve] No prior snapshot – capturing for height ${latestHeight}`);
      } else {
        console.log(
          `[reserve] Snapshot gap check | last=${lastSnapshotHeight} | gap=${heightGap} | required=${RESERVE_SNAPSHOT_INTERVAL}`
        );
      }

      if (!lastSnapshotHeight || heightGap >= RESERVE_SNAPSHOT_INTERVAL) {
        const stored = await saveReserveSnapshotToRedis(reserveInfo);
        if (stored) {
          console.log(`[reserve] Snapshot stored for previous height ${stored.previous_height}`);
        }
      } else {
        console.log(`[reserve] Skipping snapshot – gap ${heightGap} < interval ${RESERVE_SNAPSHOT_INTERVAL}`);
      }
    }

    const diffReport = await getReserveDiffs({
      targetHeight: latestHeight,
      allowSnapshots: true,
      snapshotSource: "redis",
    });
    console.log(
      `[reserve] Diff source for ${diffReport.block_height}: ${diffReport.source} (reserve_height=${diffReport.reserve_height})`
    );
    diffReport.diffs.forEach((entry) => {
      console.log(
        `[reserve] ${diffReport.block_height} | ${entry.field} | on_chain=${entry.on_chain} | cached=${
          entry.cached
        } | diff=${entry.difference} | diff_atoms=${entry.difference_atoms ?? 0}`
      );
    });
    const zephEntry = diffReport.diffs.find((entry) => entry.field === "zeph_in_reserve");
    const diffValue = Math.abs(zephEntry?.difference ?? 0);
    const toleranceValue = RESERVE_DIFF_TOLERANCE;

    const passedTolerance = diffValue <= toleranceValue;
    if (passedTolerance) {
      console.log(`[reserve] Diff check PASS at ${diffReport.block_height} (|diff|=${diffValue} <= ${toleranceValue})`);
    } else {
      console.warn(`[reserve] Diff check FAIL at ${diffReport.block_height} (|diff|=${diffValue} > ${toleranceValue})`);
    }

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
  const diff = Math.max(endingTimestamp - startTimestamp, 0);
  const totalWindows = Math.max(1, Math.ceil(diff / timestampWindow));
  // get all protocol stats between start and end timestamp
  // aggregate into a single key "protocol_stats_hourly" as a sorted set
  // store in redis

  const protocolStats = await redis.hgetall("protocol_stats");

  if (!protocolStats) {
    console.log("No protocol stats available");
    return;
  }

  let windowIndex = 0; // Track the current window index

  // Loop through the time range in increments, allowing partial windows
  for (let windowStart = startTimestamp; windowStart < endingTimestamp; windowStart += timestampWindow) {
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

    let protocolStatsWindow = [];

    // Loop through each block's data
    for (const [height, blockDataJson] of Object.entries(protocolStats)) {
      const blockData = JSON.parse(blockDataJson);
      const blockTimestamp = blockData.block_timestamp;

      // Check if the block's timestamp is within the specified time window
      if (blockTimestamp >= windowStart && blockTimestamp < windowEnd) {
        protocolStatsWindow.push(blockData);

        // console.log(`blockData`);
        // console.log(blockData);

        // console.log(`\n\n`);
        // console.log(`we are adding this in`);
        // console.log(`startTimestamp: ${windowStart}`);
        // console.log(`!!blockTimestamp: ${blockTimestamp}`);
        // console.log(`endingTimestamp: ${windowEnd}`);

        // await new Promise((resolve) => setTimeout(resolve, 10000));
      }
    }

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

        aggregatedData.reserve_ratio_high = Math.max(aggregatedData.reserve_ratio_high, blockData.reserve_ratio);
        aggregatedData.reserve_ratio_low = Math.min(aggregatedData.reserve_ratio_low, blockData.reserve_ratio);

        aggregatedData.reserve_ratio_ma_high = Math.max(
          aggregatedData.reserve_ratio_ma_high,
          blockData.reserve_ratio_ma
        );
        aggregatedData.reserve_ratio_ma_low = Math.min(aggregatedData.reserve_ratio_ma_low, blockData.reserve_ratio_ma);

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
      if (windowType === "hourly") {
        await redis.zremrangebyscore("protocol_stats_hourly", windowStart, windowStart);
        await redis.zadd("protocol_stats_hourly", windowStart, JSON.stringify(aggregatedData));
        console.log(`Hourly stats aggregated for window starting at ${windowStart}`);
        if (windowComplete) {
          await redis.set("timestamp_aggregator_hourly", windowEnd);
        }
      } else if (windowType === "daily") {
        await redis.zremrangebyscore("protocol_stats_daily", windowStart, windowStart);
        await redis.zadd("protocol_stats_daily", windowStart, JSON.stringify(aggregatedData));
        console.log(`Daily stats aggregated for window starting at ${windowStart}`);
        if (windowComplete) {
          await redis.set("timestamp_aggregator_daily", windowEnd);
        }
      }

      console.log(aggregatedData);
      console.log(`\n\n`);

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
