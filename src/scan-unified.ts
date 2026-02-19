/**
 * Unified scan: combines pricing record extraction and transaction processing
 * into a single pass over blocks, eliminating duplicate getBlock() RPC calls.
 *
 * Opt-in via UNIFIED_SCAN=true env var.
 */
import fs from "node:fs/promises";
import { Pipeline } from "ioredis";

import { getCurrentBlockHeight, getBlock, readTxBatch } from "./utils";
import { fetchConcurrent, RPC_CHUNK_SIZE, RPC_CONCURRENCY } from "./rpc-pool";
import redis from "./redis";
import { usePostgres, useRedis, getStartBlock, getEndBlock } from "./config";
import { stores } from "./storage/factory";
import { upsertBlockRewardBatch, type BlockRewardRecord } from "./db/blockRewards";
import {
  insertTransactions,
  ConversionTransactionRecord,
  deleteAllTransactions,
} from "./db/transactions";
import {
  defaultTotals,
  getTotals as getTotalsRow,
  incrementTotals,
  setTotals,
} from "./db/totals";
import type { PricingRecordInput } from "./storage/types";
import { HF_V1_BLOCK_HEIGHT } from "./constants";

const DEATOMIZE = 10 ** -12;
const ARTEMIS_HF_V5_BLOCK_HEIGHT = 295000;
const VERSION_2_HF_V6_BLOCK_HEIGHT = 360000;
const ATOMIC_UNITS = 1_000_000_000_000n;
const ATOMIC_UNITS_NUMBER = Number(ATOMIC_UNITS);
const WALKTHROUGH_DEBUG_LOG =
  process.env.WALKTHROUGH_DEBUG_LOG ?? "walkthrough_debug.log";
const BLOCK_REWARD_BATCH_SIZE = Number(
  process.env.BLOCK_REWARD_BATCH_SIZE ?? "500"
);
const PRICING_BATCH_SIZE = Number(process.env.PRICING_BATCH_SIZE ?? "500");
// ─── IncrTotals & helpers (duplicated from tx.ts to avoid circular imports) ───

interface IncrTotals {
  miner_reward: number;
  governance_reward: number;
  reserve_reward: number;
  yield_reward: number;
  conversion_transactions: number;
  mint_stable_count: number;
  mint_stable_volume: number;
  fees_zephusd: number;
  redeem_stable_count: number;
  redeem_stable_volume: number;
  fees_zeph: number;
  mint_reserve_count: number;
  mint_reserve_volume: number;
  fees_zephrsv: number;
  redeem_reserve_count: number;
  redeem_reserve_volume: number;
  mint_yield_count: number;
  mint_yield_volume: number;
  fees_zyield: number;
  redeem_yield_count: number;
  redeem_yield_volume: number;
  fees_zephusd_yield: number;
}

interface TxInfoType {
  hash: string;
  block_height: any;
  block_timestamp: any;
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
  tx_fee_asset: string;
  tx_fee_amount: number;
  tx_fee_atoms?: string;
}

function blankIncrTotals(): IncrTotals {
  return {
    miner_reward: 0,
    governance_reward: 0,
    reserve_reward: 0,
    yield_reward: 0,
    conversion_transactions: 0,
    mint_stable_count: 0,
    mint_stable_volume: 0,
    fees_zephusd: 0,
    redeem_stable_count: 0,
    redeem_stable_volume: 0,
    fees_zeph: 0,
    mint_reserve_count: 0,
    mint_reserve_volume: 0,
    fees_zephrsv: 0,
    redeem_reserve_count: 0,
    redeem_reserve_volume: 0,
    mint_yield_count: 0,
    mint_yield_volume: 0,
    fees_zyield: 0,
    redeem_yield_count: 0,
    redeem_yield_volume: 0,
    fees_zephusd_yield: 0,
  };
}

function mapTotalsDelta(delta: IncrTotals) {
  return {
    minerReward: delta.miner_reward,
    governanceReward: delta.governance_reward,
    reserveReward: delta.reserve_reward,
    yieldReward: delta.yield_reward,
    conversionTransactions: delta.conversion_transactions,
    mintStableCount: delta.mint_stable_count,
    mintStableVolume: delta.mint_stable_volume,
    feesZephusd: delta.fees_zephusd,
    redeemStableCount: delta.redeem_stable_count,
    redeemStableVolume: delta.redeem_stable_volume,
    feesZeph: delta.fees_zeph,
    mintReserveCount: delta.mint_reserve_count,
    mintReserveVolume: delta.mint_reserve_volume,
    feesZephrsv: delta.fees_zephrsv,
    redeemReserveCount: delta.redeem_reserve_count,
    redeemReserveVolume: delta.redeem_reserve_volume,
    mintYieldCount: delta.mint_yield_count,
    mintYieldVolume: delta.mint_yield_volume,
    feesZyield: delta.fees_zyield,
    redeemYieldCount: delta.redeem_yield_count,
    redeemYieldVolume: delta.redeem_yield_volume,
    feesZephusdYield: delta.fees_zephusd_yield,
  };
}

function toPostgresTransactionRecord(
  tx: TxInfoType
): ConversionTransactionRecord {
  return {
    hash: tx.hash,
    blockHeight: Number(tx.block_height),
    blockTimestamp: Number(tx.block_timestamp),
    conversionType: tx.conversion_type,
    conversionRate: tx.conversion_rate,
    fromAsset: tx.from_asset,
    fromAmount: tx.from_amount,
    fromAmountAtoms: tx.from_amount_atoms,
    toAsset: tx.to_asset,
    toAmount: tx.to_amount,
    toAmountAtoms: tx.to_amount_atoms,
    conversionFeeAsset: tx.conversion_fee_asset,
    conversionFeeAmount: tx.conversion_fee_amount,
    txFeeAsset: tx.tx_fee_asset,
    txFeeAmount: tx.tx_fee_amount,
    txFeeAtoms: tx.tx_fee_atoms,
  };
}

function atomsToDecimal(atoms: bigint): number {
  const integerPart = atoms / ATOMIC_UNITS;
  const fractionalPart = atoms % ATOMIC_UNITS;
  return Number(integerPart) + Number(fractionalPart) / ATOMIC_UNITS_NUMBER;
}

function computeRewardSplits(baseRewardAtoms: bigint, blockHeight: number) {
  let reserveRewardAtoms = 0n;
  let governanceRewardAtoms = 0n;
  let yieldRewardAtoms = 0n;

  if (blockHeight >= VERSION_2_HF_V6_BLOCK_HEIGHT) {
    reserveRewardAtoms = (baseRewardAtoms * 3n) / 10n;
    yieldRewardAtoms = baseRewardAtoms / 20n;
  } else if (blockHeight >= HF_V1_BLOCK_HEIGHT) {
    reserveRewardAtoms = baseRewardAtoms / 5n;
    governanceRewardAtoms = baseRewardAtoms / 20n;
  } else {
    governanceRewardAtoms = baseRewardAtoms / 20n;
  }

  const minerRewardAtoms =
    baseRewardAtoms -
    reserveRewardAtoms -
    governanceRewardAtoms -
    yieldRewardAtoms;

  return {
    baseRewardAtoms,
    reserveRewardAtoms,
    governanceRewardAtoms,
    yieldRewardAtoms,
    minerRewardAtoms,
  };
}

function solveBaseRewardFromMinerShare(
  minerShareAtoms: bigint,
  blockHeight: number
): bigint {
  if (minerShareAtoms <= 0n) return 0n;

  let lower = minerShareAtoms;
  let upper: bigint;

  if (blockHeight >= VERSION_2_HF_V6_BLOCK_HEIGHT) {
    upper = (minerShareAtoms * 100n) / 65n + 10n;
  } else if (blockHeight >= HF_V1_BLOCK_HEIGHT) {
    upper = (minerShareAtoms * 100n) / 75n + 10n;
  } else {
    upper = (minerShareAtoms * 100n) / 95n + 10n;
  }

  while (lower < upper) {
    const mid = (lower + upper) / 2n;
    const { minerRewardAtoms: computed } = computeRewardSplits(
      mid,
      blockHeight
    );
    if (computed === minerShareAtoms) return mid;
    if (computed < minerShareAtoms) {
      lower = mid + 1n;
    } else {
      upper = mid;
    }
  }

  return lower;
}

function determineConversionType(input: string, outputs: string[]): string {
  if (input === "ZEPH" && outputs.includes("ZEPHUSD")) return "mint_stable";
  if (input === "ZEPHUSD" && outputs.includes("ZEPH")) return "redeem_stable";
  if (input === "ZEPH" && outputs.includes("ZEPHRSV")) return "mint_reserve";
  if (input === "ZEPHRSV" && outputs.includes("ZEPH")) return "redeem_reserve";
  if (input === "ZEPHUSD" && outputs.includes("ZYIELD")) return "mint_yield";
  if (input === "ZYIELD" && outputs.includes("ZEPHUSD")) return "redeem_yield";
  if (input === "ZPH" && outputs.includes("ZSD")) return "mint_stable";
  if (input === "ZSD" && outputs.includes("ZPH")) return "redeem_stable";
  if (input === "ZPH" && outputs.includes("ZRS")) return "mint_reserve";
  if (input === "ZRS" && outputs.includes("ZPH")) return "redeem_reserve";
  if (input === "ZSD" && outputs.includes("ZYS")) return "mint_yield";
  if (input === "ZYS" && outputs.includes("ZSD")) return "redeem_yield";
  if (input === "ZEPH" && outputs.includes("ZPH")) return "audit_zeph";
  if (input === "ZEPHUSD" && outputs.includes("ZSD")) return "audit_zsd";
  if (input === "ZEPHRSV" && outputs.includes("ZRS")) return "audit_zrs";
  if (input === "ZYIELD" && outputs.includes("ZYS")) return "audit_zys";
  return "na";
}

// ─── Pricing record helpers ───

// In-memory cache of pricing records populated during the unified scan.
// This avoids DB misses when a tx references a pricing_record_height
// that's still in the write buffer (not yet flushed to DB).
const pricingCache = new Map<number, {
  height: number;
  timestamp: number;
  spot: number;
  moving_average: number;
  reserve: number;
  reserve_ma: number;
  stable: number;
  stable_ma: number;
  yield_price: number;
}>();

async function getRedisPricingRecord(height: number) {
  // Check in-memory cache first (populated by unified scan)
  const cached = pricingCache.get(height);
  if (cached) return cached;

  const record = await stores.pricing.get(height);
  if (record) {
    return {
      height: record.blockHeight,
      timestamp: record.timestamp,
      spot: record.spot,
      moving_average: record.movingAverage,
      reserve: record.reserve,
      reserve_ma: record.reserveMa,
      stable: record.stable,
      stable_ma: record.stableMa,
      yield_price: record.yieldPrice,
    };
  }
  if (useRedis()) {
    const pr = await redis.hget("pricing_records", height.toString());
    if (pr) return JSON.parse(pr);
  }
  return null;
}

// ─── Scanner state ───

async function getStoredTxHeight() {
  const height = await stores.scannerState.get("height_txs");
  if (!height) return -1;
  const parsed = Number(height);
  return Number.isFinite(parsed) ? parsed : -1;
}

async function setStoredTxHeight(height: number) {
  await stores.scannerState.set("height_txs", height.toString());
}

async function ensureTotalsBaseline() {
  if (usePostgres()) {
    const totals = await getTotalsRow();
    if (!totals) {
      await setTotals(defaultTotals);
    }
  }
  if (useRedis()) {
    const totalsExists = await redis.exists("totals");
    if (!totalsExists) {
      await redis.hset("totals", "miner_reward", 0, "governance_reward", 0);
    }
  }
}

// ─── processTx (unified version, requires prefetched data) ───

interface ProcessTxOptions {
  minerFeeAdjustmentAtoms?: bigint;
  prefetchedTxData?: any;
  blockRewardBuffer?: BlockRewardRecord[];
}

interface ProcessTxResult {
  incr_totals: IncrTotals;
  tx_info?: TxInfoType;
  feeAtoms?: bigint;
  debug?: {
    baseRewardAtoms: bigint;
    feeAdjustmentAtoms: bigint;
    reserveRewardAtoms: bigint;
    minerRewardAtoms: bigint;
  };
}

async function processTx(
  hash: string,
  verbose_logs: boolean,
  pipeline: Pipeline | null,
  options: ProcessTxOptions = {}
): Promise<ProcessTxResult> {
  const incr_totals: IncrTotals = blankIncrTotals();

  const response_data: any = options.prefetchedTxData;
  if (!response_data) {
    console.error("processTx: No prefetched data for tx", hash);
    return { incr_totals };
  }

  const { txs: [tx_data = {}] = [] } = response_data;
  const { as_json: tx_json_string } = tx_data;
  const { block_height } = tx_data;
  const { block_timestamp } = tx_data;
  const blockHeightNumber = Number(block_height ?? 0);

  if (!tx_json_string) {
    console.error("No valid transaction JSON data found.");
    return { incr_totals };
  }

  const tx_json = JSON.parse(tx_json_string);
  const {
    amount_burnt,
    amount_minted,
    vin,
    vout,
    rct_signatures,
    pricing_record_height,
  } = tx_json;
  const amountBurntAtoms = BigInt(amount_burnt ?? 0);
  const amountMintedAtoms = BigInt(amount_minted ?? 0);

  const feeAsset = vin?.[0]?.key?.asset_type ?? "ZEPH";
  const isMinerFee = feeAsset === "ZEPH" || feeAsset === "ZPH";
  const txFeeAtoms = BigInt(rct_signatures?.txnFee ?? 0);

  if (!(amount_burnt && amount_minted)) {
    const tx_amount = vout[0]?.amount || 0;
    if (tx_amount === 0) {
      return { incr_totals, feeAtoms: isMinerFee ? txFeeAtoms : 0n };
    }

    // Genesis block (block 0): the 500K ZEPH treasury is a flat pre-mine,
    // NOT a mining reward. Save an all-zero BlockReward so the aggregator
    // finds a valid record. The aggregator handles the treasury separately.
    if (blockHeightNumber === 0) {
      const zeroInfo = {
        height: 0, miner_reward: 0, governance_reward: 0, reserve_reward: 0, yield_reward: 0,
        miner_reward_atoms: "0", governance_reward_atoms: "0", reserve_reward_atoms: "0",
        yield_reward_atoms: "0", base_reward_atoms: "0", fee_adjustment_atoms: "0",
      };
      if (pipeline) {
        pipeline.hset("block_rewards", 0, JSON.stringify(zeroInfo));
      }
      if (usePostgres() && options.blockRewardBuffer) {
        options.blockRewardBuffer.push({
          blockHeight: 0, minerReward: 0, governanceReward: 0, reserveReward: 0, yieldReward: 0,
          minerRewardAtoms: "0", governanceRewardAtoms: "0", reserveRewardAtoms: "0",
          yieldRewardAtoms: "0", baseRewardAtoms: "0", feeAdjustmentAtoms: "0",
        });
      }
      return { incr_totals, feeAtoms: 0n };
    }

    // Miner reward transaction
    const minerRewardAtoms = BigInt(tx_amount);
    const minerTxFeeAtoms = txFeeAtoms;
    const feeAdjustmentAtoms = options.minerFeeAdjustmentAtoms ?? 0n;
    const minerRewardExFeesAtoms =
      minerRewardAtoms > feeAdjustmentAtoms
        ? minerRewardAtoms - feeAdjustmentAtoms
        : 0n;
    const baseRewardAtoms = solveBaseRewardFromMinerShare(
      minerRewardExFeesAtoms,
      blockHeightNumber
    );
    const splits = computeRewardSplits(baseRewardAtoms, blockHeightNumber);

    const miner_reward = atomsToDecimal(minerRewardAtoms);
    const governance_reward = atomsToDecimal(splits.governanceRewardAtoms);
    const reserve_reward = atomsToDecimal(splits.reserveRewardAtoms);
    const yield_reward = atomsToDecimal(splits.yieldRewardAtoms);

    const info = {
      height: blockHeightNumber,
      miner_reward,
      governance_reward,
      reserve_reward,
      yield_reward,
      miner_reward_atoms: minerRewardAtoms.toString(),
      governance_reward_atoms: splits.governanceRewardAtoms.toString(),
      reserve_reward_atoms: splits.reserveRewardAtoms.toString(),
      yield_reward_atoms: splits.yieldRewardAtoms.toString(),
      base_reward_atoms: baseRewardAtoms.toString(),
      fee_adjustment_atoms: feeAdjustmentAtoms.toString(),
    };

    if (pipeline) {
      pipeline.hset("block_rewards", blockHeightNumber, JSON.stringify(info));
    }

    const dbRecord: BlockRewardRecord = {
      blockHeight: blockHeightNumber,
      minerReward: miner_reward,
      governanceReward: governance_reward,
      reserveReward: reserve_reward,
      yieldReward: yield_reward,
      minerRewardAtoms: info.miner_reward_atoms,
      governanceRewardAtoms: info.governance_reward_atoms,
      reserveRewardAtoms: info.reserve_reward_atoms,
      yieldRewardAtoms: info.yield_reward_atoms,
      baseRewardAtoms: info.base_reward_atoms,
      feeAdjustmentAtoms: info.fee_adjustment_atoms,
    };

    if (usePostgres() && options.blockRewardBuffer) {
      options.blockRewardBuffer.push(dbRecord);
    }

    incr_totals.miner_reward += miner_reward;
    incr_totals.governance_reward += governance_reward;
    incr_totals.reserve_reward += reserve_reward;
    incr_totals.yield_reward += yield_reward;
    return {
      incr_totals,
      feeAtoms: minerTxFeeAtoms,
      debug: {
        baseRewardAtoms,
        feeAdjustmentAtoms,
        reserveRewardAtoms: splits.reserveRewardAtoms,
        minerRewardAtoms,
      },
    };
  }

  // Conversion transaction
  incr_totals.conversion_transactions += 1;

  const input_asset_type = vin[0]?.key?.asset_type || undefined;
  const output_asset_types = vout
    .map((v: any) => v?.target?.tagged_key?.asset_type)
    .filter(Boolean);

  const conversion_type = determineConversionType(
    input_asset_type,
    output_asset_types
  );

  const isAuditTx =
    conversion_type.startsWith("audit_") ||
    (pricing_record_height === 0 &&
      amountBurntAtoms === amountMintedAtoms);
  if (isAuditTx) {
    return { incr_totals, feeAtoms: isMinerFee ? txFeeAtoms : 0n };
  }

  if (conversion_type === "na") {
    console.log("Error - Can't determine conversion type");
    return { incr_totals, feeAtoms: isMinerFee ? txFeeAtoms : 0n };
  }

  if (pricing_record_height === 0) {
    console.error(
      "Tx - pricing_record_height is 0 for non-audit conversion transaction"
    );
    return { incr_totals, feeAtoms: isMinerFee ? txFeeAtoms : 0n };
  }

  const relevant_pr = await getRedisPricingRecord(pricing_record_height);
  if (!relevant_pr) {
    console.log(
      `No pricing record found for height: ${pricing_record_height}`
    );
    return { incr_totals, feeAtoms: isMinerFee ? txFeeAtoms : 0n };
  }

  const { spot, moving_average, reserve, reserve_ma, yield_price } =
    relevant_pr;

  let conversion_rate = 0;
  let from_asset = "";
  let from_amount = 0;
  let to_asset = "";
  let to_amount = 0;
  let conversion_fee_asset = "";
  let conversion_fee_amount = 0;
  let tx_fee_asset = "";
  let from_amount_atoms_str: string | undefined;
  let to_amount_atoms_str: string | undefined;

  switch (conversion_type) {
    case "mint_stable":
      conversion_rate = Math.max(spot, moving_average);
      from_asset = "ZEPH";
      from_amount = amount_burnt * DEATOMIZE;
      from_amount_atoms_str = amountBurntAtoms.toString();
      to_asset = "ZEPHUSD";
      to_amount = amount_minted * DEATOMIZE;
      to_amount_atoms_str = amountMintedAtoms.toString();
      {
        const fee =
          block_height < ARTEMIS_HF_V5_BLOCK_HEIGHT ? 0.02 : 0.001;
        conversion_fee_asset = to_asset;
        conversion_fee_amount = (to_amount / (1 - fee)) * fee;
        tx_fee_asset = from_asset;
      }
      incr_totals.mint_stable_count += 1;
      incr_totals.mint_stable_volume += to_amount;
      incr_totals.fees_zephusd += conversion_fee_amount;
      break;
    case "redeem_stable":
      conversion_rate = Math.min(spot, moving_average);
      from_asset = "ZEPHUSD";
      from_amount = amount_burnt * DEATOMIZE;
      from_amount_atoms_str = amountBurntAtoms.toString();
      to_asset = "ZEPH";
      to_amount = amount_minted * DEATOMIZE;
      to_amount_atoms_str = amountMintedAtoms.toString();
      {
        const fee =
          block_height < ARTEMIS_HF_V5_BLOCK_HEIGHT ? 0.02 : 0.001;
        conversion_fee_asset = to_asset;
        conversion_fee_amount = (to_amount / (1 - fee)) * fee;
        tx_fee_asset = from_asset;
      }
      incr_totals.redeem_stable_count += 1;
      incr_totals.redeem_stable_volume += to_amount;
      incr_totals.fees_zeph += conversion_fee_amount;
      break;
    case "mint_reserve":
      conversion_rate = Math.max(reserve, reserve_ma);
      from_asset = "ZEPH";
      from_amount = amount_burnt * DEATOMIZE;
      from_amount_atoms_str = amountBurntAtoms.toString();
      to_asset = "ZEPHRSV";
      to_amount = amount_minted * DEATOMIZE;
      to_amount_atoms_str = amountMintedAtoms.toString();
      {
        const fee =
          block_height < ARTEMIS_HF_V5_BLOCK_HEIGHT ? 0 : 0.01;
        conversion_fee_asset = to_asset;
        conversion_fee_amount = (to_amount / (1 - fee)) * fee;
        tx_fee_asset = from_asset;
      }
      incr_totals.mint_reserve_count += 1;
      incr_totals.mint_reserve_volume += to_amount;
      incr_totals.fees_zephrsv += conversion_fee_amount;
      break;
    case "redeem_reserve":
      conversion_rate = Math.min(reserve, reserve_ma);
      from_asset = "ZEPHRSV";
      from_amount = amount_burnt * DEATOMIZE;
      from_amount_atoms_str = amountBurntAtoms.toString();
      to_asset = "ZEPH";
      to_amount = amount_minted * DEATOMIZE;
      to_amount_atoms_str = amountMintedAtoms.toString();
      {
        const fee =
          block_height < ARTEMIS_HF_V5_BLOCK_HEIGHT ? 0.02 : 0.01;
        conversion_fee_asset = to_asset;
        conversion_fee_amount = (to_amount / (1 - fee)) * fee;
        tx_fee_asset = from_asset;
      }
      incr_totals.redeem_reserve_count += 1;
      incr_totals.redeem_reserve_volume += to_amount;
      incr_totals.fees_zeph += conversion_fee_amount;
      break;
    case "mint_yield":
      conversion_rate = yield_price;
      from_asset = "ZEPHUSD";
      from_amount = amount_burnt * DEATOMIZE;
      from_amount_atoms_str = amountBurntAtoms.toString();
      to_asset = "ZYIELD";
      to_amount = amount_minted * DEATOMIZE;
      to_amount_atoms_str = amountMintedAtoms.toString();
      {
        const fee = 0.001;
        conversion_fee_asset = to_asset;
        conversion_fee_amount = (to_amount / (1 - fee)) * fee;
        tx_fee_asset = from_asset;
      }
      incr_totals.mint_yield_count += 1;
      incr_totals.mint_yield_volume += to_amount;
      incr_totals.fees_zyield += conversion_fee_amount;
      break;
    case "redeem_yield":
      conversion_rate = yield_price;
      from_asset = "ZYIELD";
      from_amount = amount_burnt * DEATOMIZE;
      from_amount_atoms_str = amountBurntAtoms.toString();
      to_asset = "ZEPHUSD";
      to_amount = amount_minted * DEATOMIZE;
      to_amount_atoms_str = amountMintedAtoms.toString();
      {
        const fee = 0.001;
        conversion_fee_asset = to_asset;
        conversion_fee_amount = (to_amount / (1 - fee)) * fee;
        tx_fee_asset = from_asset;
      }
      incr_totals.redeem_yield_count += 1;
      incr_totals.redeem_yield_volume += to_amount;
      incr_totals.fees_zephusd_yield += conversion_fee_amount;
      break;
  }

  const tx_fee_amount = Number(txFeeAtoms) * DEATOMIZE;

  const tx_info: TxInfoType = {
    hash,
    block_height,
    block_timestamp,
    conversion_type,
    conversion_rate,
    from_asset,
    from_amount,
    from_amount_atoms: from_amount_atoms_str,
    to_asset,
    to_amount,
    to_amount_atoms: to_amount_atoms_str,
    conversion_fee_asset,
    conversion_fee_amount,
    tx_fee_asset,
    tx_fee_amount,
    tx_fee_atoms: txFeeAtoms.toString(),
  };

  return { incr_totals, tx_info, feeAtoms: isMinerFee ? txFeeAtoms : 0n };
}

// ─── Main unified scan ───

export async function scanBlocksUnified(reset = false) {
  const hfHeight = 0;
  const rpcHeight = await getCurrentBlockHeight();
  const storedPricingHeight = await stores.pricing.getLatestHeight();
  const storedTxHeight = await getStoredTxHeight();
  const postgresEnabled = usePostgres();
  const redisEnabled = useRedis();

  await ensureTotalsBaseline();

  const configStartBlock = getStartBlock();
  const configEndBlock = getEndBlock();
  const effectiveHfHeight =
    configStartBlock > 0 ? configStartBlock : hfHeight;
  const effectiveEndHeight =
    configEndBlock > 0
      ? Math.min(configEndBlock, rpcHeight - 1)
      : rpcHeight - 1;

  // Start from whichever is further behind
  const startingHeight = reset
    ? effectiveHfHeight
    : Math.max(
        Math.min(storedPricingHeight, storedTxHeight) + 1,
        effectiveHfHeight
      );

  if (reset) {
    if (redisEnabled) {
      await redis.del("totals");
      await redis.del("txs");
      await redis.del("txs_by_block");
      await redis.del("block_rewards");
    }
    await ensureTotalsBaseline();
    await setStoredTxHeight(hfHeight);
    if (postgresEnabled) {
      await deleteAllTransactions();
      const { deleteAllBlockRewards } = await import("./db/blockRewards");
      await deleteAllBlockRewards();
      await setTotals(defaultTotals);
    }
  }

  console.log("Fired unified scanner...");
  console.log(
    `Starting height: ${startingHeight} | Ending height: ${effectiveEndHeight}${configEndBlock > 0 ? ` (capped by END_BLOCK)` : ""}`
  );

  if (process.env.WALKTHROUGH_MODE === "true") {
    await fs.writeFile(WALKTHROUGH_DEBUG_LOG, "");
  }

  const verbose_logs = false;
  const total_of_total_increments: IncrTotals = blankIncrTotals();
  const progressStep = Math.max(
    1,
    Math.floor((effectiveEndHeight - startingHeight) / 100)
  );
  const pipeline = redisEnabled
    ? (redis.pipeline() as Pipeline)
    : null;

  // Buffers
  const pricingBuffer: PricingRecordInput[] = [];
  const blockRewardBuffer: BlockRewardRecord[] = [];
  const canBatchPricing = typeof stores.pricing.saveBatch === "function";

  const flushPricing = async () => {
    if (pricingBuffer.length === 0) return;
    if (canBatchPricing) {
      await stores.pricing.saveBatch!(pricingBuffer);
    } else {
      for (const record of pricingBuffer) {
        await stores.pricing.save(record);
      }
    }
    pricingBuffer.length = 0;
  };

  const flushBlockRewards = async (upToHeight: number) => {
    if (postgresEnabled && blockRewardBuffer.length > 0) {
      await upsertBlockRewardBatch(blockRewardBuffer);
      blockRewardBuffer.length = 0;
    }
    if (postgresEnabled && !redisEnabled) {
      await setStoredTxHeight(upToHeight);
    }
  };

  const flushAll = async (upToHeight: number) => {
    await flushPricing();
    await flushBlockRewards(upToHeight);
  };

  // Process blocks in chunks with concurrent RPC fetching
  for (
    let chunkStart = startingHeight;
    chunkStart <= effectiveEndHeight;
    chunkStart += RPC_CHUNK_SIZE
  ) {
    const chunkEnd = Math.min(
      chunkStart + RPC_CHUNK_SIZE - 1,
      effectiveEndHeight
    );
    const heights = Array.from(
      { length: chunkEnd - chunkStart + 1 },
      (_, i) => chunkStart + i
    );

    // Fetch all blocks in this chunk concurrently
    const blocks = await fetchConcurrent(
      heights,
      (h) => getBlock(h),
      RPC_CONCURRENCY
    );

    // Process each block sequentially
    for (let i = 0; i < heights.length; i++) {
      const height = heights[i];
      const block = blocks[i];

      if (!block) {
        console.log(
          `${height}/${effectiveEndHeight} - No block info found, exiting try later`
        );
        await flushAll(height);
        return;
      }

      // ── Pricing record extraction ──
      if (redisEnabled) {
        await redis.hset(
          "block_hashes",
          height,
          block.result.block_header.hash
        );
      }
      const pricingRecord = block.result.block_header.pricing_record;
      let prInput: PricingRecordInput;
      if (!pricingRecord) {
        prInput = {
          blockHeight: height,
          timestamp: block.result.block_header.timestamp,
          spot: 0,
          movingAverage: 0,
          reserve: 0,
          reserveMa: 0,
          stable: 0,
          stableMa: 0,
          yieldPrice: 0,
        };
      } else {
        prInput = {
          blockHeight: height,
          timestamp: pricingRecord.timestamp,
          spot: pricingRecord.spot * DEATOMIZE,
          movingAverage: pricingRecord.moving_average * DEATOMIZE,
          reserve: pricingRecord.reserve * DEATOMIZE,
          reserveMa: pricingRecord.reserve_ma * DEATOMIZE,
          stable: pricingRecord.stable * DEATOMIZE,
          stableMa: pricingRecord.stable_ma * DEATOMIZE,
          yieldPrice: pricingRecord.yield_price
            ? pricingRecord.yield_price * DEATOMIZE
            : 0,
        };
      }
      pricingBuffer.push(prInput);

      // Populate in-memory cache so tx processing can look up recent pricing records
      // that haven't been flushed to DB yet
      pricingCache.set(height, {
        height: prInput.blockHeight,
        timestamp: prInput.timestamp,
        spot: prInput.spot,
        moving_average: prInput.movingAverage,
        reserve: prInput.reserve,
        reserve_ma: prInput.reserveMa,
        stable: prInput.stable,
        stable_ma: prInput.stableMa,
        yield_price: prInput.yieldPrice,
      });

      // ── Transaction processing ──
      if (height % progressStep === 0 || height === effectiveEndHeight - 1) {
        const percentComplete = (
          ((height - startingHeight) /
            (effectiveEndHeight - startingHeight)) *
          100
        ).toFixed(2);
        console.log(
          `UNIFIED SCANNING BLOCK: [${height + 1}/${effectiveEndHeight}] (${percentComplete}%)`
        );
      }

      // Pre-V1 fast path: handles all pre-V1 blocks (including genesis).
      // Pricing record already saved above; this skips get_transactions RPC.
      if (height < HF_V1_BLOCK_HEIGHT) {
        if (height === 0) {
          // Genesis: save all-zero block reward (aggregator handles 500K treasury)
          const zeroInfo = {
            height: 0, miner_reward: 0, governance_reward: 0, reserve_reward: 0, yield_reward: 0,
            miner_reward_atoms: "0", governance_reward_atoms: "0", reserve_reward_atoms: "0",
            yield_reward_atoms: "0", base_reward_atoms: "0", fee_adjustment_atoms: "0",
          };
          if (pipeline) pipeline.hset("block_rewards", 0, JSON.stringify(zeroInfo));
          if (postgresEnabled) {
            blockRewardBuffer.push({
              blockHeight: 0, minerReward: 0, governanceReward: 0, reserveReward: 0, yieldReward: 0,
              minerRewardAtoms: "0", governanceRewardAtoms: "0", reserveRewardAtoms: "0",
              yieldRewardAtoms: "0", baseRewardAtoms: "0", feeAdjustmentAtoms: "0",
            });
          }
        } else {
          // Extract reward from block.result.json
          const blockJson = JSON.parse(block.result.json);
          const vout = blockJson.miner_tx?.vout;
          const governanceAtoms = BigInt(vout[1].amount);
          const minerShareAtoms = BigInt(vout[0].amount);
          const baseRewardAtoms = governanceAtoms * 20n;
          const minerRewardAtoms = baseRewardAtoms - governanceAtoms;
          const feeAdjustmentAtoms = minerShareAtoms - minerRewardAtoms;

          const minerRewardDec = atomsToDecimal(minerShareAtoms);
          const governanceRewardDec = atomsToDecimal(governanceAtoms);

          const info = {
            height,
            miner_reward: minerRewardDec,
            governance_reward: governanceRewardDec,
            reserve_reward: 0,
            yield_reward: 0,
            miner_reward_atoms: minerShareAtoms.toString(),
            governance_reward_atoms: governanceAtoms.toString(),
            reserve_reward_atoms: "0",
            yield_reward_atoms: "0",
            base_reward_atoms: baseRewardAtoms.toString(),
            fee_adjustment_atoms: feeAdjustmentAtoms.toString(),
          };

          if (pipeline) {
            pipeline.hset("block_rewards", height, JSON.stringify(info));
          }

          if (postgresEnabled) {
            blockRewardBuffer.push({
              blockHeight: height,
              minerReward: minerRewardDec,
              governanceReward: governanceRewardDec,
              reserveReward: 0,
              yieldReward: 0,
              minerRewardAtoms: info.miner_reward_atoms,
              governanceRewardAtoms: info.governance_reward_atoms,
              reserveRewardAtoms: "0",
              yieldRewardAtoms: "0",
              baseRewardAtoms: info.base_reward_atoms,
              feeAdjustmentAtoms: info.fee_adjustment_atoms,
            });
          }

          total_of_total_increments.miner_reward += minerRewardDec;
          total_of_total_increments.governance_reward += governanceRewardDec;
        }

        if (pipeline) pipeline.hset("txs_by_block", height.toString(), JSON.stringify([]));

        if (pricingBuffer.length >= PRICING_BATCH_SIZE || blockRewardBuffer.length >= BLOCK_REWARD_BATCH_SIZE) {
          await flushAll(height);
        }
        continue;
      }

      const txs: string[] = block.result.tx_hashes || [];
      const miner_tx = block.result.miner_tx_hash;
      let totalBlockFeeAtoms = 0n;
      const blockHashes: string[] = [];
      const postgresBlockTransactions: ConversionTransactionRecord[] = [];

      // Batch-fetch all transactions in a single RPC call
      const allHashes = [...txs, miner_tx];
      const batchData = await readTxBatch(allHashes);

      for (const hash of txs) {
        const prefetchedTxData = batchData?.get(hash) ?? null;
        const { incr_totals, tx_info, feeAtoms } = await processTx(
          hash,
          verbose_logs,
          pipeline,
          { prefetchedTxData, blockRewardBuffer }
        );
        if (incr_totals) {
          for (const key of Object.keys(
            incr_totals
          ) as (keyof IncrTotals)[]) {
            total_of_total_increments[key] += incr_totals[key];
          }
        }
        if (tx_info) {
          blockHashes.push(hash);
          if (pipeline) {
            pipeline.hset("txs", hash, JSON.stringify(tx_info));
          }
          if (postgresEnabled) {
            postgresBlockTransactions.push(
              toPostgresTransactionRecord(tx_info)
            );
          }
        }
        if (typeof feeAtoms === "bigint") {
          totalBlockFeeAtoms += feeAtoms;
        }
      }

      const minerPrefetchedData = batchData?.get(miner_tx) ?? null;
      const { incr_totals: miner_tx_incr_totals, debug: minerDebug } =
        await processTx(miner_tx, verbose_logs, pipeline, {
          minerFeeAdjustmentAtoms: totalBlockFeeAtoms,
          prefetchedTxData: minerPrefetchedData,
          blockRewardBuffer,
        });

      if (process.env.WALKTHROUGH_MODE === "true" && minerDebug) {
        const headerRewardAtoms = BigInt(
          block.result?.block_header?.reward ?? 0
        );
        const debugLine = {
          block_height: height,
          header_reward_atoms: headerRewardAtoms.toString(),
          fee_atoms: minerDebug.feeAdjustmentAtoms.toString(),
          reconstructed_base_atoms: minerDebug.baseRewardAtoms.toString(),
          reserve_reward_atoms: minerDebug.reserveRewardAtoms.toString(),
          miner_share_atoms: minerDebug.minerRewardAtoms.toString(),
        };
        await fs.appendFile(
          WALKTHROUGH_DEBUG_LOG,
          `${JSON.stringify(debugLine)}\n`
        );
      }

      for (const key of Object.keys(
        miner_tx_incr_totals
      ) as (keyof IncrTotals)[]) {
        total_of_total_increments[key] += miner_tx_incr_totals[key];
      }

      if (pipeline) {
        pipeline.hset(
          "txs_by_block",
          height.toString(),
          JSON.stringify(blockHashes)
        );
      }

      if (postgresEnabled && postgresBlockTransactions.length > 0) {
        await insertTransactions(postgresBlockTransactions);
      }

      // Periodic flush
      if (
        pricingBuffer.length >= PRICING_BATCH_SIZE ||
        blockRewardBuffer.length >= BLOCK_REWARD_BATCH_SIZE
      ) {
        await flushAll(height);
      }
    }
  }

  // Final flush
  await flushAll(effectiveEndHeight);

  if (pipeline) {
    for (const key of Object.keys(
      total_of_total_increments
    ) as (keyof IncrTotals)[]) {
      pipeline.hincrbyfloat("totals", key, total_of_total_increments[key]);
    }
    console.log(`Pipeline command count: ${pipeline.length}`);
    await pipeline.exec();
  }

  if (postgresEnabled) {
    await incrementTotals(mapTotalsDelta(total_of_total_increments));
  }

  await setStoredTxHeight(effectiveEndHeight);
}
