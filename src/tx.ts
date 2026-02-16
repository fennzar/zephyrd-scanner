import fs from "node:fs/promises";
import { Pipeline } from "ioredis";

import { getCurrentBlockHeight, getBlock, readTx } from "./utils";
import redis from "./redis";
import { usePostgres, useRedis, getStartBlock, getEndBlock } from "./config";
import { stores } from "./storage/factory";
import { deleteAllBlockRewards, upsertBlockReward } from "./db/blockRewards";
import { insertTransactions, ConversionTransactionRecord, deleteAllTransactions } from "./db/transactions";
import { defaultTotals, getTotals as getTotalsRow, incrementTotals, setTotals } from "./db/totals";

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

const DEATOMIZE = 10 ** -12;
const HF_V1_BLOCK_HEIGHT = 89300;
const ARTEMIS_HF_V5_BLOCK_HEIGHT = 295000;
const VERSION_2_HF_V6_BLOCK_HEIGHT = 360000;

const ATOMIC_UNITS = 1_000_000_000_000n;
const ATOMIC_UNITS_NUMBER = Number(ATOMIC_UNITS);
const WALKTHROUGH_DEBUG_LOG = process.env.WALKTHROUGH_DEBUG_LOG ?? "walkthrough_debug.log";

const MINER_REWARD_BASELINE = 1391857.1317692809;
const GOVERNANCE_REWARD_BASELINE = 73255.6385141733;

function toPostgresTransactionRecord(tx: TxInfoType): ConversionTransactionRecord {
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

async function ensureTotalsBaseline() {
  if (usePostgres()) {
    const totals = await getTotalsRow();
    if (!totals) {
      await setTotals({
        ...defaultTotals,
        minerReward: MINER_REWARD_BASELINE,
        governanceReward: GOVERNANCE_REWARD_BASELINE,
      });
    } else {
      const updates: Partial<typeof defaultTotals> = {};
      if (totals.minerReward <= 0) {
        updates.minerReward = MINER_REWARD_BASELINE;
      }
      if (totals.governanceReward <= 0) {
        updates.governanceReward = GOVERNANCE_REWARD_BASELINE;
      }
      if (Object.keys(updates).length > 0) {
        await incrementTotals(updates);
      }
    }
  }

  if (useRedis()) {
    const totalsExists = await redis.exists("totals");
    if (!totalsExists) {
      await redis.hset("totals", "miner_reward", MINER_REWARD_BASELINE, "governance_reward", GOVERNANCE_REWARD_BASELINE);
      return;
    }

    const [minerReward, governanceReward] = await redis.hmget("totals", "miner_reward", "governance_reward");

    if (minerReward === null) {
      await redis.hset("totals", "miner_reward", MINER_REWARD_BASELINE);
    }

    if (governanceReward === null) {
      await redis.hset("totals", "governance_reward", GOVERNANCE_REWARD_BASELINE);
    }
  }
}

async function getStoredTxHeight() {
  const height = await stores.scannerState.get("height_txs");
  if (!height) {
    return 0;
  }
  const parsed = Number(height);
  return Number.isFinite(parsed) ? parsed : 0;
}

async function setStoredTxHeight(height: number) {
  await stores.scannerState.set("height_txs", height.toString());
}

async function getRedisPricingRecord(height: number) {
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

async function saveBlockRewardInfo(
  height: number,
  miner_reward: number,
  governance_reward: number,
  reserve_reward: number,
  yield_reward: number,
  pipeline: Pipeline | null,
  rewardAtoms?: {
    miner: bigint;
    governance: bigint;
    reserve: bigint;
    yield: bigint;
    base?: bigint;
    feeAdjustment?: bigint;
  }
) {
  let block_reward_info = {
    height: height,
    miner_reward: miner_reward,
    governance_reward: governance_reward,
    reserve_reward: reserve_reward,
    yield_reward: yield_reward,
    miner_reward_atoms: rewardAtoms ? rewardAtoms.miner.toString() : undefined,
    governance_reward_atoms: rewardAtoms ? rewardAtoms.governance.toString() : undefined,
    reserve_reward_atoms: rewardAtoms ? rewardAtoms.reserve.toString() : undefined,
    yield_reward_atoms: rewardAtoms ? rewardAtoms.yield.toString() : undefined,
    base_reward_atoms: rewardAtoms?.base ? rewardAtoms.base.toString() : undefined,
    fee_adjustment_atoms: rewardAtoms?.feeAdjustment != null ? rewardAtoms.feeAdjustment.toString() : undefined,
  };

  const block_reward_info_json = JSON.stringify(block_reward_info);
  if (pipeline) {
    pipeline.hset("block_rewards", height, block_reward_info_json);
  }

  if (usePostgres()) {
    await upsertBlockReward({
      blockHeight: height,
      minerReward: miner_reward,
      governanceReward: governance_reward,
      reserveReward: reserve_reward,
      yieldReward: yield_reward,
      minerRewardAtoms: block_reward_info.miner_reward_atoms,
      governanceRewardAtoms: block_reward_info.governance_reward_atoms,
      reserveRewardAtoms: block_reward_info.reserve_reward_atoms,
      yieldRewardAtoms: block_reward_info.yield_reward_atoms,
      baseRewardAtoms: block_reward_info.base_reward_atoms,
      feeAdjustmentAtoms: block_reward_info.fee_adjustment_atoms,
    });
  }

  // increment totals - doing at an upper level now to avoid multiple calls
  // pipeline.hincrbyfloat("totals", "miner_reward", miner_reward);
  // pipeline.hincrbyfloat("totals", "governance_reward", governance_reward);
  // pipeline.hincrbyfloat("totals", "reserve_reward", reserve_reward);
  // pipeline.hincrbyfloat("totals", "yield_reward", yield_reward);
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

  const minerRewardAtoms = baseRewardAtoms - reserveRewardAtoms - governanceRewardAtoms - yieldRewardAtoms;

  return {
    baseRewardAtoms,
    reserveRewardAtoms,
    governanceRewardAtoms,
    yieldRewardAtoms,
    minerRewardAtoms,
  };
}

function solveBaseRewardFromMinerShare(minerShareAtoms: bigint, blockHeight: number): bigint {
  if (minerShareAtoms <= 0n) {
    return 0n;
  }

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
    const { minerRewardAtoms: computed } = computeRewardSplits(mid, blockHeight);
    if (computed === minerShareAtoms) {
      return mid;
    }
    if (computed < minerShareAtoms) {
      lower = mid + 1n;
    } else {
      upper = mid;
    }
  }

  return lower;
}

function blankIncrTotals(): IncrTotals {
  return {
    // From saveBlockRewardInfo
    miner_reward: 0,
    governance_reward: 0,
    reserve_reward: 0,
    yield_reward: 0,
    // --- Conversion transactions ---
    conversion_transactions: 0,
    // Mint Stable
    mint_stable_count: 0,
    mint_stable_volume: 0,
    fees_zephusd: 0,
    // Redeem Stable
    redeem_stable_count: 0,
    redeem_stable_volume: 0,
    fees_zeph: 0,
    // Mint Reserve
    mint_reserve_count: 0,
    mint_reserve_volume: 0,
    fees_zephrsv: 0,
    // Redeem Reserve
    redeem_reserve_count: 0,
    redeem_reserve_volume: 0,
    // fees_zeph: 0, - already defined with redeem_stable
    // Mint Yield
    mint_yield_count: 0,
    mint_yield_volume: 0,
    fees_zyield: 0,
    // Redeem Yield
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

interface ProcessTxOptions {
  minerFeeAdjustmentAtoms?: bigint;
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
  if (verbose_logs) console.log(`\tProcessing tx: ${hash}`);

  // increment totals object for this transaction
  const incr_totals: IncrTotals = blankIncrTotals();

  const response_data: any = await readTx(hash);
  if (!response_data) {
    console.error("Failed to retrieve transaction data.");
    return { incr_totals };
  }

  const { txs: [tx_data = {}] = [] } = response_data;
  const { as_json: tx_json_string } = tx_data;
  const { block_height: block_height } = tx_data;
  const { block_timestamp: block_timestamp } = tx_data;
  const blockHeightNumber = Number(block_height ?? 0);

  if (!tx_json_string) {
    console.error("No valid transaction JSON data found.");
    return { incr_totals };
  }

  const tx_json = JSON.parse(tx_json_string);
  const { amount_burnt, amount_minted, vin, vout, rct_signatures, pricing_record_height } = tx_json;
  const amountBurntAtoms = BigInt(amount_burnt ?? 0);
  const amountMintedAtoms = BigInt(amount_minted ?? 0);

  // Fee asset = input asset type. Only ZEPH/ZPH fees go to the miner's coinbase;
  // non-ZEPH fees (e.g. from redeem_stable paying in ZEPHUSD) must NOT be
  // subtracted from the miner payout when reconstructing the base reward.
  const feeAsset = vin?.[0]?.key?.asset_type ?? "ZEPH";
  const isMinerFee = feeAsset === "ZEPH" || feeAsset === "ZPH";
  const txFeeAtoms = BigInt(rct_signatures?.txnFee ?? 0);

  if (!(amount_burnt && amount_minted)) {
    const tx_amount = vout[0]?.amount || 0;
    if (tx_amount === 0) {
      if (verbose_logs) console.log("\t\tSKIP -> Not a conversion transaction or block reward transaction");
      return { incr_totals, feeAtoms: isMinerFee ? txFeeAtoms : 0n };
    }

    // Miner reward transaction!
    const minerRewardAtoms = BigInt(tx_amount);
    const minerTxFeeAtoms = txFeeAtoms;
    const feeAdjustmentAtoms = options.minerFeeAdjustmentAtoms ?? 0n;
    const minerRewardExFeesAtoms = minerRewardAtoms > feeAdjustmentAtoms ? minerRewardAtoms - feeAdjustmentAtoms : 0n;
    const baseRewardAtoms = solveBaseRewardFromMinerShare(minerRewardExFeesAtoms, blockHeightNumber);
    const splits = computeRewardSplits(baseRewardAtoms, blockHeightNumber);

    const miner_reward = atomsToDecimal(minerRewardAtoms);
    const governance_reward = atomsToDecimal(splits.governanceRewardAtoms);
    const reserve_reward = atomsToDecimal(splits.reserveRewardAtoms);
    const yield_reward = atomsToDecimal(splits.yieldRewardAtoms);

    if (verbose_logs) {
      console.log(
        `\tBlock reward transaction! base=${atomsToDecimal(
          baseRewardAtoms
        )} miner=${miner_reward} reserve=${reserve_reward} governance=${governance_reward} yield=${yield_reward}`
      );
    }

    await saveBlockRewardInfo(
      blockHeightNumber,
      miner_reward,
      governance_reward,
      reserve_reward,
      yield_reward,
      pipeline,
      {
        miner: minerRewardAtoms,
        governance: splits.governanceRewardAtoms,
        reserve: splits.reserveRewardAtoms,
        yield: splits.yieldRewardAtoms,
        base: baseRewardAtoms,
        feeAdjustment: feeAdjustmentAtoms,
      }
    );

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

  // Conversion transaction!
  if (verbose_logs) console.log("\tConversion transaction!");
  // Add the hash to the txs_by_block for the specific block height
  // handled at block level now

  const input_asset_type = vin[0]?.key?.asset_type || undefined;
  const output_asset_types = vout.map((v: any) => v?.target?.tagged_key?.asset_type).filter(Boolean);

  let conversion_type = determineConversionType(input_asset_type, output_asset_types);

  // Audit/migration transactions (e.g. ZEPH→ZPH) have amount_burnt == amount_minted
  // and pricing_record_height == 0. They are NOT economic conversions — just asset type
  // renames introduced at the AUDIT hardfork. They don't affect reserve or circulation,
  // but their ZEPH fees DO go to the miner's coinbase and must be accounted for.
  const isAuditTx = conversion_type.startsWith("audit_") || (pricing_record_height === 0 && amountBurntAtoms === amountMintedAtoms);
  if (isAuditTx) {
    if (verbose_logs) console.log("\t\tAudit/migration transaction, skipping conversion processing");
    return { incr_totals, feeAtoms: isMinerFee ? txFeeAtoms : 0n };
  }

  if (conversion_type === "na") {
    console.log("Error - Can't determine conversion type");
    return { incr_totals, feeAtoms: isMinerFee ? txFeeAtoms : 0n };
  }

  // pipeline.hincrbyfloat("totals", "conversion_transactions", 1);
  incr_totals.conversion_transactions += 1;

  if (pricing_record_height === 0) {
    console.error(
      "Tx - pricing_record_height is 0 for non-audit conversion transaction"
    );
    console.error("Transaction hash:", hash);
    console.error("Transaction JSON:", tx_json);
    return { incr_totals, feeAtoms: isMinerFee ? txFeeAtoms : 0n };
  }

  if (verbose_logs)
    console.log(`tx_json data (from the node):
    
    amount_burnt: ${amount_burnt} (${amountBurntAtoms} atoms)
    amount_minted: ${amount_minted} (${amountMintedAtoms} atoms)
    input_asset_type: ${input_asset_type}
    output_asset_types: ${output_asset_types.join(", ")}
    conversion_type: ${conversion_type}
    pricing_record_height: ${pricing_record_height}`);

  const relevant_pr = await getRedisPricingRecord(pricing_record_height);

  if (!relevant_pr) {
    console.log(`No pricing record found for height: ${pricing_record_height}`);
    return { incr_totals, feeAtoms: isMinerFee ? txFeeAtoms : 0n };
  }

  const { spot, moving_average, reserve, reserve_ma, stable, stable_ma, yield_price } = relevant_pr;

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

      const mint_stable_conversion_fee = block_height < ARTEMIS_HF_V5_BLOCK_HEIGHT ? 0.02 : 0.001; // 2% -> 0.1%
      // conversion fee is always "paid" in terms of the converted to_asset, in a form of a loss on the resulting converted value (to_amount)
      conversion_fee_asset = to_asset;
      // to_amount is the net amount recieved, conversion fee needs to be back calculated
      conversion_fee_amount = (to_amount / (1 - mint_stable_conversion_fee)) * mint_stable_conversion_fee;
      // transaction fee is always paid in the from_asset. Thinking: Designed as such as you always have some of this asset since you are converting from it.
      tx_fee_asset = from_asset;

      // pipeline.hincrbyfloat("totals", "mint_stable_count", 1);
      // pipeline.hincrbyfloat("totals", "mint_stable_volume", to_amount);
      // pipeline.hincrbyfloat("totals", "fees_zephusd", conversion_fee_amount);
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

      const redeem_stable_conversion_fee = block_height < ARTEMIS_HF_V5_BLOCK_HEIGHT ? 0.02 : 0.001; // 2% -> 0.1%

      conversion_fee_asset = to_asset;
      conversion_fee_amount = (to_amount / (1 - redeem_stable_conversion_fee)) * redeem_stable_conversion_fee;
      tx_fee_asset = from_asset;

      // pipeline.hincrbyfloat("totals", "redeem_stable_count", 1);
      // pipeline.hincrbyfloat("totals", "redeem_stable_volume", to_amount);
      // pipeline.hincrbyfloat("totals", "fees_zeph", conversion_fee_amount);
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

      const mint_reserve_conversion_fee = block_height < ARTEMIS_HF_V5_BLOCK_HEIGHT ? 0 : 0.01; // 0% -> 1%

      conversion_fee_asset = to_asset;
      conversion_fee_amount = (to_amount / (1 - mint_reserve_conversion_fee)) * mint_reserve_conversion_fee;

      tx_fee_asset = from_asset;

      // pipeline.hincrbyfloat("totals", "mint_reserve_count", 1);
      // pipeline.hincrbyfloat("totals", "mint_reserve_volume", to_amount);
      // pipeline.hincrbyfloat("totals", "fees_zephrsv", conversion_fee_amount);
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

      const redeem_reserve_conversion_fee = block_height < ARTEMIS_HF_V5_BLOCK_HEIGHT ? 0.02 : 0.01; // 2% -> 1%

      conversion_fee_asset = to_asset;
      conversion_fee_amount = (to_amount / (1 - redeem_reserve_conversion_fee)) * redeem_reserve_conversion_fee;
      tx_fee_asset = from_asset;

      // pipeline.hincrbyfloat("totals", "redeem_reserve_count", 1);
      // pipeline.hincrbyfloat("totals", "redeem_reserve_volume", to_amount);
      // pipeline.hincrbyfloat("totals", "fees_zeph", conversion_fee_amount);
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

      const mint_yield_conversion_fee = 0.001; // 0.1%

      conversion_fee_asset = to_asset;
      conversion_fee_amount = (to_amount / (1 - mint_yield_conversion_fee)) * mint_yield_conversion_fee;
      tx_fee_asset = from_asset;

      // pipeline.hincrbyfloat("totals", "mint_yield_count", 1);
      // pipeline.hincrbyfloat("totals", "mint_yield_volume", to_amount); // ZYS
      // pipeline.hincrbyfloat("totals", "fees_zyield", conversion_fee_amount); // effective loss in ZYS for minting ZYIELD
      incr_totals.mint_yield_count += 1;
      incr_totals.mint_yield_volume += to_amount; // ZYS
      incr_totals.fees_zyield += conversion_fee_amount; // effective loss in ZYS for minting ZYIELD
      break;

    case "redeem_yield":
      conversion_rate = yield_price;
      from_asset = "ZYIELD";
      from_amount = amount_burnt * DEATOMIZE;
      from_amount_atoms_str = amountBurntAtoms.toString();
      to_asset = "ZEPHUSD";
      to_amount = amount_minted * DEATOMIZE;
      to_amount_atoms_str = amountMintedAtoms.toString();

      const redeem_yield_conversion_fee = 0.001; // 0.1%

      conversion_fee_asset = to_asset;
      conversion_fee_amount = (to_amount / (1 - redeem_yield_conversion_fee)) * redeem_yield_conversion_fee;
      tx_fee_asset = from_asset;

      // pipeline.hincrbyfloat("totals", "redeem_yield_count", 1);
      // pipeline.hincrbyfloat("totals", "redeem_yield_volume", to_amount); // ZEPHUSD
      // pipeline.hincrbyfloat("totals", "fees_zephusd_yield", conversion_fee_amount); // effective loss in ZEPHUSD for redeeming ZYIELD
      incr_totals.redeem_yield_count += 1;
      incr_totals.redeem_yield_volume += to_amount; // ZEPHUSD
      incr_totals.fees_zephusd_yield += conversion_fee_amount; // effective loss in ZEPHUSD for redeeming ZYIELD
      break;
  }

  const tx_fee_amount = Number(txFeeAtoms) * DEATOMIZE;

  const tx_info = {
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

  if (verbose_logs) console.log(tx_info);

  return { incr_totals, tx_info, feeAtoms: isMinerFee ? txFeeAtoms : 0n };
}

function determineConversionType(input: string, outputs: string[]): string {
  if (input === "ZEPH" && outputs.includes("ZEPHUSD")) return "mint_stable";
  if (input === "ZEPHUSD" && outputs.includes("ZEPH")) return "redeem_stable";
  if (input === "ZEPH" && outputs.includes("ZEPHRSV")) return "mint_reserve";
  if (input === "ZEPHRSV" && outputs.includes("ZEPH")) return "redeem_reserve";
  if (input === "ZEPHUSD" && outputs.includes("ZYIELD")) return "mint_yield";
  if (input === "ZYIELD" && outputs.includes("ZEPHUSD")) return "redeem_yield";
  // Asset type V2
  if (input === "ZPH" && outputs.includes("ZSD")) return "mint_stable";
  if (input === "ZSD" && outputs.includes("ZPH")) return "redeem_stable";
  if (input === "ZPH" && outputs.includes("ZRS")) return "mint_reserve";
  if (input === "ZRS" && outputs.includes("ZPH")) return "redeem_reserve";
  if (input === "ZSD" && outputs.includes("ZYS")) return "mint_yield";
  if (input === "ZYS" && outputs.includes("ZSD")) return "redeem_yield";
  // For Debug, we can return audit to ensutre we are catching all cases
  if (input === "ZEPH" && outputs.includes("ZPH")) return "audit_zeph";
  if (input === "ZEPHUSD" && outputs.includes("ZSD")) return "audit_zsd";
  if (input === "ZEPHRSV" && outputs.includes("ZRS")) return "audit_zrs";
  if (input === "YZIELD" && outputs.includes("ZYS")) return "audit_zys";
  return "na";
}

export async function scanTransactions(reset = false) {
  const hfHeight = 89300; // if we just want to scan from the v1.0.0 HF block
  const rpcHeight = await getCurrentBlockHeight();
  // const rpcHeight = 89303; // TEMP OVERRIDE FOR TESTING
  const redisHeight = await getStoredTxHeight();
  const postgresEnabled = usePostgres();

  await ensureTotalsBaseline();

  const configStartBlock = getStartBlock();
  const configEndBlock = getEndBlock();
  const effectiveHfHeight = configStartBlock > 0 ? configStartBlock : hfHeight;

  // Cap tx scan to the pricing scan's height so we never process blocks whose
  // pricing_record_height hasn't been stored yet (the daemon can advance between
  // the pricing scan and tx scan, creating a window of missing pricing records).
  const pricingHeight = await stores.pricing.getLatestHeight();
  const endCandidates = [rpcHeight - 1, pricingHeight];
  if (configEndBlock > 0) endCandidates.push(configEndBlock);
  const effectiveEndHeight = Math.min(...endCandidates);

  let startingHeight = Math.max(redisHeight + 1, effectiveHfHeight);
  if (reset) {
    startingHeight = effectiveHfHeight;
    if (useRedis()) {
      await redis.del("totals");
      await redis.del("txs");
      await redis.del("txs_by_block");
      await redis.del("block_rewards");
    }
    await ensureTotalsBaseline();
    // reset scanner height
    await setStoredTxHeight(hfHeight);
    if (postgresEnabled) {
      await deleteAllTransactions();
      await deleteAllBlockRewards();
      await setTotals({
        ...defaultTotals,
        minerReward: MINER_REWARD_BASELINE,
        governanceReward: GOVERNANCE_REWARD_BASELINE,
      });
    }
  }

  console.log("Fired tx scanner...");
  const cappedByPricing = effectiveEndHeight < rpcHeight - 1 && effectiveEndHeight === pricingHeight;
  console.log(`Starting height: ${startingHeight} | Ending height: ${effectiveEndHeight}${configEndBlock > 0 ? ` (capped by END_BLOCK)` : ''}${cappedByPricing ? ` (capped by pricing height)` : ''}`);

  if (process.env.WALKTHROUGH_MODE === "true") {
    await fs.writeFile(WALKTHROUGH_DEBUG_LOG, "");
  }

  let verbose_logs = false;
  if (effectiveEndHeight - startingHeight > 1000) {
    console.log("This is a large scan, verbose logs are disabled.");
    verbose_logs = false;
  }

  const total_of_total_increments: IncrTotals = blankIncrTotals();
  const progressStep = Math.max(1, Math.floor((effectiveEndHeight - startingHeight) / 100));
  const redisEnabled = useRedis();
  const pipeline = redisEnabled ? redis.pipeline() as Pipeline : null;

  for (let height = startingHeight; height <= effectiveEndHeight; height++) {
    const blockHashes: string[] = [];
    const postgresBlockTransactions: ConversionTransactionRecord[] = [];
    const block: any = await getBlock(height);
    if (!block) {
      console.log(`${height}/${effectiveEndHeight} - No block info found, exiting try later`);
      await setStoredTxHeight(height);
      return;
    }

    if (height % progressStep === 0 || height === effectiveEndHeight - 1) {
      const percentComplete = (((height - startingHeight) / (effectiveEndHeight - startingHeight)) * 100).toFixed(2);
      console.log(`TXs SCANNING BLOCK: [${height + 1}/${effectiveEndHeight}] Processed (${percentComplete}%)`);
    }

    // console.log(`TXs SCANNING BLOCK: ${height}/${rpcHeight - 1} \t | ${percentComplete}%`);

    const txs: string[] = block.result.tx_hashes || [];
    const miner_tx = block.result.miner_tx_hash;
    let totalBlockFeeAtoms = 0n;

    for (const hash of txs) {
      const { incr_totals, tx_info, feeAtoms } = await processTx(hash, verbose_logs, pipeline);
      if (incr_totals) {
        // increment totals
        for (const key of Object.keys(incr_totals) as (keyof IncrTotals)[]) {
          total_of_total_increments[key] += incr_totals[key];
        }
      }
      if (tx_info) {
        blockHashes.push(hash);
        if (pipeline) {
          pipeline.hset("txs", hash, JSON.stringify(tx_info));
        }
        if (postgresEnabled) {
          postgresBlockTransactions.push(toPostgresTransactionRecord(tx_info));
        }
      }
      if (typeof feeAtoms === "bigint") {
        totalBlockFeeAtoms += feeAtoms;
      }
    }

    const { incr_totals: miner_tx_incr_totals, debug: minerDebug } = await processTx(miner_tx, verbose_logs, pipeline, {
      minerFeeAdjustmentAtoms: totalBlockFeeAtoms,
    });

    if (process.env.WALKTHROUGH_MODE === "true" && minerDebug) {
      const headerRewardAtoms = BigInt(block.result?.block_header?.reward ?? 0);
      const debugLine = {
        block_height: height,
        header_reward_atoms: headerRewardAtoms.toString(),
        fee_atoms: minerDebug.feeAdjustmentAtoms.toString(),
        reconstructed_base_atoms: minerDebug.baseRewardAtoms.toString(),
        reserve_reward_atoms: minerDebug.reserveRewardAtoms.toString(),
        miner_share_atoms: minerDebug.minerRewardAtoms.toString(),
      };
      await fs.appendFile(WALKTHROUGH_DEBUG_LOG, `${JSON.stringify(debugLine)}\n`);
    }

    for (const key of Object.keys(miner_tx_incr_totals) as (keyof IncrTotals)[]) {
      total_of_total_increments[key] += miner_tx_incr_totals[key];
    }

    if (pipeline) {
      pipeline.hset("txs_by_block", height.toString(), JSON.stringify(blockHashes));
    }

    if (postgresEnabled && postgresBlockTransactions.length > 0) {
      await insertTransactions(postgresBlockTransactions);
    }

    // In postgres mode, advance height per-block so a crash doesn't leave
    // height_txs behind height_prs (which causes "No block reward info" spam).
    // In redis mode, height is set once after the pipeline executes.
    if (postgresEnabled && !redisEnabled) {
      await setStoredTxHeight(height);
    }
  }

  if (pipeline) {
    for (const key of Object.keys(total_of_total_increments) as (keyof IncrTotals)[]) {
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

// (async () => {
//   await scanTransactions();
// })();
