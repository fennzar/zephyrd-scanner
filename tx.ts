import { getCurrentBlockHeight, getBlock, readTx } from "./utils";
import redis from "./redis";
import { Pipeline } from "ioredis";


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
  to_asset: string;
  to_amount: number;
  conversion_fee_asset: string;
  conversion_fee_amount: number;
  tx_fee_asset: string;
  tx_fee_amount: number;
}

const DEATOMIZE = 10 ** -12;
const HF_V1_BLOCK_HEIGHT = 89300;
const ARTEMIS_HF_V5_BLOCK_HEIGHT = 295000;
const VERSION_2_HF_V6_BLOCK_HEIGHT = 360000;

async function getRedisHeight() {
  const height = await redis.get("height_txs");
  if (!height) {
    return 0;
  }
  return parseInt(height);
}

async function setRedisHeight(height: number) {
  await redis.set("height_txs", height);
}

async function getRedisPricingRecord(height: number) {
  const pr = await redis.hget("pricing_records", height.toString());
  if (!pr) {
    return null;
  }
  return JSON.parse(pr);
}

async function saveBlockRewardInfo(
  height: number,
  miner_reward: number,
  governance_reward: number,
  reserve_reward: number,
  yield_reward: number,
  pipeline: Pipeline,
) {
  let block_reward_info = {
    height: height,
    miner_reward: miner_reward,
    governance_reward: governance_reward,
    reserve_reward: reserve_reward,
    yield_reward: yield_reward,
  };

  const block_reward_info_json = JSON.stringify(block_reward_info);
  pipeline.hset("block_rewards", height, block_reward_info_json);

  // increment totals - doing at an upper level now to avoid multiple calls
  // pipeline.hincrbyfloat("totals", "miner_reward", miner_reward);
  // pipeline.hincrbyfloat("totals", "governance_reward", governance_reward);
  // pipeline.hincrbyfloat("totals", "reserve_reward", reserve_reward);
  // pipeline.hincrbyfloat("totals", "yield_reward", yield_reward);
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
  }
}

async function processTx(hash: string, verbose_logs: boolean, pipeline: Pipeline): Promise<{ incr_totals: IncrTotals; tx_info?: TxInfoType }> {
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

  if (!tx_json_string) {
    console.error("No valid transaction JSON data found.");
    return { incr_totals };
  }

  const tx_json = JSON.parse(tx_json_string);
  const { amount_burnt, amount_minted, vin, vout, rct_signatures, pricing_record_height } = tx_json;

  if (!(amount_burnt && amount_minted)) {
    const tx_amount = vout[0]?.amount || 0;
    if (tx_amount == 0) {
      if (verbose_logs) console.log("\t\tSKIP -> Not a conversion transaction or block reward transaction");
      return { incr_totals };
    }

    // Miner reward transaction!
    const miner_reward = tx_amount * DEATOMIZE;
    let governance_reward = 0;
    let reserve_reward = 0;
    let yield_reward = 0;

    // Prior to HF V6 / Version 2 - ZSD Yield.
    if (block_height < VERSION_2_HF_V6_BLOCK_HEIGHT) {

      // v0 -> v1 DJED Implementation
      let log_message = "\tBlock reward transaction!";
      // Miner Reward = 95%
      // Governance Reward = 5%
      governance_reward = vout[1]?.amount * DEATOMIZE;

      if (block_height >= HF_V1_BLOCK_HEIGHT) {
        log_message += " v1 DJED Implementation";
        // Miner Reward = 95% -> 75%
        // Governance Reward = 5%
        // Reserve Reward = 0% -> 20% (added)
        // Yield Reward = 0% (not implemented)
        const total_reward = miner_reward / 0.75;
        reserve_reward = total_reward * 0.2;
      }
      if (verbose_logs) console.log(log_message);
      await saveBlockRewardInfo(block_height, miner_reward, governance_reward, reserve_reward, yield_reward, pipeline);
    }

    // Post HF V6 / Version 2 - ZSD Yield.
    if (block_height >= VERSION_2_HF_V6_BLOCK_HEIGHT) {
      // Miner reward = 75% -> 65%
      // Reserve Reward = 20% -> 30%
      // Governance Reward = 5% -> 0% (removed)
      // Yield Reward = 0% -> 5% (added)

      const total_reward = miner_reward / 0.65;
      reserve_reward = total_reward * 0.3;
      yield_reward = total_reward * 0.05;

      if (verbose_logs) console.log("\tBlock reward transaction! v2 ZSD Yield");
      await saveBlockRewardInfo(block_height, miner_reward, governance_reward, reserve_reward, yield_reward, pipeline);
    }

    // update incr_totals
    incr_totals.miner_reward += miner_reward;
    incr_totals.governance_reward += governance_reward;
    incr_totals.reserve_reward += reserve_reward;
    incr_totals.yield_reward += yield_reward;
    return { incr_totals };
  }

  // Conversion transaction!
  if (verbose_logs) console.log("\tConversion transaction!");
  // Add the hash to the txs_by_block for the specific block height
  const txsByBlockHashes = await redis.hget("txs_by_block", block_height.toString());
  let hashes: string[] = txsByBlockHashes ? JSON.parse(txsByBlockHashes) : [];
  hashes.push(hash);
  pipeline.hset("txs_by_block", block_height.toString(), JSON.stringify(hashes));

  const input_asset_type = vin[0]?.key?.asset_type || undefined;
  const output_asset_types = vout.map((v: any) => v?.target?.tagged_key?.asset_type).filter(Boolean);

  let conversion_type = determineConversionType(input_asset_type, output_asset_types);

  if (conversion_type === "na") {
    console.log("Error - Can't determine conversion type");
    return { incr_totals };
  }

  // pipeline.hincrbyfloat("totals", "conversion_transactions", 1);
  incr_totals.conversion_transactions += 1;

  if (pricing_record_height === 0) {
    console.error("Tx - REALLY MESSED UP DATA? - pricing_record_height is 0 from await readTx() and should be here in conversion (or miner?) transaction");
    // Print details for debugging
    console.error("Transaction hash:", hash);
    console.error("response_data:", response_data);
    console.error("Transaction JSON:", tx_json);
    return { incr_totals };
  }

  const relevant_pr = await getRedisPricingRecord(pricing_record_height);

  if (!relevant_pr) {
    console.log(`No pricing record found for height: ${pricing_record_height}`);
    return { incr_totals };
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

  switch (conversion_type) {
    case "mint_stable":
      conversion_rate = Math.max(spot, moving_average);
      from_asset = "ZEPH";
      from_amount = amount_burnt * DEATOMIZE;
      to_asset = "ZEPHUSD";
      to_amount = amount_minted * DEATOMIZE;

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
      to_asset = "ZEPH";
      to_amount = amount_minted * DEATOMIZE;

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
      to_asset = "ZEPHRSV";
      to_amount = amount_minted * DEATOMIZE;

      const mint_reserve_conversion_fee = block_height < ARTEMIS_HF_V5_BLOCK_HEIGHT ? 0 : 0.01; // 0% -> 1%

      conversion_fee_asset = to_asset;
      conversion_fee_amount = (to_amount / 1 - mint_reserve_conversion_fee) * mint_reserve_conversion_fee;

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
      to_asset = "ZEPH";
      to_amount = amount_minted * DEATOMIZE;

      const redeem_reserve_conversion_fee = block_height < ARTEMIS_HF_V5_BLOCK_HEIGHT ? 0.02 : 0.01; // 2% -> 1%

      conversion_fee_asset = to_asset;
      conversion_fee_amount = (to_amount / 1 - redeem_reserve_conversion_fee) * redeem_reserve_conversion_fee;
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
      to_asset = "ZYIELD";
      to_amount = amount_minted * DEATOMIZE;

      const mint_yield_conversion_fee = 0.001; // 0.1%

      conversion_fee_asset = to_asset;
      conversion_fee_amount = (to_amount / 1 - mint_yield_conversion_fee) * mint_yield_conversion_fee;
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
      to_asset = "ZEPHUSD";
      to_amount = amount_minted * DEATOMIZE;

      const redeem_yield_conversion_fee = 0.001; // 0.1%

      conversion_fee_asset = to_asset;
      conversion_fee_amount = (to_amount / 1 - redeem_yield_conversion_fee) * redeem_yield_conversion_fee;
      tx_fee_asset = from_asset;

      // pipeline.hincrbyfloat("totals", "redeem_yield_count", 1);
      // pipeline.hincrbyfloat("totals", "redeem_yield_volume", to_amount); // ZEPHUSD
      // pipeline.hincrbyfloat("totals", "fees_zephusd_yield", conversion_fee_amount); // effective loss in ZEPHUSD for redeeming ZYIELD
      incr_totals.redeem_yield_count += 1;
      incr_totals.redeem_yield_volume += to_amount; // ZEPHUSD
      incr_totals.fees_zephusd_yield += conversion_fee_amount; // effective loss in ZEPHUSD for redeeming ZYIELD
      break;

  }

  const tx_fee_amount = rct_signatures.txnFee * DEATOMIZE;

  const tx_info = {
    hash,
    block_height,
    block_timestamp,
    conversion_type,
    conversion_rate,
    from_asset,
    from_amount,
    to_asset,
    to_amount,
    conversion_fee_asset,
    conversion_fee_amount,
    tx_fee_asset,
    tx_fee_amount,
  };

  if (verbose_logs) console.log(tx_info);

  return { incr_totals, tx_info };
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
  const redisHeight = await getRedisHeight();

  let startingHeight = Math.max(redisHeight + 1, hfHeight);
  if (reset) {
    startingHeight = hfHeight;
    // clear totals
    await redis.del("totals");
    // clear txs
    await redis.del("txs");
    // clear txs_by_block
    await redis.del("txs_by_block");
    // clear block_rewards
    await redis.del("block_rewards");
    // reset scanner height
    await setRedisHeight(hfHeight);
  }

  console.log("Fired tx scanner...");
  console.log(`Starting height: ${startingHeight} | Ending height: ${rpcHeight - 1}`);

  let verbose_logs = false;
  if (rpcHeight - startingHeight > 1000) {
    console.log("This is a large scan, verbose logs are disabled.");
    verbose_logs = false;
  }

  const total_of_total_increments: IncrTotals = blankIncrTotals();
  const progressStep = Math.max(1, Math.floor((rpcHeight - 1) / 100));
  const pipeline = redis.pipeline() as Pipeline;

  for (let height = startingHeight; height <= rpcHeight - 1; height++) {
    const block: any = await getBlock(height);
    if (!block) {
      console.log(`${height}/${rpcHeight - 1} - No block info found, exiting try later`);
      await setRedisHeight(height);
      return;
    }


    if (height % progressStep === 0 || height === (rpcHeight - 1) - 1) {
      // const percent = ((height + 1) / (rpcHeight - 1) * 100).toFixed(2);
      const percentComplete = (((height - startingHeight) / (rpcHeight - startingHeight)) * 100).toFixed(2);
      console.log(`TXs SCANNING BLOCK: [${height + 1}/${(rpcHeight - 1)}] Processed (${percentComplete}%)`);
    }

    // console.log(`TXs SCANNING BLOCK: ${height}/${rpcHeight - 1} \t | ${percentComplete}%`);

    const txs = block.result.tx_hashes;
    const miner_tx = block.result.miner_tx_hash;

    const { incr_totals: miner_tx_incr_totals, tx_info: undefined } = await processTx(miner_tx, verbose_logs, pipeline);

    // if (miner_tx_incr_totals) {
    //   for (const key in miner_tx_incr_totals) total_of_total_increments[key] += miner_tx_incr_totals[key];
    // }

    for (const key of Object.keys(miner_tx_incr_totals) as (keyof IncrTotals)[]) {
      total_of_total_increments[key] += miner_tx_incr_totals[key];
    }


    if (!txs) {
      if (verbose_logs) console.log(`\t - No Additional txs`);
      await setRedisHeight(height);
      continue;
    }
    for (const hash of txs) {
      const { incr_totals, tx_info } = await processTx(hash, verbose_logs, pipeline);
      if (incr_totals) {
        // increment totals
        for (const key of Object.keys(incr_totals) as (keyof IncrTotals)[]) {
          total_of_total_increments[key] += incr_totals[key];
        }
      }
      if (tx_info) {
        pipeline.hset("txs", hash, JSON.stringify(tx_info));
      }
    }

    // await setRedisHeight(height);

    // update scan height
    // pipeline.set("height_txs", height);

    // Print out count of pipeline commands
  }

  // add the increment totals to pipeline
  for (const key of Object.keys(total_of_total_increments) as (keyof IncrTotals)[]) {
    pipeline.hincrbyfloat("totals", key, total_of_total_increments[key]);
  }

  const pipelineCommandCount = pipeline.length;
  console.log(`Pipeline command count: ${pipelineCommandCount}`);

  // EXECUTE ALL REDIS COMMANDS IN BATCH
  await pipeline.exec();
  await setRedisHeight(rpcHeight - 1);
}



// (async () => {
//   await scanTransactions();
// })();
