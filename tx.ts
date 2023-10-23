import { getCurrentBlockHeight, getBlock, readTx } from "./utils";
import redis from "./redis";

const DEATOMIZE = 10 ** -12;

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
  reserve_reward: number
) {
  let block_reward_info = {
    height: height,
    miner_reward: miner_reward,
    governance_reward: governance_reward,
    reserve_reward: reserve_reward,
  };

  const block_reward_info_json = JSON.stringify(block_reward_info);
  await redis.hset("block_rewards", height, block_reward_info_json);

  // increment totals
  await redis.hincrbyfloat("totals", "miner_reward", miner_reward);
  await redis.hincrbyfloat("totals", "governance_reward", governance_reward);
  await redis.hincrbyfloat("totals", "reserve_reward", reserve_reward);
}

async function processTx(hash: string) {
  console.log(`\tProcessing tx: ${hash}`);
  const response_data = await readTx(hash);
  if (!response_data) {
    console.error("Failed to retrieve transaction data.");
    return;
  }

  const { txs: [tx_data = {}] = [] } = response_data;
  const { as_json: tx_json_string } = tx_data;
  const { block_height: block_height } = tx_data;
  const { block_timestamp: block_timestamp } = tx_data;

  if (!tx_json_string) {
    console.error("No valid transaction JSON data found.");
    return;
  }

  const tx_json = JSON.parse(tx_json_string);
  const { amount_burnt, amount_minted, vin, vout, rct_signatures, pricing_record_height } = tx_json;

  if (!(amount_burnt && amount_minted)) {
    const tx_amount = vout[0]?.amount || 0;
    if (tx_amount == 0) {
      console.log("\t\tSKIP -> Not a conversion transaction or block reward transaction");
      return;
    }

    // Miner reward transaction!
    const miner_reward = tx_amount * DEATOMIZE;
    const governance_reward = vout[1]?.amount * DEATOMIZE;
    let reserve_reward = 0;
    if (block_height >= 89300) {
      // HF block
      reserve_reward = (miner_reward / 0.75) * 0.2;
    }
    console.log("\tBlock reward transaction!");
    await saveBlockRewardInfo(block_height, miner_reward, governance_reward, reserve_reward);
    return;
  }

  // Conversion transaction!
  console.log("\tConversion transaction!");
  await redis.hincrbyfloat("totals", "conversion_transactions", 1);

  const input_asset_type = vin[0]?.key?.asset_type || undefined;
  const output_asset_types = vout.map((v: any) => v?.target?.tagged_key?.asset_type).filter(Boolean);

  let conversion_type = determineConversionType(input_asset_type, output_asset_types);

  if (conversion_type === "na") {
    console.log("Error - Can't determine conversion type");
    return;
  }

  const relevant_pr = await getRedisPricingRecord(pricing_record_height);

  if (!relevant_pr) {
    console.log(`No pricing record found for height: ${pricing_record_height}`);
    return;
  }

  const { spot, moving_average, reserve, reserve_ma, stable, stable_ma } = relevant_pr;

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

      conversion_fee_asset = to_asset;
      conversion_fee_amount = (to_amount / 0.98) * 0.02;
      tx_fee_asset = from_asset;
      await redis.hincrbyfloat("totals", "mint_stable_count", 1);
      await redis.hincrbyfloat("totals", "mint_stable_volume", to_amount);
      await redis.hincrbyfloat("totals", "fees_zephusd", conversion_fee_amount);
      break;

    case "redeem_stable":
      conversion_rate = Math.min(spot, moving_average);
      from_asset = "ZEPHUSD";
      from_amount = amount_burnt * DEATOMIZE;
      to_asset = "ZEPH";
      to_amount = amount_minted * DEATOMIZE;

      conversion_fee_asset = to_asset;
      conversion_fee_amount = (to_amount / 0.98) * 0.02;
      tx_fee_asset = from_asset;
      await redis.hincrbyfloat("totals", "redeem_stable_count", 1);
      await redis.hincrbyfloat("totals", "redeem_stable_volume", to_amount);
      await redis.hincrbyfloat("totals", "fees_zeph", conversion_fee_amount);
      break;

    case "mint_reserve":
      conversion_rate = Math.max(reserve, reserve_ma);
      from_asset = "ZEPH";
      from_amount = amount_burnt * DEATOMIZE;
      to_asset = "ZEPHRSV";
      to_amount = amount_minted * DEATOMIZE;

      conversion_fee_asset = "N/A";
      conversion_fee_amount = 0;
      tx_fee_asset = from_asset;
      await redis.hincrbyfloat("totals", "mint_reserve_count", 1);
      await redis.hincrbyfloat("totals", "mint_reserve_volume", to_amount);
      break;

    case "redeem_reserve":
      conversion_rate = Math.min(reserve, reserve_ma);
      from_asset = "ZEPHRSV";
      from_amount = amount_burnt * DEATOMIZE;
      to_asset = "ZEPH";
      to_amount = amount_minted * DEATOMIZE;

      conversion_fee_asset = to_asset;
      conversion_fee_amount = (to_amount / 0.98) * 0.02;
      tx_fee_asset = from_asset;
      await redis.hincrbyfloat("totals", "redeem_reserve_count", 1);
      await redis.hincrbyfloat("totals", "redeem_reserve_volume", to_amount);
      await redis.hincrbyfloat("totals", "fees_zeph", conversion_fee_amount);
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

  console.log(tx_info);

  return tx_info;
}

function determineConversionType(input: string, outputs: string[]): string {
  if (input === "ZEPH" && outputs.includes("ZEPHUSD")) return "mint_stable";
  if (input === "ZEPHUSD" && outputs.includes("ZEPH")) return "redeem_stable";
  if (input === "ZEPH" && outputs.includes("ZEPHRSV")) return "mint_reserve";
  if (input === "ZEPHRSV" && outputs.includes("ZEPH")) return "redeem_reserve";
  return "na";
}

export async function scanTransactions() {
  const hfHeight = 89300; // if we just want to scan from the v1.0.0 HF block
  const rpcHeight = await getCurrentBlockHeight();
  const redisHeight = await getRedisHeight();

  const startingHeight = Math.max(redisHeight + 1, hfHeight);

  console.log("Fired tx scanner...");
  console.log(`Starting height: ${startingHeight} | Ending height: ${rpcHeight - 1}`);

  for (let height = startingHeight; height <= rpcHeight - 1; height++) {
    const block = await getBlock(height);
    if (!block) {
      console.log(`${height}/${rpcHeight - 1} - No block`);
      await setRedisHeight(height);
      continue;
    }
    console.log(`SCANNING BLOCK: ${height}/${rpcHeight - 1}`);

    const txs = block.result.tx_hashes;
    const miner_tx = block.result.miner_tx_hash;
    await processTx(miner_tx);
    if (!txs) {
      console.log(`\t - No Additional txs`);
      await setRedisHeight(height);
      continue;
    }
    for (const hash of txs) {
      const tx_info = await processTx(hash);
      if (tx_info) {
        await redis.hset("txs", hash, JSON.stringify(tx_info));
      }
    }
    await setRedisHeight(height);
  }
}

// (async () => {
//   await scanTransactions();
// })();
