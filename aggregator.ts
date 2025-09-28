// Take all data and aggregate into a single redis key done by block, hourly and daily.

import e from "express";
import redis from "./redis";
import { AggregatedData, ProtocolStats, getCurrentBlockHeight, getRedisBlockRewardInfo, getRedisHeight, getRedisPricingRecord, getRedisTimestampDaily, getRedisTimestampHourly, getRedisTransaction, setRedisHeight } from "./utils";
// const DEATOMIZE = 10 ** -12;
const HF_VERSION_1_HEIGHT = 89300;
const HF_VERSION_1_TIMESTAMP = 1696152427;

const ARTEMIS_HF_V5_BLOCK_HEIGHT = 295000;

const VERSION_2_HF_V6_BLOCK_HEIGHT = 360000;
const VERSION_2_HF_V6_TIMESTAMP = 1728817200; // ESTIMATED. TO BE UPDATED?
const VERSION_2_3_0_HF_V11_BLOCK_HEIGHT = 536000; // Post Audit, asset type changes.

// Function to get transaction hashes by block height
async function getRedisTransactionHashesByBlock(blockHeight: number): Promise<string[]> {
  try {
    // Fetch the JSON string of transaction hashes for the given block height
    const txHashesJson = await redis.hget("txs_by_block", blockHeight.toString());
    if (!txHashesJson) {
      // console.log(`No transactions found for block ${blockHeight}.`);
      return [];
    }

    // Parse the JSON string to get the array of transaction hashes
    const txHashes = JSON.parse(txHashesJson);
    return txHashes;
  } catch (error) {
    console.error("Error fetching transaction hashes by block:", error);
    return [];
  }
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

  console.log(`\tAggregating from block: ${height_to_process} to ${current_height_prs - 1}`);

  for (let i = height_to_process; i <= current_height_prs - 1; i++) {
    await aggregateBlock(i);
  }

  // get pr for current_height_prs
  const current_pr = await getRedisPricingRecord(current_height_prs);
  const timestamp_hourly = await getRedisTimestampHourly();
  const timestamp_daily = await getRedisTimestampDaily();

  // by hour
  await aggregateByTimestamp(Math.max(timestamp_hourly, HF_VERSION_1_TIMESTAMP), current_pr.timestamp, "hourly");
  // by day
  await aggregateByTimestamp(Math.max(timestamp_daily, HF_VERSION_1_TIMESTAMP), current_pr.timestamp, "daily");

  console.log(`Finished aggregation`);
}

async function aggregateBlock(height_to_process: number) {
  // const height_to_process = Math.max(height + 1, HF_VERSION_1_HEIGHT);

  console.log(`\tAggregating block: ${height_to_process}`);

  const pr = await getRedisPricingRecord(height_to_process);
  if (!pr) {
    console.log("No pricing record found for height: ", height_to_process);
    return;
  }
  const bri = await getRedisBlockRewardInfo(height_to_process);
  if (!bri) {
    console.log("No block reward info found for height: ", height_to_process);
    return;
  }

  // Fetch previous block's data for initialization
  const prevBlockDataJson = await redis.hget("protocol_stats", (height_to_process - 1).toString());
  let prevBlockData = prevBlockDataJson ? JSON.parse(prevBlockDataJson) : {};

  // initialize the block data
  let blockData: ProtocolStats = {
    block_height: height_to_process,
    block_timestamp: pr ? pr.timestamp : null, // Get timestamp from pricing record
    spot: pr ? pr.spot : 0, // Get spot from pricing record
    moving_average: pr ? pr.moving_average : 0, // Get moving average from pricing record
    reserve: pr ? pr.reserve : 0, // Get reserve from pricing record
    reserve_ma: pr ? pr.reserve_ma : 0, // Get reserve moving average from pricing record
    stable: pr ? pr.stable : 0, // Get stable from pricing record
    stable_ma: pr ? pr.stable_ma : 0, // Get stable moving average from pricing record
    yield_price: pr ? pr.yield_price : 0, // Get yield price from pricing record
    zeph_in_reserve: prevBlockData.zeph_in_reserve || 0, // Initialize from previous block or 0
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

  // console.log(`pr`);
  // console.log(pr);
  // console.log(`\n\n`);
  // console.log(`bri`);
  // console.log(bri);
  // console.log(`\n\n`);

  const block_txs = await getRedisTransactionHashesByBlock(height_to_process);
  // console.log(`block_txs`);
  // console.log(block_txs);
  blockData.zeph_in_reserve += bri.reserve_reward;

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
    (bri?.miner_reward ?? 0) +
    (bri?.governance_reward ?? 0) +
    (bri?.reserve_reward ?? 0) +
    (bri?.yield_reward ?? 0);

  if (block_txs.length != 0) {
    console.log(`\tFound Conversion Transactions (${block_txs.length}) in block: ${height_to_process} - Processing...`);
    let failureCount = 0;
    let failureTxs: string[] = [];


    for (const tx_hash of block_txs) {
      try {
        const tx: Transaction = await getRedisTransaction(tx_hash);
        switch (tx.conversion_type) {
          case "mint_stable":
            blockData.conversion_transactions_count += 1;
            // to = ZEPHUSD (ZSD)
            // from = ZEPH
            blockData.mint_stable_count += 1;
            blockData.mint_stable_volume += tx.to_amount;
            blockData.fees_zephusd += tx.conversion_fee_amount;
            blockData.zephusd_circ += tx.to_amount;
            blockData.zeph_in_reserve += tx.from_amount;
            break;
          case "redeem_stable":
            blockData.conversion_transactions_count += 1;
            // to = ZEPH
            // from = ZEPHUSD (ZSD)
            blockData.redeem_stable_count += 1;
            blockData.redeem_stable_volume += tx.from_amount;
            blockData.fees_zeph += tx.conversion_fee_amount;
            blockData.zeph_in_reserve -= tx.to_amount;
            blockData.zephusd_circ -= tx.from_amount;
            break;
          case "mint_reserve":
            blockData.conversion_transactions_count += 1;
            // to = ZEPHRSV (ZRS)
            // from = ZEPH
            blockData.mint_reserve_count += 1;
            blockData.mint_reserve_volume += tx.to_amount;
            blockData.zeph_in_reserve += tx.from_amount;
            blockData.zephrsv_circ += tx.to_amount;
            blockData.fees_zephrsv += tx.conversion_fee_amount
            break;
          case "redeem_reserve":
            blockData.conversion_transactions_count += 1;
            // to = ZEPH
            // from = ZEPHRSV (ZRS)
            blockData.redeem_reserve_count += 1;
            blockData.redeem_reserve_volume += tx.from_amount;
            blockData.zeph_in_reserve -= tx.to_amount;
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
        console.error(`Error processing conversion transactions for block ${height_to_process}: ${error}`);
        failureCount++;
        failureTxs.push(tx_hash);
      }
    }

    if (failureCount > 0) {
      console.log(`Failed to process ${failureCount} conversion transactions for block ${height_to_process}`);
      console.log(failureTxs);
    }
  }

  // Calculate additional stats
  blockData.assets = blockData.zeph_in_reserve * blockData.spot;
  blockData.assets_ma = blockData.zeph_in_reserve * blockData.moving_average;
  blockData.liabilities = blockData.zephusd_circ;
  blockData.equity = blockData.assets - blockData.liabilities;
  blockData.equity_ma = blockData.assets_ma - blockData.liabilities;

  // Calculate reserve ratio
  blockData.reserve_ratio = blockData.liabilities > 0 ? blockData.assets / blockData.liabilities : 0;
  blockData.reserve_ratio_ma = blockData.liabilities > 0 ? blockData.assets_ma / blockData.liabilities : 0;

  // Calculate ZSD Yield Reserve Accrual and ZSD Minted this block
  if (height_to_process >= VERSION_2_HF_V6_BLOCK_HEIGHT) {
    if (blockData.reserve_ratio >= 2 && blockData.reserve_ratio_ma >= 2) {
      const yield_reward_zeph = bri.yield_reward;
      const zsd_auto_minted = blockData.spot * yield_reward_zeph;
      blockData.zsd_minted_for_yield = zsd_auto_minted;
      blockData.zsd_accrued_in_yield_reserve_from_yield_reward += zsd_auto_minted;
      blockData.zsd_in_yield_reserve += zsd_auto_minted;
      //add to circ
      blockData.zephusd_circ += zsd_auto_minted;
    }
  }

  await redis.hset("protocol_stats", height_to_process.toString(), JSON.stringify(blockData));
  // console.log(`Protocol stats aggregated for block ${height_to_process}`);
  // console.log(blockData);
  // console.log(`\n\n`);
  setRedisHeight(height_to_process);
}

async function aggregateByTimestamp(startTimestamp: number, endingTimestamp: number, windowType = "hourly" || "daily") {
  console.log(`\tAggregating by timestamp: ${startTimestamp} to ${endingTimestamp} for ${windowType}`);
  let timestampWindow = windowType === "hourly" ? 3600 : 86400;
  // is there more than
  const diff = endingTimestamp - startTimestamp;
  if (diff < timestampWindow) {
    return;
  }

  // Calculate the total number of windows
  const totalWindows = Math.ceil(diff / timestampWindow);
  // get all protocol stats between start and end timestamp
  // aggregate into a single key "protocol_stats_hourly" as a sorted set
  // store in redis

  const protocolStats = await redis.hgetall("protocol_stats");

  if (!protocolStats) {
    console.log("No protocol stats available");
    return;
  }

  let windowIndex = 0; // Track the current window index

  // Loop through the time range in x increments
  for (
    let windowStart = startTimestamp;
    windowStart < endingTimestamp - timestampWindow;
    windowStart += timestampWindow
  ) {
    const windowEnd = windowStart + timestampWindow;
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
      aggregatedData.zsd_in_yield_reserve_close = protocolStatsWindow[protocolStatsWindow.length - 1].zsd_in_yield_reserve ?? 0;

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

        aggregatedData.zsd_in_yield_reserve_high = Math.max(aggregatedData.zsd_in_yield_reserve_high, blockData.zsd_in_yield_reserve);
        aggregatedData.zsd_in_yield_reserve_low = Math.min(aggregatedData.zsd_in_yield_reserve_low, blockData.zsd_in_yield_reserve);

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
        await redis.zadd("protocol_stats_hourly", windowStart, JSON.stringify(aggregatedData));
        console.log(`Hourly stats aggregated for window starting at ${windowStart}`);
        // update redis timestamp_aggregator_hourly
        await redis.set("timestamp_aggregator_hourly", windowEnd);
      } else if (windowType === "daily") {
        await redis.zadd("protocol_stats_daily", windowStart, JSON.stringify(aggregatedData));
        console.log(`Daily stats aggregated for window starting at ${windowStart}`);
        // update redis timestamp_aggregator_daily
        await redis.set("timestamp_aggregator_daily", windowEnd);
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
