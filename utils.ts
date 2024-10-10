import fetch from "node-fetch";
import { Agent } from "http"; // or 'https' for secure connections

// Create a global agent
const agent = new Agent({ keepAlive: true });
import redis from "./redis";
const RPC_URL = "http://127.0.0.1:17767";
const HEADERS = {
  "Content-Type": "application/json",
};

export async function getCurrentBlockHeight(): Promise<number> {
  try {
    const response = await fetch(`${RPC_URL}/get_height`, {
      method: "POST",
      headers: HEADERS,
    });

    const responseData = await response.json();

    // Check if responseData is an object and has the 'height' property
    if (responseData && typeof responseData === "object" && "height" in responseData) {
      if (typeof responseData.height === "number") {
        return responseData.height;
      } else {
        return 0;
      }
    } else {
      return 0;
    }
  } catch (e) {
    console.log(e);
    return 0;
  }
}

interface GetBlockResponse {
  id: string;
  jsonrpc: string;
  result: {
    blob: string;
    block_header: {
      block_size: number;
      block_weight: number;
      cumulative_difficulty: number;
      cumulative_difficulty_top64: number;
      depth: number;
      difficulty: number;
      difficulty_top64: number;
      hash: string;
      height: number;
      long_term_weight: number;
      major_version: number;
      miner_tx_hash: string;
      minor_version: number;
      nonce: number;
      num_txes: number;
      orphan_status: boolean;
      pow_hash: string;
      prev_hash: string;
      pricing_record: {
        moving_average: number;
        reserve: number;
        reserve_ma: number;
        signature: string;
        spot: number;
        stable: number;
        stable_ma: number;
        yield_price?: number;
        timestamp: number;
      };
      reward: number;
      timestamp: number;
      wide_cumulative_difficulty: string;
      wide_difficulty: string;
    };
    credits: number;
    json: string;
    miner_tx_hash: string;
    status: string;
    top_hash: string;
    tx_hashes: string[];
    untrusted: boolean;
  };
}

export async function getBlock(height: number) {
  try {
    const response = await fetch(`${RPC_URL}/json_rpc`, {
      method: "POST",
      headers: HEADERS,
      body: JSON.stringify({
        jsonrpc: "2.0",
        id: "0",
        method: "get_block",
        params: { height },
      }),
    });

    return await response.json() as GetBlockResponse;
  } catch (e) {
    console.log(e);
    return;
  }
}

export async function getPricingRecordFromBlock(height: number) {
  const blockData = await getBlock(height);
  if (!blockData) {
    return;
  }

  const pricingRecord = blockData.result.block_header.pricing_record;
  return pricingRecord;
}

export async function readTx(hash: string) {
  try {
    const response = await fetch(`${RPC_URL}/get_transactions`, {
      method: "POST",
      headers: HEADERS,
      body: JSON.stringify({
        txs_hashes: [hash],
        decode_as_json: true,
      }),
      agent: agent, // Use the agent in your fetch request
    });

    if (!response.ok) {
      console.error(`HTTP error! status: ${response.status}`);
      return null;
    }

    return await response.json();
  } catch (error) {
    console.error("Error in fetching transaction:", error);
    return null;
  }
}

export async function getTotalsFromRedis() {
  const totals = await redis.hgetall("totals");
  if (!totals) {
    return null;
  }
  return totals;
}

export async function getProtocolStatsFromRedis(scale: "block" | "hour" | "day", from?: string, to?: string) {
  let redisKey = "";
  switch (scale) {
    case "block":
      redisKey = "protocol_stats";
      let start = from ? parseInt(from) : 0;
      let end = to ? parseInt(to) : Number(await redis.get("height_aggregator"));
      let blockData = [];
      for (let i = start; i <= end; i++) {
        const statsJson = await redis.hget(redisKey, i.toString());
        if (statsJson) {
          const stats = JSON.parse(statsJson);
          blockData.push({ block_height: i, data: stats });
        }
      }
      return blockData;

    case "hour":
    case "day":
      redisKey = scale === "hour" ? "protocol_stats_hourly" : "protocol_stats_daily";
      let startScore = from ? parseInt(from) : "-inf";
      let endScore = to ? parseInt(to) : "+inf";
      // console.log(`calling redis with: ${redisKey}, ${startScore}, ${endScore}`);
      let results = await redis.zrangebyscore(redisKey, startScore, endScore, "WITHSCORES");
      return formatZrangeResults(results);
  }
}

function formatZrangeResults(results: any) {
  let formattedResults = [];
  for (let i = 0; i < results.length; i += 2) {
    formattedResults.push({ timestamp: results[i + 1], data: JSON.parse(results[i]) });
  }
  return formattedResults;
}

export interface ProtocolStats {
  // Prices
  spot_open: number;
  spot_close: number;
  spot_high: number;
  spot_low: number;
  moving_average_open: number;
  moving_average_close: number;
  moving_average_high: number;
  moving_average_low: number;
  reserve_open: number;
  reserve_close: number;
  reserve_high: number;
  reserve_low: number;
  reserve_ma_open: number;
  reserve_ma_close: number;
  reserve_ma_high: number;
  reserve_ma_low: number;
  stable_open: number;
  stable_close: number;
  stable_high: number;
  stable_low: number;
  stable_ma_open: number;
  stable_ma_close: number;
  stable_ma_high: number;
  stable_ma_low: number;
  zyield_price_open: number;
  zyield_price_close: number;
  zyield_price_high: number;
  zyield_price_low: number;

  // Circulating Reserve Amounts
  zeph_in_reserve_open: number;
  zeph_in_reserve_close: number;
  zeph_in_reserve_high: number;
  zeph_in_reserve_low: number;
  zsd_in_yield_reserve_open: number;
  zsd_in_yield_reserve_close: number;
  zsd_in_yield_reserve_high: number;
  zsd_in_yield_reserve_low: number;

  // Circulating Supply
  zeph_circ_open: number;
  zeph_circ_close: number;
  zeph_circ_high: number;
  zeph_circ_low: number;
  zephusd_circ_open: number;
  zephusd_circ_close: number;
  zephusd_circ_high: number;
  zephusd_circ_low: number;
  zephrsv_circ_open: number;
  zephrsv_circ_close: number;
  zephrsv_circ_high: number;
  zephrsv_circ_low: number;
  zyield_circ_open: number;
  zyield_circ_close: number;
  zyield_circ_high: number;
  zyield_circ_low: number;

  // Djed Mechanics Stats
  assets_open: number;
  assets_close: number;
  assets_high: number;
  assets_low: number;
  assets_ma_open: number;
  assets_ma_close: number;
  assets_ma_high: number;
  assets_ma_low: number;
  liabilities_open: number;
  liabilities_close: number;
  liabilities_high: number;
  liabilities_low: number;
  equity_open: number;
  equity_close: number;
  equity_high: number;
  equity_low: number;
  equity_ma_open: number;
  equity_ma_close: number;
  equity_ma_high: number;
  equity_ma_low: number;
  reserve_ratio_open: number;
  reserve_ratio_close: number;
  reserve_ratio_high: number;
  reserve_ratio_low: number;
  reserve_ratio_ma_open: number;
  reserve_ratio_ma_close: number;
  reserve_ratio_ma_high: number;
  reserve_ratio_ma_low: number;

  // Conversion Stats
  conversion_transactions_count: number;
  yield_conversion_transactions_count: number;
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
}

export async function getRedisHeight() {
  const height = await redis.get("height_aggregator");
  if (!height) {
    return 0;
  }
  return parseInt(height);
}

export async function getRedisTimestampHourly() {
  const height = await redis.get("timestamp_aggregator_hourly");
  if (!height) {
    return 0;
  }
  return parseInt(height);
}

export async function getRedisTimestampDaily() {
  const height = await redis.get("timestamp_aggregator_daily");
  if (!height) {
    return 0;
  }
  return parseInt(height);
}

export async function setRedisHeight(height: number) {
  await redis.set("height_aggregator", height);
}

export async function getRedisPricingRecord(height: number) {
  const pr = await redis.hget("pricing_records", height.toString());
  if (!pr) {
    return null;
  }
  return JSON.parse(pr);
}

export async function getRedisBlockRewardInfo(height: number) {
  const bri = await redis.hget("block_rewards", height.toString());
  if (!bri) {
    return null;
  }
  return JSON.parse(bri);
}

export async function getRedisTransaction(hash: string) {
  const txs = await redis.hget("txs", hash);
  if (!txs) {
    return null;
  }
  return JSON.parse(txs);
}


// Example usage

// (async () => {
//   const height = await getCurrentBlockHeight();
//   console.log("Current Block Height:", height);
// })();
