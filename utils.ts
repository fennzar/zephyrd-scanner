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

    return await response.json();
  } catch (e) {
    console.log(e);
    return;
  }
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

// Example usage

// (async () => {
//   const height = await getCurrentBlockHeight();
//   console.log("Current Block Height:", height);
// })();
