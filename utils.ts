import redis from "./redis";
const RPC_URL = "http://127.0.0.1:17767";
const HEADERS = {
  "Content-Type": "application/json",
};

export async function getCurrentBlockHeight() {
  const response = await fetch(`${RPC_URL}/get_height`, {
    method: "POST",
    headers: HEADERS,
  });

  const responseData = await response.json();
  if (responseData && "height" in responseData) {
    return responseData.height;
  } else {
    return 0;
  }
}

export async function getBlock(height: number) {
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
}

export async function readTx(hash: string) {
  const response = await fetch(`${RPC_URL}/get_transactions`, {
    method: "POST",
    headers: HEADERS,
    body: JSON.stringify({
      txs_hashes: [hash],
      decode_as_json: true,
    }),
  });

  return await response.json();
}

export async function getTotalsFromRedis() {
  const totals = await redis.hgetall("totals");
  if (!totals) {
    return null;
  }
  return totals;
}

// Example usage

// (async () => {
//   const height = await getCurrentBlockHeight();
//   console.log("Current Block Height:", height);
// })();
