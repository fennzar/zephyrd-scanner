import dotenv from "dotenv";
import fetch from "node-fetch";

import redis from "../redis";
import { saveReserveSnapshotToRedis } from "../utils";

dotenv.config();

const RPC_URL = process.env.ZEPHYR_RPC_URL ?? "http://127.0.0.1:17767";
const POLL_INTERVAL_MS = Number(process.env.RESERVE_SNAPSHOT_POLL_INTERVAL_MS ?? "5000");
const RESERVE_SNAPSHOT_REDIS_KEY = process.env.RESERVE_SNAPSHOT_REDIS_KEY ?? "reserve_snapshots";

interface ReserveInfoResponse {
  id: string;
  jsonrpc: string;
  result?: {
    height: number;
  } & Record<string, unknown>;
  error?: { code: number; message: string };
}

async function fetchReserveInfo(): Promise<ReserveInfoResponse> {
  const response = await fetch(`${RPC_URL}/json_rpc`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ jsonrpc: "2.0", id: "0", method: "get_reserve_info" }),
  });

  if (!response.ok) {
    throw new Error(`HTTP ${response.status} ${response.statusText}`);
  }

  return (await response.json()) as ReserveInfoResponse;
}

async function main() {
  console.log("[snapshotter] Starting reserve snapshot capture loop");

  while (true) {
    try {
      const reserveInfo = await fetchReserveInfo();

      if (reserveInfo.error) {
        console.error(`[snapshotter] RPC error: ${reserveInfo.error.code} ${reserveInfo.error.message}`);
      } else if (reserveInfo.result) {
        const stored = await saveReserveSnapshotToRedis(reserveInfo as any);
        if (stored) {
          console.log(`[snapshotter] Stored snapshot for prev height ${stored.previous_height}`);
        } else {
          console.warn("[snapshotter] Reserve info missing height – skipping");
        }
      } else {
        console.warn("[snapshotter] Unexpected response from daemon", reserveInfo);
      }
    } catch (error) {
      console.error("[snapshotter] Failed to capture snapshot:", error);
    }

    await new Promise((resolve) => setTimeout(resolve, POLL_INTERVAL_MS));
  }
}

process.on("SIGINT", async () => {
  console.log("[snapshotter] Caught SIGINT. Closing Redis connection...");
  await redis.quit();
  process.exit(0);
});

process.on("SIGTERM", async () => {
  console.log("[snapshotter] Caught SIGTERM. Closing Redis connection...");
  await redis.quit();
  process.exit(0);
});

void main();
