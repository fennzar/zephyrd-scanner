/* eslint-disable no-console */
process.env.DATA_STORE = process.env.DATA_STORE ?? "postgres";

import dotenv from "dotenv";
import path from "node:path";

import redis from "../redis";
import { getPrismaClient } from "../db";
import { upsertBlockReward } from "../db/blockRewards";
import { deleteAllTransactions, insertTransactions } from "../db/transactions";
import { defaultTotals, setTotals } from "../db/totals";
import {
  saveBlockProtocolStats,
  saveAggregatedProtocolStats,
} from "../db/protocolStats";
import { upsertReserveSnapshot } from "../db/reserveSnapshots";
import { upsertReserveMismatch } from "../db/reserveMismatches";
import { upsertLiveStats } from "../db/liveStats";
import {
  upsertHistoricalReturns,
  upsertProjectedReturns,
  replaceApyHistory,
  appendZysPriceHistory,
} from "../db/yieldAnalytics";
import { stores } from "../storage/factory";
import { LiveStats } from "../utils";

dotenv.config();

const prisma = getPrismaClient();

const BATCH_SIZE = 10_000;

async function chunked<T>(items: T[], size: number, worker: (chunk: T[]) => Promise<void>, label: string) {
  const total = items.length;
  for (let i = 0; i < total; i += size) {
    const chunk = items.slice(i, i + size);
    await worker(chunk);
    const processed = Math.min(total, i + size);
    const percent = ((processed / total) * 100).toFixed(2);
    console.log(`[migrate] ${label}: ${processed}/${total} (${percent}%)`);
  }
}

async function migratePricingRecords() {
  const entries = await redis.hgetall("pricing_records");
  const rows = Object.values(entries);
  console.log(`[migrate] Pricing records ${rows.length}`);
  await chunked(rows, BATCH_SIZE, async (chunk) => {
    const data: { blockHeight: number; timestamp: number; spot: number; movingAverage: number; reserve: number; reserveMa: number; stable: number; stableMa: number; yieldPrice: number }[] = [];
    for (const json of chunk) {
      try {
        const parsed = JSON.parse(json);
        if (typeof parsed.height !== "number") continue;
        data.push({
          blockHeight: parsed.height,
          timestamp: parsed.timestamp ?? 0,
          spot: parsed.spot ?? 0,
          movingAverage: parsed.moving_average ?? 0,
          reserve: parsed.reserve ?? 0,
          reserveMa: parsed.reserve_ma ?? 0,
          stable: parsed.stable ?? 0,
          stableMa: parsed.stable_ma ?? 0,
          yieldPrice: parsed.yield_price ?? 0,
        });
      } catch (error) {
        console.warn("[migrate] Failed to parse pricing record", error);
      }
    }
    if (data.length > 0) {
      await prisma.pricingRecord.createMany({
        data,
        skipDuplicates: true,
      });
    }
  }, "pricing records");
}

async function migrateBlockRewards() {
  const entries = await redis.hgetall("block_rewards");
  const rows = Object.values(entries);
  console.log(`[migrate] Block rewards ${rows.length}`);
  await chunked(rows, BATCH_SIZE, async (chunk) => {
    const data = [];
    for (const json of chunk) {
      try {
        const parsed = JSON.parse(json);
        if (typeof parsed.height !== "number") continue;
        data.push(parsed);
      } catch (error) {
        console.warn("[migrate] Failed to parse block reward", error);
      }
    }
    await prisma.blockReward.createMany({
      data: data.map((parsed) => ({
        blockHeight: parsed.height,
        minerReward: parsed.miner_reward ?? 0,
        governanceReward: parsed.governance_reward ?? 0,
        reserveReward: parsed.reserve_reward ?? 0,
        yieldReward: parsed.yield_reward ?? 0,
        minerRewardAtoms: parsed.miner_reward_atoms,
        governanceRewardAtoms: parsed.governance_reward_atoms,
        reserveRewardAtoms: parsed.reserve_reward_atoms,
        yieldRewardAtoms: parsed.yield_reward_atoms,
        baseRewardAtoms: parsed.base_reward_atoms,
        feeAdjustmentAtoms: parsed.fee_adjustment_atoms,
      })),
      skipDuplicates: true,
    });
  }, "block rewards");
}

async function migrateTransactions() {
  const entries = await redis.hvals("txs");
  console.log(`[migrate] Transactions ${entries.length}`);
  await deleteAllTransactions();
  for (let i = 0; i < entries.length; i += BATCH_SIZE) {
    const chunk = entries.slice(i, i + BATCH_SIZE);
    const rows = [];
    for (const json of chunk) {
      try {
        const parsed = JSON.parse(json);
        if (typeof parsed.hash !== "string" || typeof parsed.block_height !== "number") {
          continue;
        }
        rows.push({
          hash: parsed.hash,
          blockHeight: parsed.block_height,
          blockTimestamp: parsed.block_timestamp ?? 0,
          conversionType: parsed.conversion_type ?? "na",
          conversionRate: parsed.conversion_rate ?? 0,
          fromAsset: parsed.from_asset ?? null,
          fromAmount: parsed.from_amount ?? 0,
          fromAmountAtoms: parsed.from_amount_atoms ?? undefined,
          toAsset: parsed.to_asset ?? null,
          toAmount: parsed.to_amount ?? 0,
          toAmountAtoms: parsed.to_amount_atoms ?? undefined,
          conversionFeeAsset: parsed.conversion_fee_asset ?? null,
          conversionFeeAmount: parsed.conversion_fee_amount ?? 0,
          txFeeAsset: parsed.tx_fee_asset ?? null,
          txFeeAmount: parsed.tx_fee_amount ?? 0,
          txFeeAtoms: parsed.tx_fee_atoms ?? undefined,
        });
      } catch (error) {
        console.warn("[migrate] Failed to parse transaction", error);
      }
    }
    await insertTransactions(rows);
    console.log(`[migrate] Inserted ${Math.min(entries.length, i + BATCH_SIZE)} / ${entries.length} transactions`);
  }
}

async function migrateProtocolStats() {
  const entries = await redis.hgetall("protocol_stats");
  console.log(`[migrate] Protocol stats ${Object.keys(entries).length}`);
  const statsChunks = Object.entries(entries);
  await chunked(statsChunks, BATCH_SIZE, async (chunk) => {
    for (const [height, json] of chunk) {
      try {
        const parsed = JSON.parse(json);
        parsed.block_height = Number(height);
        await saveBlockProtocolStats(parsed);
      } catch (error) {
        console.warn("[migrate] Failed to parse protocol stats", height, error);
      }
    }
  }, "protocol stats");

  const aggScales: Array<["hour" | "day", string]> = [
    ["hour", "protocol_stats_hourly"],
    ["day", "protocol_stats_daily"],
  ];

  for (const [scale, key] of aggScales) {
    const entries = await redis.zrange(key, 0, -1, "WITHSCORES");
    for (let i = 0; i < entries.length; i += 2) {
      const json = entries[i];
      const timestamp = Number(entries[i + 1]);
      try {
        const data = JSON.parse(json);
        await saveAggregatedProtocolStats(scale === "hour" ? "hour" : "day", timestamp, data.window_end, data, !!data.pending);
      } catch (error) {
        console.warn("[migrate] Failed to parse aggregated stats", key, error);
      }
    }
  }
}

async function migrateReserveSnapshots() {
  const entries = await redis.hgetall("reserve_snapshots");
  console.log(`[migrate] Reserve snapshots ${Object.keys(entries).length}`);
  await chunked(Object.values(entries), BATCH_SIZE, async (chunk) => {
    for (const json of chunk) {
      try {
        const snapshot = JSON.parse(json);
        await upsertReserveSnapshot(snapshot);
      } catch (error) {
        console.warn("[migrate] Failed to parse reserve snapshot", error);
      }
    }
  }, "reserve snapshots");
}

async function migrateReserveMismatches() {
  const entries = await redis.hgetall("reserve_mismatch_heights");
  console.log(`[migrate] Reserve mismatch reports ${Object.keys(entries).length}`);
  await chunked(Object.values(entries), BATCH_SIZE, async (chunk) => {
    for (const json of chunk) {
      try {
        const report = JSON.parse(json);
        await upsertReserveMismatch(report);
      } catch (error) {
        console.warn("[migrate] Failed to parse reserve mismatch report", error);
      }
    }
  }, "reserve mismatches");
}

async function migrateLiveStats() {
  const value = await redis.get("live_stats");
  if (!value) {
    return;
  }
  try {
    const parsed = JSON.parse(value) as LiveStats;
    await upsertLiveStats(parsed);
    console.log("[migrate] Live stats imported");
  } catch (error) {
    console.warn("[migrate] Failed to parse live stats", error);
  }
}

async function migrateTotals() {
  const entries = await redis.hgetall("totals");
  if (!entries || Object.keys(entries).length === 0) {
    return;
  }
  const totals = { ...defaultTotals };
  for (const [key, value] of Object.entries(entries)) {
    const numeric = Number(value);
    if (Number.isFinite(numeric) && key in totals) {
      // @ts-expect-error runtime mapping
      totals[key] = numeric;
    }
  }
  await setTotals(totals);
  console.log("[migrate] Totals imported");
}

async function migrateScannerState() {
  const entries: Record<string, string | null> = {
    height_aggregator: await redis.get("height_aggregator"),
    timestamp_aggregator_hourly: await redis.get("timestamp_aggregator_hourly"),
    timestamp_aggregator_daily: await redis.get("timestamp_aggregator_daily"),
    height_prs: await redis.get("height_prs"),
    height_txs: await redis.get("height_txs"),
  };
  for (const [key, value] of Object.entries(entries)) {
    if (value != null) {
      await stores.scannerState.set(key, value);
    }
  }
  console.log("[migrate] Scanner state imported");
}

async function migrateHistoricalReturns() {
  const raw = await redis.get("historical_returns");
  if (!raw) return;
  try {
    const parsed = JSON.parse(raw);
    await upsertHistoricalReturns(parsed);
    console.log("[migrate] Historical returns imported");
  } catch (error) {
    console.warn("[migrate] Failed to parse historical returns", error);
  }
}

async function migrateProjectedReturns() {
  const raw = await redis.get("projected_returns");
  if (!raw) return;
  try {
    const parsed = JSON.parse(raw);
    await upsertProjectedReturns(parsed);
    console.log("[migrate] Projected returns imported");
  } catch (error) {
    console.warn("[migrate] Failed to parse projected returns", error);
  }
}

async function migrateApyHistory() {
  const raw = await redis.get("apy_history");
  if (!raw) return;
  try {
    const parsed = JSON.parse(raw);
    await replaceApyHistory(parsed);
    console.log("[migrate] APY history imported");
  } catch (error) {
    console.warn("[migrate] Failed to parse APY history", error);
  }
}

async function migrateZysPriceHistory() {
  const entries = await redis.zrange("zys_price_history", 0, -1, "WITHSCORES");
  const rows = [];
  for (let i = 0; i < entries.length; i += 2) {
    const json = entries[i];
    const timestamp = Number(entries[i + 1]);
    try {
      const parsed = JSON.parse(json);
      rows.push({
        block_height: parsed.block_height ?? 0,
        zys_price: parsed.zys_price ?? 0,
        timestamp,
      });
    } catch (error) {
      console.warn("[migrate] Failed to parse zys price entry", error);
    }
  }
  await appendZysPriceHistory(rows);
  console.log(`[migrate] ZYS price history imported (${rows.length})`);
}

async function main() {
  try {
    await prisma.$connect();
    console.log("[migrate] Starting Redis -> Postgres migration");
    await migratePricingRecords();
    await migrateBlockRewards();
    await migrateTransactions();
    await migrateProtocolStats();
    await migrateReserveSnapshots();
    await migrateReserveMismatches();
    await migrateLiveStats();
    await migrateTotals();
    await migrateScannerState();
    await migrateHistoricalReturns();
    await migrateProjectedReturns();
    await migrateApyHistory();
    await migrateZysPriceHistory();
    console.log("[migrate] Migration complete");
  } catch (error) {
    console.error("[migrate] Migration failed", error);
    process.exitCode = 1;
  } finally {
    await redis.quit();
    await prisma.$disconnect();
  }
}

void main();
