/* eslint-disable no-console */
process.env.DATA_STORE = process.env.DATA_STORE ?? "hybrid";

import dotenv from "dotenv";
import { randomInt } from "node:crypto";

import redis from "../redis";
import { stores } from "../storage/factory";
import { fetchBlockProtocolStats } from "../db/protocolStats";
import { getTransactionsFromRedis, getTotalsFromRedis } from "../utils";
import { queryTransactions } from "../db/transactions";
import { getTotals as getSqlTotals } from "../db/totals";
import { getReserveSnapshotByPreviousHeight } from "../db/reserveSnapshots";
import { getZYSPriceHistoryFromRedis } from "../pr";
import { fetchZysPriceHistory } from "../db/yieldAnalytics";

dotenv.config();

interface ComparisonResult {
  label: string;
  issues: string[];
}

function compareNumbers(label: string, a: number, b: number, tolerance = 1e-6): string | null {
  if (Number.isNaN(a) && Number.isNaN(b)) {
    return null;
  }
  if (Math.abs(a - b) > tolerance) {
    return `${label}: redis=${a} postgres=${b}`;
  }
  return null;
}

async function comparePricingRecords(): Promise<ComparisonResult> {
  const issues: string[] = [];
  for (let i = 0; i < 5; i++) {
    const height = randomInt(100_000, 550_000);
    const redisValue = await redis.hget("pricing_records", height.toString());
    const sqlValue = await stores.pricing.get(height);
    if (!redisValue || !sqlValue) {
      issues.push(`pricing height ${height}: missing in ${redisValue ? "postgres" : "redis"}`);
      continue;
    }
    const redisParsed = JSON.parse(redisValue);
    const sqlMap = {
      spot: sqlValue.spot,
      moving_average: sqlValue.movingAverage,
      reserve: sqlValue.reserve,
      stable: sqlValue.stable,
      yield_price: sqlValue.yieldPrice,
    };
    for (const key of ["spot", "moving_average", "reserve", "stable", "yield_price"] as const) {
      const diff = compareNumbers(
        `pricing ${height}.${key}`,
        redisParsed[key] ?? 0,
        sqlMap[key]
      );
      if (diff) issues.push(diff);
    }
  }
  return { label: "pricing_records", issues };
}

async function compareProtocolStats(): Promise<ComparisonResult> {
  const issues: string[] = [];
  const heights = [randomInt(100_000, 553_000), randomInt(100_000, 553_000)];
  const sqlRows = await fetchBlockProtocolStats(Math.min(...heights), Math.max(...heights));
  for (const row of sqlRows) {
    const redisValue = await redis.hget("protocol_stats", row.block_height.toString());
    if (!redisValue) {
      issues.push(`protocol_stats ${row.block_height}: missing in redis`);
      continue;
    }
    const redisParsed = JSON.parse(redisValue);
    const diff = compareNumbers(
      `protocol_stats ${row.block_height}.reserve_ratio`,
      redisParsed.reserve_ratio ?? 0,
      row.reserve_ratio ?? 0,
      1e-4
    );
    if (diff) issues.push(diff);
  }
  return { label: "protocol_stats", issues };
}

async function compareTransactions(): Promise<ComparisonResult> {
  const issues: string[] = [];
  const from = Date.now() / 1000 - 60 * 60 * 24 * randomInt(1, 7);
  const redisResult = await getTransactionsFromRedis({ fromTimestamp: from, limit: 20, order: "asc" });
  const sqlResult = await queryTransactions({ fromTimestamp: from, limit: 20, order: "asc" });
  if (redisResult.total !== sqlResult.total) {
    issues.push(`transactions total mismatch: redis=${redisResult.total} postgres=${sqlResult.total}`);
  }
  return { label: "transactions", issues };
}

async function compareTotals(): Promise<ComparisonResult> {
  const issues: string[] = [];
  const redisTotals = await getTotalsFromRedis();
  const sqlTotals = await getSqlTotals();
  if (!redisTotals || !sqlTotals) {
    issues.push("totals missing in one store");
    return { label: "totals", issues };
  }
  const diff = compareNumbers(
    "totals.reserve_reward",
    Number(redisTotals.reserve_reward ?? "0"),
    sqlTotals.reserveReward
  );
  if (diff) issues.push(diff);
  return { label: "totals", issues };
}

async function compareSnapshots(): Promise<ComparisonResult> {
  const issues: string[] = [];
  const height = randomInt(100_000, 550_000);
  const redisValue = await redis.hget("reserve_snapshots", height.toString());
  const sqlValue = await getReserveSnapshotByPreviousHeight(height);
  if (!!redisValue !== !!sqlValue) {
    issues.push(`reserve snapshot ${height}: presence mismatch`);
  }
  return { label: "reserve_snapshots", issues };
}

async function compareZysPriceHistory(): Promise<ComparisonResult> {
  const issues: string[] = [];
  const redisHistory = await getZYSPriceHistoryFromRedis();
  const sqlHistory = await fetchZysPriceHistory();
  if (redisHistory.length !== sqlHistory.length) {
    issues.push(`zys_price_history length mismatch: redis=${redisHistory.length} postgres=${sqlHistory.length}`);
  }
  return { label: "zys_price_history", issues };
}

async function main() {
  const comparisons = await Promise.all([
    comparePricingRecords(),
    compareProtocolStats(),
    compareTransactions(),
    compareTotals(),
    compareSnapshots(),
    compareZysPriceHistory(),
  ]);

  console.log("=== Redis vs Postgres Comparison ===");
  for (const { label, issues } of comparisons) {
    if (issues.length === 0) {
      console.log(`✔ ${label} OK`);
    } else {
      console.log(`✖ ${label}`);
      for (const issue of issues) {
        console.log(`  - ${issue}`);
      }
    }
  }

  await redis.quit();
}

main().catch((error) => {
  console.error("[compare] failed", error);
  process.exit(1);
});
