import { describe, expect, test, afterAll } from "bun:test";
import { setupTestDatabase, resetTestData, teardownTestDatabase } from "./setup/db";
import redis from "../src/redis";
import {
  getLastReserveSnapshotPreviousHeight,
  getLatestReserveSnapshot,
  getLatestProtocolStats,
  getTotalsSummaryData,
  getBlockProtocolStatsFromRedis,
  getAggregatedProtocolStatsFromRedis,
  getPricingRecordFromStore,
  getRedisBlockRewardInfo,
  getPricingRecords,
  getBlockRewards,
  getRedisTransaction,
  getReserveSnapshots,
  getTransactions,
  getLiveStats,
} from "../src/utils";
import {
  getHistoricalReturnsFromRedis,
  getProjectedReturnsFromRedis,
  getAPYHistoryFromRedis,
} from "../src/yield";
import {
  getZYSPriceHistoryFromRedis,
  getMostRecentBlockHeightFromRedis,
} from "../src/pr";

// Ensure the test database exists before running tests
await setupTestDatabase();
await resetTestData();

function assertRedisIdle() {
  expect(redis.status).toBe("wait");
}

describe("redis gating — DATA_STORE=postgres", () => {
  afterAll(async () => {
    await teardownTestDatabase();
  });

  // --- utils.ts gated functions ---

  test("getLastReserveSnapshotPreviousHeight() returns null, redis untouched", async () => {
    const result = await getLastReserveSnapshotPreviousHeight();
    expect(result).toBeNull();
    assertRedisIdle();
  });

  test("getLatestReserveSnapshot() returns null, redis untouched", async () => {
    const result = await getLatestReserveSnapshot();
    expect(result).toBeNull();
    assertRedisIdle();
  });

  test("getLatestProtocolStats() returns null, redis untouched", async () => {
    const result = await getLatestProtocolStats();
    expect(result).toBeNull();
    assertRedisIdle();
  });

  test("getTotalsSummaryData() returns zeros from empty DB, redis untouched", async () => {
    const result = await getTotalsSummaryData();
    expect(result).not.toBeNull();
    expect(result!.conversion_transactions).toBe(0);
    expect(result!.mint_stable_volume).toBe(0);
    assertRedisIdle();
  });

  test("getBlockProtocolStatsFromRedis() returns [], redis untouched", async () => {
    const result = await getBlockProtocolStatsFromRedis();
    expect(result).toEqual([]);
    assertRedisIdle();
  });

  test("getAggregatedProtocolStatsFromRedis('hour') returns [], redis untouched", async () => {
    const result = await getAggregatedProtocolStatsFromRedis("hour");
    expect(result).toEqual([]);
    assertRedisIdle();
  });

  test("getPricingRecordFromStore(100) returns null, redis untouched", async () => {
    const result = await getPricingRecordFromStore(100);
    expect(result).toBeNull();
    assertRedisIdle();
  });

  test("getRedisBlockRewardInfo(100) returns null, redis untouched", async () => {
    const result = await getRedisBlockRewardInfo(100);
    expect(result).toBeNull();
    assertRedisIdle();
  });

  test("getPricingRecords({}) returns empty result, redis untouched", async () => {
    const result = await getPricingRecords({});
    expect(result.total).toBe(0);
    expect(result.results).toEqual([]);
    assertRedisIdle();
  });

  test("getBlockRewards({}) returns empty result, redis untouched", async () => {
    const result = await getBlockRewards({});
    expect(result.total).toBe(0);
    expect(result.results).toEqual([]);
    assertRedisIdle();
  });

  test("getRedisTransaction('fakehash') returns null, redis untouched", async () => {
    const result = await getRedisTransaction("fakehash");
    expect(result).toBeNull();
    assertRedisIdle();
  });

  test("getReserveSnapshots({}) returns empty result, redis untouched", async () => {
    const result = await getReserveSnapshots({});
    expect(result.total).toBe(0);
    expect(result.results).toEqual([]);
    assertRedisIdle();
  });

  test("getTransactions({}) returns empty result, redis untouched", async () => {
    const result = await getTransactions({});
    expect(result.total).toBe(0);
    expect(result.results).toEqual([]);
    assertRedisIdle();
  });

  // getLiveStats calls daemon RPC which is unavailable in test,
  // but it still should not touch Redis. It may return null due to
  // no daemon / no cached data.
  test("getLiveStats() does not touch redis", async () => {
    const result = await getLiveStats();
    // May be null (no daemon, no cached data) — that's OK
    expect(result === null || typeof result === "object").toBe(true);
    assertRedisIdle();
  });

  // --- yield.ts gated functions ---

  test("getHistoricalReturnsFromRedis() returns null, redis untouched", async () => {
    const result = await getHistoricalReturnsFromRedis();
    expect(result).toBeNull();
    assertRedisIdle();
  });

  test("getProjectedReturnsFromRedis() returns null, redis untouched", async () => {
    const result = await getProjectedReturnsFromRedis();
    expect(result).toBeNull();
    assertRedisIdle();
  });

  test("getAPYHistoryFromRedis() returns null, redis untouched", async () => {
    const result = await getAPYHistoryFromRedis();
    expect(result).toBeNull();
    assertRedisIdle();
  });

  // --- pr.ts gated functions ---

  test("getZYSPriceHistoryFromRedis() returns [], redis untouched", async () => {
    const result = await getZYSPriceHistoryFromRedis();
    expect(result).toEqual([]);
    assertRedisIdle();
  });

  test("getMostRecentBlockHeightFromRedis() returns 0, redis untouched", async () => {
    const result = await getMostRecentBlockHeightFromRedis();
    expect(result).toBe(0);
    assertRedisIdle();
  });

  // --- Final assertion ---

  test("redis.status is still 'wait' after all gated calls", () => {
    assertRedisIdle();
  });
});
