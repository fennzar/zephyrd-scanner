// Integration test: full scan cycle with real daemon RPC calls.
// Requires a running zephyrd daemon on 127.0.0.1:17767.

// Set env vars for a small block range before importing src/ modules
const START = 89300;
const END = 89304;
process.env.START_BLOCK = START.toString();
process.env.END_BLOCK = END.toString();

import { describe, expect, test, beforeAll, afterAll } from "bun:test";
import { setupTestDatabase, resetTestData, teardownTestDatabase, getTestPrisma } from "./setup/db";
import { scanPricingRecords } from "../src/pr";
import { scanTransactions } from "../src/tx";
import { stores } from "../src/storage/factory";
import redis from "../src/redis";

function assertRedisIdle() {
  expect(redis.status).toBe("wait");
}

describe("scanner integration — real daemon RPC", () => {
  beforeAll(async () => {
    await setupTestDatabase();
    await resetTestData();
  });

  afterAll(async () => {
    await teardownTestDatabase();
  });

  test("scanPricingRecords() saves 5 pricing records to DB", async () => {
    await scanPricingRecords();

    const latestHeight = await stores.pricing.getLatestHeight();
    expect(latestHeight).toBe(END);

    for (let h = START; h <= END; h++) {
      const record = await stores.pricing.get(h);
      expect(record).not.toBeNull();
      expect(record!.blockHeight).toBe(h);
      expect(record!.timestamp).toBeGreaterThan(0);
    }

    assertRedisIdle();
  }, 30_000);

  test("pricing record values are realistic", async () => {
    const record = await stores.pricing.get(START);
    expect(record).not.toBeNull();

    // Block 89300 is the HF block — verify values are positive and reasonable
    expect(record!.spot).toBeGreaterThan(0);
    expect(record!.movingAverage).toBeGreaterThan(0);
    expect(record!.reserve).toBeGreaterThan(0);
    expect(record!.reserveMa).toBeGreaterThan(0);
    expect(record!.stable).toBeGreaterThan(0);
    expect(record!.stableMa).toBeGreaterThan(0);

    assertRedisIdle();
  });

  test("scanTransactions() saves block rewards and advances height_txs", async () => {
    await scanTransactions();

    // Verify tx scanner height was advanced
    const txHeight = await stores.scannerState.get("height_txs");
    expect(Number(txHeight)).toBe(END);

    // Verify block rewards were created for the range
    const prisma = getTestPrisma();
    const blockRewards = await prisma.blockReward.findMany({
      where: { blockHeight: { gte: START, lte: END } },
      orderBy: { blockHeight: "asc" },
    });

    expect(blockRewards.length).toBe(END - START + 1);
    expect(blockRewards[0].blockHeight).toBe(START);

    // Miner reward should be positive
    expect(blockRewards[0].minerReward).toBeGreaterThan(0);

    assertRedisIdle();
  }, 30_000);

  test("aggregate() creates protocol_stats rows", async () => {
    const { aggregate } = await import("../src/aggregator");
    await aggregate();

    const prisma = getTestPrisma();
    const protocolStats = await prisma.protocolStatsBlock.findMany({
      where: { blockHeight: { gte: START, lte: END } },
      orderBy: { blockHeight: "asc" },
    });

    expect(protocolStats.length).toBe(END - START + 1);

    const firstBlock = protocolStats[0];
    expect(firstBlock.blockHeight).toBe(START);
    expect(firstBlock.spot).toBeGreaterThan(0);

    // Verify aggregator height was updated
    const aggregatorHeight = await stores.scannerState.get("height_aggregator");
    expect(Number(aggregatorHeight)).toBe(END);

    assertRedisIdle();
  }, 30_000);

  test("redis.status is still 'wait' after entire integration cycle", () => {
    assertRedisIdle();
  });
});
