import { describe, expect, test, beforeAll, afterAll } from "bun:test";
import { setupTestDatabase, resetTestData, teardownTestDatabase } from "./setup/db";
import { stores } from "../src/storage/factory";
import redis from "../src/redis";

beforeAll(async () => {
  await setupTestDatabase();
  await resetTestData();
});

afterAll(async () => {
  await teardownTestDatabase();
});

function assertRedisIdle() {
  expect(redis.status).toBe("wait");
}

describe("storage — scannerState round-trips", () => {
  test("set then get returns the stored value", async () => {
    await stores.scannerState.set("test_key", "42");
    const value = await stores.scannerState.get("test_key");
    expect(value).toBe("42");
    assertRedisIdle();
  });

  test("get returns null for missing key", async () => {
    const value = await stores.scannerState.get("nonexistent_key");
    expect(value).toBeNull();
    assertRedisIdle();
  });

  test("set overwrites existing key", async () => {
    await stores.scannerState.set("overwrite_key", "first");
    await stores.scannerState.set("overwrite_key", "second");
    const value = await stores.scannerState.get("overwrite_key");
    expect(value).toBe("second");
    assertRedisIdle();
  });
});

describe("storage — pricing round-trips", () => {
  const testRecord = {
    blockHeight: 89300,
    timestamp: 1696152427,
    spot: 1.5,
    movingAverage: 1.45,
    reserve: 0.8,
    reserveMa: 0.78,
    stable: 0.95,
    stableMa: 0.94,
    yieldPrice: 1.02,
  };

  test("save then get returns the stored pricing record", async () => {
    await stores.pricing.save(testRecord);
    const result = await stores.pricing.get(testRecord.blockHeight);
    expect(result).not.toBeNull();
    expect(result!.blockHeight).toBe(testRecord.blockHeight);
    expect(result!.timestamp).toBe(testRecord.timestamp);
    expect(result!.spot).toBeCloseTo(testRecord.spot, 4);
    expect(result!.movingAverage).toBeCloseTo(testRecord.movingAverage, 4);
    expect(result!.reserve).toBeCloseTo(testRecord.reserve, 4);
    expect(result!.reserveMa).toBeCloseTo(testRecord.reserveMa, 4);
    expect(result!.stable).toBeCloseTo(testRecord.stable, 4);
    expect(result!.stableMa).toBeCloseTo(testRecord.stableMa, 4);
    expect(result!.yieldPrice).toBeCloseTo(testRecord.yieldPrice, 4);
    assertRedisIdle();
  });

  test("getLatestHeight returns highest saved block", async () => {
    // Save a second record at a higher height
    await stores.pricing.save({
      ...testRecord,
      blockHeight: 89305,
    });
    const latestHeight = await stores.pricing.getLatestHeight();
    expect(latestHeight).toBe(89305);
    assertRedisIdle();
  });

  test("get returns null for missing block height", async () => {
    const result = await stores.pricing.get(999999);
    expect(result).toBeNull();
    assertRedisIdle();
  });

  test("getLatestHeight returns 0 for empty table", async () => {
    // Reset data to clear all pricing records
    await resetTestData();
    const latestHeight = await stores.pricing.getLatestHeight();
    expect(latestHeight).toBe(0);
    assertRedisIdle();
  });
});

describe("storage — final redis check", () => {
  test("redis.status is still 'wait' after all storage operations", () => {
    assertRedisIdle();
  });
});
