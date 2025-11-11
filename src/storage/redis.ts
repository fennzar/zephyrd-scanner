import redis from "../redis";
import { DataStores, PricingRecordInput, PricingRecordResult, PricingStore, ScannerStateStore } from "./types";

function serializePricingRecord(record: PricingRecordInput) {
  return JSON.stringify({
    height: record.blockHeight,
    timestamp: record.timestamp,
    spot: record.spot,
    moving_average: record.movingAverage,
    reserve: record.reserve,
    reserve_ma: record.reserveMa,
    stable: record.stable,
    stable_ma: record.stableMa,
    yield_price: record.yieldPrice,
  });
}

function parsePricingRecord(json: string | null): PricingRecordResult | null {
  if (!json) {
    return null;
  }
  try {
    const parsed = JSON.parse(json) as {
      height?: number;
      timestamp?: number;
      spot?: number;
      moving_average?: number;
      reserve?: number;
      reserve_ma?: number;
      stable?: number;
      stable_ma?: number;
      yield_price?: number;
    };
    if (typeof parsed.height !== "number" || typeof parsed.timestamp !== "number") {
      return null;
    }
    return {
      blockHeight: parsed.height,
      timestamp: parsed.timestamp,
      spot: parsed.spot ?? 0,
      movingAverage: parsed.moving_average ?? 0,
      reserve: parsed.reserve ?? 0,
      reserveMa: parsed.reserve_ma ?? 0,
      stable: parsed.stable ?? 0,
      stableMa: parsed.stable_ma ?? 0,
      yieldPrice: parsed.yield_price ?? 0,
    };
  } catch (error) {
    console.error("Failed to parse pricing record from redis:", error);
    return null;
  }
}

const pricingStore: PricingStore = {
  async save(record) {
    await redis.hset("pricing_records", record.blockHeight.toString(), serializePricingRecord(record));
    await redis.set("height_prs", record.blockHeight.toString());
  },

  async get(blockHeight) {
    const json = await redis.hget("pricing_records", blockHeight.toString());
    return parsePricingRecord(json);
  },

  async getLatestHeight() {
    const height = await redis.get("height_prs");
    return height ? Number(height) || 0 : 0;
  },
};

const scannerStateStore: ScannerStateStore = {
  async get(key) {
    return redis.get(key);
  },

  async set(key, value) {
    await redis.set(key, value);
  },
};

export function createRedisStores(): DataStores {
  return {
    pricing: pricingStore,
    scannerState: scannerStateStore,
  };
}
