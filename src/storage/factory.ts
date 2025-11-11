import { usePostgres, useRedis } from "../config";
import { createPostgresStores } from "./postgres";
import { createRedisStores } from "./redis";
import { DataStores, PricingStore, ScannerStateStore } from "./types";

let redisStores: DataStores | null = null;
let postgresStores: DataStores | null = null;

function ensureRedisStores(): DataStores {
  if (!redisStores) {
    redisStores = createRedisStores();
  }
  return redisStores;
}

function ensurePostgresStores(): DataStores {
  if (!postgresStores) {
    postgresStores = createPostgresStores();
  }
  return postgresStores;
}

const pricingStore: PricingStore = {
  async save(record) {
    if (usePostgres()) {
      await ensurePostgresStores().pricing.save(record);
    }
    if (useRedis()) {
      await ensureRedisStores().pricing.save(record);
    }
  },

  async get(blockHeight) {
    if (usePostgres()) {
      const fromPostgres = await ensurePostgresStores().pricing.get(blockHeight);
      if (fromPostgres) {
        return fromPostgres;
      }
      if (!useRedis()) {
        return null;
      }
    }
    if (useRedis()) {
      return ensureRedisStores().pricing.get(blockHeight);
    }
    return null;
  },

  async getLatestHeight() {
    if (usePostgres()) {
      return ensurePostgresStores().pricing.getLatestHeight();
    }
    return ensureRedisStores().pricing.getLatestHeight();
  },
};

const scannerStateStore: ScannerStateStore = {
  async get(key) {
    if (usePostgres()) {
      const value = await ensurePostgresStores().scannerState.get(key);
      if (value !== null) {
        return value;
      }
      if (!useRedis()) {
        return null;
      }
    }
    if (useRedis()) {
      return ensureRedisStores().scannerState.get(key);
    }
    return null;
  },

  async set(key, value) {
    if (usePostgres()) {
      await ensurePostgresStores().scannerState.set(key, value);
    }
    if (useRedis()) {
      await ensureRedisStores().scannerState.set(key, value);
    }
  },
};

export const stores = {
  pricing: pricingStore,
  scannerState: scannerStateStore,
};
