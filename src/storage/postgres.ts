import { Prisma } from "@prisma/client";

import { getPrismaClient } from "../db";
import { DataStores, PricingRecordInput, PricingRecordResult, PricingStore, ScannerStateStore } from "./types";

function toPrismaData(record: PricingRecordInput): Prisma.PricingRecordCreateInput {
  return {
    blockHeight: record.blockHeight,
    timestamp: record.timestamp,
    spot: record.spot,
    movingAverage: record.movingAverage,
    reserve: record.reserve,
    reserveMa: record.reserveMa,
    stable: record.stable,
    stableMa: record.stableMa,
    yieldPrice: record.yieldPrice,
  };
}

function fromPrismaRecord(record: { blockHeight: number; timestamp: number; spot: number; movingAverage: number; reserve: number; reserveMa: number; stable: number; stableMa: number; yieldPrice: number }): PricingRecordResult {
  return {
    blockHeight: record.blockHeight,
    timestamp: record.timestamp,
    spot: record.spot,
    movingAverage: record.movingAverage,
    reserve: record.reserve,
    reserveMa: record.reserveMa,
    stable: record.stable,
    stableMa: record.stableMa,
    yieldPrice: record.yieldPrice,
  };
}

const scannerStateStore: ScannerStateStore = {
  async get(key) {
    const prisma = getPrismaClient();
    const entry = await prisma.scannerState.findUnique({ where: { key } });
    return entry?.value ?? null;
  },

  async set(key, value) {
    const prisma = getPrismaClient();
    await prisma.scannerState.upsert({
      where: { key },
      update: { value },
      create: { key, value },
    });
  },
};

const pricingStore: PricingStore = {
  async save(record) {
    const prisma = getPrismaClient();
    await prisma.pricingRecord.upsert({
      where: { blockHeight: record.blockHeight },
      update: toPrismaData(record),
      create: toPrismaData(record),
    });
    // Only advance height_prs, never regress (TX scanner saves pre-V1 pricing
    // records concurrently with/after the PR scanner's higher heights)
    const currentHeightStr = await scannerStateStore.get("height_prs");
    const currentHeight = currentHeightStr ? Number(currentHeightStr) : -1;
    if (record.blockHeight > currentHeight) {
      await scannerStateStore.set("height_prs", record.blockHeight.toString());
    }
  },

  async saveBatch(records) {
    if (records.length === 0) return;
    const prisma = getPrismaClient();

    // Build a raw SQL INSERT ... ON CONFLICT DO UPDATE for the batch
    const placeholders: string[] = [];
    const values: (number | string)[] = [];
    let idx = 1;
    for (const r of records) {
      placeholders.push(`($${idx}, $${idx + 1}, $${idx + 2}, $${idx + 3}, $${idx + 4}, $${idx + 5}, $${idx + 6}, $${idx + 7}, $${idx + 8})`);
      values.push(r.blockHeight, r.timestamp, r.spot, r.movingAverage, r.reserve, r.reserveMa, r.stable, r.stableMa, r.yieldPrice);
      idx += 9;
    }

    const sql = `INSERT INTO pricing_records (block_height, timestamp, spot, moving_average, reserve, reserve_ma, stable, stable_ma, yield_price)
VALUES ${placeholders.join(", ")}
ON CONFLICT (block_height) DO UPDATE SET
  timestamp = EXCLUDED.timestamp,
  spot = EXCLUDED.spot,
  moving_average = EXCLUDED.moving_average,
  reserve = EXCLUDED.reserve,
  reserve_ma = EXCLUDED.reserve_ma,
  stable = EXCLUDED.stable,
  stable_ma = EXCLUDED.stable_ma,
  yield_price = EXCLUDED.yield_price,
  updated_at = NOW()`;

    await prisma.$executeRawUnsafe(sql, ...values);

    // Update height_prs only if this batch advances it (never regress)
    const maxHeight = records[records.length - 1].blockHeight;
    const currentHeightStr = await scannerStateStore.get("height_prs");
    const currentHeight = currentHeightStr ? Number(currentHeightStr) : -1;
    if (maxHeight > currentHeight) {
      await scannerStateStore.set("height_prs", maxHeight.toString());
    }
  },

  async get(blockHeight) {
    const prisma = getPrismaClient();
    const record = await prisma.pricingRecord.findUnique({
      where: { blockHeight },
      select: {
        blockHeight: true,
        timestamp: true,
        spot: true,
        movingAverage: true,
        reserve: true,
        reserveMa: true,
        stable: true,
        stableMa: true,
        yieldPrice: true,
      },
    });
    return record ? fromPrismaRecord(record) : null;
  },

  async getLatestHeight() {
    const fromState = await scannerStateStore.get("height_prs");
    if (fromState) {
      const parsed = Number(fromState);
      if (Number.isFinite(parsed)) {
        return parsed;
      }
    }
    const prisma = getPrismaClient();
    const latest = await prisma.pricingRecord.findFirst({
      orderBy: { blockHeight: "desc" },
      select: { blockHeight: true },
    });
    return latest?.blockHeight ?? -1;
  },
};

export async function getPricingRecordRange(fromHeight: number, toHeight: number): Promise<Map<number, PricingRecordResult>> {
  const prisma = getPrismaClient();
  const rows = await prisma.pricingRecord.findMany({
    where: {
      blockHeight: { gte: fromHeight, lte: toHeight },
    },
    orderBy: { blockHeight: "asc" },
    select: {
      blockHeight: true,
      timestamp: true,
      spot: true,
      movingAverage: true,
      reserve: true,
      reserveMa: true,
      stable: true,
      stableMa: true,
      yieldPrice: true,
    },
  });
  const map = new Map<number, PricingRecordResult>();
  for (const row of rows) {
    map.set(row.blockHeight, fromPrismaRecord(row));
  }
  return map;
}

export function createPostgresStores(): DataStores {
  return {
    pricing: pricingStore,
    scannerState: scannerStateStore,
  };
}
