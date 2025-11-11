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
    await scannerStateStore.set("height_prs", record.blockHeight.toString());
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
    return latest?.blockHeight ?? 0;
  },
};

export function createPostgresStores(): DataStores {
  return {
    pricing: pricingStore,
    scannerState: scannerStateStore,
  };
}
