import { Prisma } from "@prisma/client";

import { getPrismaClient } from "./index";

export interface PricingRecordRow {
  blockHeight: number;
  timestamp: number;
  spot: number;
  movingAverage: number;
  reserve: number;
  reserveMa: number;
  stable: number;
  stableMa: number;
  yieldPrice: number;
}

export interface PricingRecordRangeQuery {
  fromHeight?: number;
  toHeight?: number;
  limit?: number;
  order?: "asc" | "desc";
}

export interface PricingRecordRangeResult {
  total: number;
  rows: PricingRecordRow[];
}

export async function queryPricingRecords(
  options: PricingRecordRangeQuery = {}
): Promise<PricingRecordRangeResult> {
  const prisma = getPrismaClient();
  const { fromHeight, toHeight, limit, order = "asc" } = options;

  const where: Prisma.PricingRecordWhereInput = {};
  const heightFilter: Prisma.IntFilter = {};
  if (fromHeight != null) {
    heightFilter.gte = fromHeight;
  }
  if (toHeight != null) {
    heightFilter.lte = toHeight;
  }
  if (Object.keys(heightFilter).length > 0) {
    where.blockHeight = heightFilter;
  }

  const resolvedLimit = limit && Number.isFinite(limit) && limit > 0 ? Math.floor(limit) : undefined;
  const sortOrder = order === "desc" ? "desc" : "asc";

  const [total, rows] = await Promise.all([
    prisma.pricingRecord.count({ where }),
    prisma.pricingRecord.findMany({
      where,
      orderBy: { blockHeight: sortOrder },
      take: resolvedLimit,
    }),
  ]);

  return {
    total,
    rows: rows.map((row) => ({
      blockHeight: row.blockHeight,
      timestamp: row.timestamp,
      spot: row.spot,
      movingAverage: row.movingAverage,
      reserve: row.reserve,
      reserveMa: row.reserveMa,
      stable: row.stable,
      stableMa: row.stableMa,
      yieldPrice: row.yieldPrice,
    })),
  };
}
