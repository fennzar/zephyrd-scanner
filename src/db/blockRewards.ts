import { Prisma } from "@prisma/client";

import { getPrismaClient } from "./index";

export interface BlockRewardRecord {
  blockHeight: number;
  minerReward: number;
  governanceReward: number;
  reserveReward: number;
  yieldReward: number;
  minerRewardAtoms?: string;
  governanceRewardAtoms?: string;
  reserveRewardAtoms?: string;
  yieldRewardAtoms?: string;
  baseRewardAtoms?: string;
  feeAdjustmentAtoms?: string;
}

export async function upsertBlockReward(record: BlockRewardRecord): Promise<void> {
  const prisma = getPrismaClient();
  await prisma.blockReward.upsert({
    where: { blockHeight: record.blockHeight },
    update: {
      minerReward: record.minerReward,
      governanceReward: record.governanceReward,
      reserveReward: record.reserveReward,
      yieldReward: record.yieldReward,
      minerRewardAtoms: record.minerRewardAtoms,
      governanceRewardAtoms: record.governanceRewardAtoms,
      reserveRewardAtoms: record.reserveRewardAtoms,
      yieldRewardAtoms: record.yieldRewardAtoms,
      baseRewardAtoms: record.baseRewardAtoms,
      feeAdjustmentAtoms: record.feeAdjustmentAtoms,
    },
    create: {
      blockHeight: record.blockHeight,
      minerReward: record.minerReward,
      governanceReward: record.governanceReward,
      reserveReward: record.reserveReward,
      yieldReward: record.yieldReward,
      minerRewardAtoms: record.minerRewardAtoms,
      governanceRewardAtoms: record.governanceRewardAtoms,
      reserveRewardAtoms: record.reserveRewardAtoms,
      yieldRewardAtoms: record.yieldRewardAtoms,
      baseRewardAtoms: record.baseRewardAtoms,
      feeAdjustmentAtoms: record.feeAdjustmentAtoms,
    },
  });
}

export async function getBlockReward(blockHeight: number) {
  const prisma = getPrismaClient();
  return prisma.blockReward.findUnique({
    where: { blockHeight },
  });
}

export async function deleteBlockRewardsAboveHeight(blockHeight: number): Promise<void> {
  const prisma = getPrismaClient();
  await prisma.blockReward.deleteMany({
    where: {
      blockHeight: {
        gt: blockHeight,
      },
    },
  });
}

export async function deleteAllBlockRewards(): Promise<void> {
  const prisma = getPrismaClient();
  await prisma.blockReward.deleteMany();
}

export interface BlockRewardRangeQuery {
  fromHeight?: number;
  toHeight?: number;
  limit?: number;
  order?: "asc" | "desc";
}

export interface BlockRewardRangeResult {
  total: number;
  rows: BlockRewardRecord[];
}

export async function queryBlockRewardsRange(options: BlockRewardRangeQuery = {}): Promise<BlockRewardRangeResult> {
  const prisma = getPrismaClient();
  const { fromHeight, toHeight, limit, order = "asc" } = options;

  const where: Prisma.BlockRewardWhereInput = {};
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
    prisma.blockReward.count({ where }),
    prisma.blockReward.findMany({
      where,
      orderBy: { blockHeight: sortOrder },
      take: resolvedLimit,
    }),
  ]);

  return {
    total,
    rows: rows.map((row) => ({
      blockHeight: row.blockHeight,
      minerReward: row.minerReward,
      governanceReward: row.governanceReward,
      reserveReward: row.reserveReward,
      yieldReward: row.yieldReward,
      minerRewardAtoms: row.minerRewardAtoms ?? undefined,
      governanceRewardAtoms: row.governanceRewardAtoms ?? undefined,
      reserveRewardAtoms: row.reserveRewardAtoms ?? undefined,
      yieldRewardAtoms: row.yieldRewardAtoms ?? undefined,
      baseRewardAtoms: row.baseRewardAtoms ?? undefined,
      feeAdjustmentAtoms: row.feeAdjustmentAtoms ?? undefined,
    })),
  };
}
