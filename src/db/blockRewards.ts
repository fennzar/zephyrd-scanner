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

export async function upsertBlockRewardBatch(records: BlockRewardRecord[]): Promise<void> {
  if (records.length === 0) return;
  const prisma = getPrismaClient();

  const placeholders: string[] = [];
  const values: (number | string | null)[] = [];
  let idx = 1;
  for (const r of records) {
    placeholders.push(
      `($${idx}, $${idx + 1}, $${idx + 2}, $${idx + 3}, $${idx + 4}, $${idx + 5}, $${idx + 6}, $${idx + 7}, $${idx + 8}, $${idx + 9}, $${idx + 10})`
    );
    values.push(
      r.blockHeight,
      r.minerReward,
      r.governanceReward,
      r.reserveReward,
      r.yieldReward,
      r.minerRewardAtoms ?? null,
      r.governanceRewardAtoms ?? null,
      r.reserveRewardAtoms ?? null,
      r.yieldRewardAtoms ?? null,
      r.baseRewardAtoms ?? null,
      r.feeAdjustmentAtoms ?? null,
    );
    idx += 11;
  }

  const sql = `INSERT INTO block_rewards (block_height, miner_reward, governance_reward, reserve_reward, yield_reward, miner_reward_atoms, governance_reward_atoms, reserve_reward_atoms, yield_reward_atoms, base_reward_atoms, fee_adjustment_atoms)
VALUES ${placeholders.join(", ")}
ON CONFLICT (block_height) DO UPDATE SET
  miner_reward = EXCLUDED.miner_reward,
  governance_reward = EXCLUDED.governance_reward,
  reserve_reward = EXCLUDED.reserve_reward,
  yield_reward = EXCLUDED.yield_reward,
  miner_reward_atoms = EXCLUDED.miner_reward_atoms,
  governance_reward_atoms = EXCLUDED.governance_reward_atoms,
  reserve_reward_atoms = EXCLUDED.reserve_reward_atoms,
  yield_reward_atoms = EXCLUDED.yield_reward_atoms,
  base_reward_atoms = EXCLUDED.base_reward_atoms,
  fee_adjustment_atoms = EXCLUDED.fee_adjustment_atoms`;

  await prisma.$executeRawUnsafe(sql, ...values);
}

export async function getBlockReward(blockHeight: number) {
  const prisma = getPrismaClient();
  return prisma.blockReward.findUnique({
    where: { blockHeight },
  });
}

export async function getBlockRewardRange(fromHeight: number, toHeight: number): Promise<Map<number, BlockRewardRecord>> {
  const prisma = getPrismaClient();
  const rows = await prisma.blockReward.findMany({
    where: {
      blockHeight: { gte: fromHeight, lte: toHeight },
    },
    orderBy: { blockHeight: "asc" },
  });
  const map = new Map<number, BlockRewardRecord>();
  for (const row of rows) {
    map.set(row.blockHeight, {
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
    });
  }
  return map;
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
