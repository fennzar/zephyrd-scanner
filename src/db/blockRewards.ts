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
