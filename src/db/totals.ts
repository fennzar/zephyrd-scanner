import { Prisma } from "@prisma/client";

import { getPrismaClient } from "./index";

export interface TotalsRecord {
  conversionTransactions: number;
  yieldConversionTransactions: number;
  mintReserveCount: number;
  mintReserveVolume: number;
  feesZephrsv: number;
  redeemReserveCount: number;
  redeemReserveVolume: number;
  feesZephusd: number;
  mintStableCount: number;
  mintStableVolume: number;
  redeemStableCount: number;
  redeemStableVolume: number;
  feesZeph: number;
  mintYieldCount: number;
  mintYieldVolume: number;
  feesZyield: number;
  redeemYieldCount: number;
  redeemYieldVolume: number;
  feesZephusdYield: number;
  minerReward: number;
  governanceReward: number;
  reserveReward: number;
  yieldReward: number;
}

export const defaultTotals: TotalsRecord = {
  conversionTransactions: 0,
  yieldConversionTransactions: 0,
  mintReserveCount: 0,
  mintReserveVolume: 0,
  feesZephrsv: 0,
  redeemReserveCount: 0,
  redeemReserveVolume: 0,
  feesZephusd: 0,
  mintStableCount: 0,
  mintStableVolume: 0,
  redeemStableCount: 0,
  redeemStableVolume: 0,
  feesZeph: 0,
  mintYieldCount: 0,
  mintYieldVolume: 0,
  feesZyield: 0,
  redeemYieldCount: 0,
  redeemYieldVolume: 0,
  feesZephusdYield: 0,
  minerReward: 0,
  governanceReward: 0,
  reserveReward: 0,
  yieldReward: 0,
};

function toPrismaTotalsInput(values: TotalsRecord): Prisma.TotalsUpsertArgs["create"] {
  return {
    id: 1,
    conversionTransactions: values.conversionTransactions,
    yieldConversionTransactions: values.yieldConversionTransactions,
    mintReserveCount: values.mintReserveCount,
    mintReserveVolume: values.mintReserveVolume,
    feesZephrsv: values.feesZephrsv,
    redeemReserveCount: values.redeemReserveCount,
    redeemReserveVolume: values.redeemReserveVolume,
    feesZephusd: values.feesZephusd,
    mintStableCount: values.mintStableCount,
    mintStableVolume: values.mintStableVolume,
    redeemStableCount: values.redeemStableCount,
    redeemStableVolume: values.redeemStableVolume,
    feesZeph: values.feesZeph,
    mintYieldCount: values.mintYieldCount,
    mintYieldVolume: values.mintYieldVolume,
    feesZyield: values.feesZyield,
    redeemYieldCount: values.redeemYieldCount,
    redeemYieldVolume: values.redeemYieldVolume,
    feesZephusdYield: values.feesZephusdYield,
    minerReward: values.minerReward,
    governanceReward: values.governanceReward,
    reserveReward: values.reserveReward,
    yieldReward: values.yieldReward,
  };
}

export async function setTotals(values: TotalsRecord): Promise<void> {
  const prisma = getPrismaClient();
  await prisma.totals.upsert({
    where: { id: 1 },
    update: toPrismaTotalsInput(values),
    create: toPrismaTotalsInput(values),
  });
}

type TotalsDelta = Partial<TotalsRecord>;

function buildIncrement(delta: TotalsDelta): Prisma.TotalsUpdateInput {
  const update: Prisma.TotalsUpdateInput = {};
  const assign = (key: keyof TotalsRecord, field: keyof Prisma.TotalsUpdateInput) => {
    const value = delta[key];
    if (value && value !== 0) {
      update[field] = { increment: value };
    }
  };

  assign("conversionTransactions", "conversionTransactions");
  assign("yieldConversionTransactions", "yieldConversionTransactions");
  assign("mintReserveCount", "mintReserveCount");
  assign("mintReserveVolume", "mintReserveVolume");
  assign("feesZephrsv", "feesZephrsv");
  assign("redeemReserveCount", "redeemReserveCount");
  assign("redeemReserveVolume", "redeemReserveVolume");
  assign("feesZephusd", "feesZephusd");
  assign("mintStableCount", "mintStableCount");
  assign("mintStableVolume", "mintStableVolume");
  assign("redeemStableCount", "redeemStableCount");
  assign("redeemStableVolume", "redeemStableVolume");
  assign("feesZeph", "feesZeph");
  assign("mintYieldCount", "mintYieldCount");
  assign("mintYieldVolume", "mintYieldVolume");
  assign("feesZyield", "feesZyield");
  assign("redeemYieldCount", "redeemYieldCount");
  assign("redeemYieldVolume", "redeemYieldVolume");
  assign("feesZephusdYield", "feesZephusdYield");
  assign("minerReward", "minerReward");
  assign("governanceReward", "governanceReward");
  assign("reserveReward", "reserveReward");
  assign("yieldReward", "yieldReward");

  return update;
}

export async function incrementTotals(delta: TotalsDelta): Promise<void> {
  const prisma = getPrismaClient();
  const update = buildIncrement(delta);
  if (Object.keys(update).length === 0) {
    return;
  }
  await prisma.totals.upsert({
    where: { id: 1 },
    update,
    create: toPrismaTotalsInput({ ...defaultTotals, ...delta }),
  });
}

export async function getTotals(): Promise<TotalsRecord | null> {
  const prisma = getPrismaClient();
  const row = await prisma.totals.findUnique({ where: { id: 1 } });
  if (!row) {
    return null;
  }
  return {
    conversionTransactions: row.conversionTransactions,
    yieldConversionTransactions: row.yieldConversionTransactions,
    mintReserveCount: row.mintReserveCount,
    mintReserveVolume: row.mintReserveVolume,
    feesZephrsv: row.feesZephrsv,
    redeemReserveCount: row.redeemReserveCount,
    redeemReserveVolume: row.redeemReserveVolume,
    feesZephusd: row.feesZephusd,
    mintStableCount: row.mintStableCount,
    mintStableVolume: row.mintStableVolume,
    redeemStableCount: row.redeemStableCount,
    redeemStableVolume: row.redeemStableVolume,
    feesZeph: row.feesZeph,
    mintYieldCount: row.mintYieldCount,
    mintYieldVolume: row.mintYieldVolume,
    feesZyield: row.feesZyield,
    redeemYieldCount: row.redeemYieldCount,
    redeemYieldVolume: row.redeemYieldVolume,
    feesZephusdYield: row.feesZephusdYield,
    minerReward: row.minerReward,
    governanceReward: row.governanceReward,
    reserveReward: row.reserveReward,
    yieldReward: row.yieldReward,
  };
}
