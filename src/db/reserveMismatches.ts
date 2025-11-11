import { Prisma } from "@prisma/client";

import { ReserveDiffReport } from "../utils";
import { getPrismaClient } from "./index";

function toJson(value: unknown): Prisma.InputJsonValue {
  return value as Prisma.InputJsonValue;
}

export async function upsertReserveMismatch(report: ReserveDiffReport): Promise<void> {
  const prisma = getPrismaClient();
  await prisma.reserveMismatchReport.upsert({
    where: { blockHeight: report.block_height },
    update: {
      reserveHeight: report.reserve_height,
      mismatch: report.mismatch,
      source: report.source,
      sourceHeight: report.source_height,
      snapshotPath: report.snapshot_path,
      diffs: toJson(report.diffs),
    },
    create: {
      blockHeight: report.block_height,
      reserveHeight: report.reserve_height,
      mismatch: report.mismatch,
      source: report.source,
      sourceHeight: report.source_height,
      snapshotPath: report.snapshot_path,
      diffs: toJson(report.diffs),
    },
  });
}

export async function deleteReserveMismatch(blockHeight: number): Promise<void> {
  const prisma = getPrismaClient();
  await prisma.reserveMismatchReport.deleteMany({
    where: { blockHeight },
  });
}

export async function getReserveMismatch(blockHeight: number): Promise<ReserveDiffReport | null> {
  const prisma = getPrismaClient();
  const row = await prisma.reserveMismatchReport.findUnique({
    where: { blockHeight },
  });
  if (!row) {
    return null;
  }
  return {
    block_height: row.blockHeight,
    reserve_height: row.reserveHeight,
    mismatch: row.mismatch,
    source: row.source as ReserveDiffReport["source"],
    source_height: row.sourceHeight ?? undefined,
    snapshot_path: row.snapshotPath ?? undefined,
    diffs: row.diffs as unknown as ReserveDiffReport["diffs"],
  };
}
