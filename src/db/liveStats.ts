import { Prisma } from "@prisma/client";
import { LiveStats } from "../utils";
import { getPrismaClient } from "./index";

export async function upsertLiveStats(payload: LiveStats): Promise<void> {
  const prisma = getPrismaClient();
  await prisma.liveStatsCache.upsert({
    where: { id: 1 },
    update: { payload: payload as unknown as Prisma.InputJsonValue },
    create: { id: 1, payload: payload as unknown as Prisma.InputJsonValue },
  });
}

export async function getLiveStats(): Promise<LiveStats | null> {
  const prisma = getPrismaClient();
  const record = await prisma.liveStatsCache.findUnique({ where: { id: 1 } });
  return (record?.payload as unknown as LiveStats) ?? null;
}
