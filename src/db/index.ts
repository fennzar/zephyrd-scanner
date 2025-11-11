import { PrismaClient } from "@prisma/client";
import { usePostgres } from "../config";

let prisma: PrismaClient | null = null;

export function getPrismaClient(): PrismaClient {
  if (!usePostgres()) {
    throw new Error("Postgres data store disabled (DATA_STORE=redis).");
  }
  if (!prisma) {
    prisma = new PrismaClient();
  }
  return prisma;
}

export async function disconnectPrisma(): Promise<void> {
  if (prisma) {
    await prisma.$disconnect();
    prisma = null;
  }
}
