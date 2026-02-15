import { PrismaClient } from "@prisma/client";
import { execFileSync } from "child_process";

let testPrisma: PrismaClient | null = null;

export function getTestPrisma(): PrismaClient {
  if (!testPrisma) {
    testPrisma = new PrismaClient({
      datasources: {
        db: { url: process.env.DATABASE_URL },
      },
    });
  }
  return testPrisma;
}

export async function setupTestDatabase(): Promise<void> {
  execFileSync("bunx", ["prisma", "db", "push", "--force-reset", "--skip-generate"], {
    env: {
      ...process.env,
      // User-consented: this targets the zephyrdscanner_test database only
      PRISMA_USER_CONSENT_FOR_DANGEROUS_AI_ACTION: "Yes, allow it",
    },
    stdio: "pipe",
  });
}

const TRUNCATE_TABLES = [
  "pricing_records",
  "block_rewards",
  "transactions",
  "protocol_stats",
  "protocol_stats_hourly",
  "protocol_stats_daily",
  "reserve_snapshots",
  "reserve_mismatch_reports",
  "totals",
  "scanner_state",
  "live_stats_cache",
  "historical_returns",
  "projected_returns",
  "apy_history",
  "zys_price_history",
];

export async function resetTestData(): Promise<void> {
  const prisma = getTestPrisma();
  await prisma.$executeRawUnsafe(
    `TRUNCATE TABLE ${TRUNCATE_TABLES.join(", ")} CASCADE`
  );
}

export async function teardownTestDatabase(): Promise<void> {
  if (testPrisma) {
    await testPrisma.$disconnect();
    testPrisma = null;
  }
}
