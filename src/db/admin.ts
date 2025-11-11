import { usePostgres } from "../config";
import { stores } from "../storage/factory";
import { getPrismaClient } from "./index";

const prisma = getPrismaClient();

async function resetScannerStateValues() {
  await Promise.all([
    stores.scannerState.set("height_aggregator", "0"),
    stores.scannerState.set("timestamp_aggregator_hourly", "0"),
    stores.scannerState.set("timestamp_aggregator_daily", "0"),
    stores.scannerState.set("height_prs", "0"),
    stores.scannerState.set("height_txs", "0"),
  ]);
}

export async function clearPostgresAggregationState(): Promise<void> {
  if (!usePostgres()) {
    return;
  }
  await prisma.$transaction([
    prisma.protocolStatsBlock.deleteMany(),
    prisma.protocolStatsHourly.deleteMany(),
    prisma.protocolStatsDaily.deleteMany(),
    prisma.historicalReturn.deleteMany(),
    prisma.projectedReturn.deleteMany(),
    prisma.apyHistoryEntry.deleteMany(),
    prisma.zysPriceHistoryEntry.deleteMany(),
    prisma.liveStatsCache.deleteMany(),
  ]);
  await resetScannerStateValues();
}

export async function truncatePostgresData(): Promise<void> {
  if (!usePostgres()) {
    return;
  }

  await prisma.$transaction([
    prisma.conversionTransaction.deleteMany(),
    prisma.blockReward.deleteMany(),
    prisma.pricingRecord.deleteMany(),
    prisma.protocolStatsBlock.deleteMany(),
    prisma.protocolStatsHourly.deleteMany(),
    prisma.protocolStatsDaily.deleteMany(),
    prisma.reserveSnapshot.deleteMany(),
    prisma.reserveMismatchReport.deleteMany(),
    prisma.liveStatsCache.deleteMany(),
    prisma.totals.deleteMany(),
    prisma.scannerState.deleteMany(),
    prisma.historicalReturn.deleteMany(),
    prisma.projectedReturn.deleteMany(),
    prisma.apyHistoryEntry.deleteMany(),
    prisma.zysPriceHistoryEntry.deleteMany(),
  ]);

  await resetScannerStateValues();
}
