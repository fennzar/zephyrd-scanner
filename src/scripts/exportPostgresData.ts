/* eslint-disable no-console */
import dotenv from "dotenv";
import fs from "node:fs/promises";
import path from "node:path";

dotenv.config();
process.env.DATA_STORE = process.env.DATA_STORE ?? "postgres";

import { getPrismaClient } from "../db";

const prisma = getPrismaClient();

interface CliOptions {
  dir?: string;
  pretty: boolean;
}

function parseArgs(): CliOptions {
  const args = process.argv.slice(2);
  let dir: string | undefined;
  let pretty = false;
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === "--dir" || arg === "-d") {
      dir = args[++i];
    } else if (arg === "--pretty" || arg === "-p") {
      pretty = true;
    }
  }
  return { dir, pretty };
}

async function writeJson(filePath: string, data: unknown, pretty: boolean) {
  await fs.writeFile(filePath, JSON.stringify(data, null, pretty ? 2 : undefined));
}

async function exportTable(name: string, rows: unknown[], dir: string, pretty: boolean) {
  const file = path.join(dir, `${name}.json`);
  await writeJson(file, rows, pretty);
  console.log(`[export-sql] ${name} (${rows.length} rows)`);
}

async function exportDatabase(options: CliOptions) {
  const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
  const outDir =
    options.dir ??
    path.resolve(process.cwd(), "exports", "sql", `postgres_export_${timestamp}`);
  await fs.mkdir(outDir, { recursive: true });

  await exportTable("pricing_records", await prisma.pricingRecord.findMany(), outDir, options.pretty);
  await exportTable("block_rewards", await prisma.blockReward.findMany(), outDir, options.pretty);
  await exportTable("transactions", await prisma.conversionTransaction.findMany(), outDir, options.pretty);
  await exportTable("protocol_stats", await prisma.protocolStatsBlock.findMany(), outDir, options.pretty);
  await exportTable("protocol_stats_hourly", await prisma.protocolStatsHourly.findMany(), outDir, options.pretty);
  await exportTable("protocol_stats_daily", await prisma.protocolStatsDaily.findMany(), outDir, options.pretty);
  await exportTable("reserve_snapshots", await prisma.reserveSnapshot.findMany(), outDir, options.pretty);
  await exportTable("reserve_mismatch_reports", await prisma.reserveMismatchReport.findMany(), outDir, options.pretty);
  await exportTable("live_stats_cache", await prisma.liveStatsCache.findMany(), outDir, options.pretty);
  await exportTable("totals", await prisma.totals.findMany(), outDir, options.pretty);
  await exportTable("scanner_state", await prisma.scannerState.findMany(), outDir, options.pretty);
  await exportTable("historical_returns", await prisma.historicalReturn.findMany(), outDir, options.pretty);
  await exportTable("projected_returns", await prisma.projectedReturn.findMany(), outDir, options.pretty);
  await exportTable("apy_history", await prisma.apyHistoryEntry.findMany(), outDir, options.pretty);
  await exportTable("zys_price_history", await prisma.zysPriceHistoryEntry.findMany(), outDir, options.pretty);

  console.log(`[export-sql] Export finished at ${outDir}`);
}

async function main() {
  const options = parseArgs();
  try {
    await prisma.$connect();
    await exportDatabase(options);
  } catch (error) {
    console.error("[export-sql] Failed", error);
    process.exitCode = 1;
  } finally {
    await prisma.$disconnect();
  }
}

void main();
