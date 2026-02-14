import path from "node:path";
import { spawn } from "node:child_process";
import { createRequire } from "node:module";
import dotenv from "dotenv";

import { aggregate } from "./aggregator";
import { getZYSPriceHistoryFromRedis, processZYSPriceHistory, scanPricingRecords } from "./pr";
import { scanTransactions } from "./tx";
import {
  getPricingRecordHeight,
  getScannerHeight,
  getLatestProtocolStats,
  getLatestReserveSnapshot,
  getTotalsFromRedis,
  getTransactionHeight,
  refreshLiveStatsCache,
} from "./utils";
import {
  determineAPYHistory,
  determineHistoricalReturns,
  determineProjectedReturns,
} from "./yield";
import { detectAndHandleReorg } from "./rollback";
import redis from "./redis";
import { logScannerHealth, logTotals, TotalsSummary } from "./logger";
import { logRuntimeConfig, usePostgres, useRedis } from "./config";
import { stores } from "./storage/factory";

dotenv.config();
logRuntimeConfig("scanner");

const AUTO_EXPORT_ENABLED = process.env.AUTO_EXPORT_ENABLED !== "false";
const AUTO_EXPORT_INTERVAL = (() => {
  const value = Number(process.env.AUTO_EXPORT_INTERVAL ?? "100000");
  return Number.isFinite(value) && value > 0 ? value : 100000;
})();
const AUTO_EXPORT_DIR = process.env.AUTO_EXPORT_DIR?.trim() ?? "";
const AUTO_EXPORT_PRETTY = process.env.AUTO_EXPORT_PRETTY === "true";
const AUTO_EXPORT_KEY = "auto_export:last_height";
const AUTO_EXPORT_LOG_PREFIX = "[auto-export]";

const requireForCli = createRequire(__filename);
const TSX_CLI_PATH = requireForCli.resolve("tsx/cli");
const EXPORT_SCRIPT_PATH = path.join(__dirname, "scripts", "exportRedisData.ts");
const BACKUP_SCRIPT_PATH = path.join(__dirname, "scripts", "backupPostgres.ts");

const VERSION_2_HF_V6_BLOCK_HEIGHT = 360000;
const WALKTHROUGH_MODE = process.env.WALKTHROUGH_MODE === "true";
const MAIN_SLEEP_MS = process.env.MAIN_SLEEP_MS ? parseInt(process.env.MAIN_SLEEP_MS, 10) : 120000;
const MAIN_PAUSE_MS = process.env.MAIN_PAUSE_MS ? parseInt(process.env.MAIN_PAUSE_MS, 10) : 5000;
const SCANNER_PAUSE_KEY = "scanner_paused";

let mainRunning = false;

async function delay(ms: number) {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

export async function runScannerCycle(): Promise<void> {
  if (process.env.ONLINE !== "true") {
    return;
  }

  if (mainRunning) {
    console.log("Scanner cycle already running, skipping this trigger");
    return;
  }

  mainRunning = true;

  try {
    console.log(`MAIN sleep is set to ${MAIN_SLEEP_MS} ms`);

    await delay(MAIN_PAUSE_MS);

    const isPaused = (await redis.get(SCANNER_PAUSE_KEY)) === "true";
    if (isPaused) {
      console.log("Scanner paused by API request, skipping this cycle");
      return;
    }

    const isRollingBack = (await redis.get("scanner_rolling_back")) === "true";
    if (isRollingBack) {
      console.log("Scanner is rolling back, skipping this cycle");
      return;
    }

    await detectAndHandleReorg();
    console.log("---------| MAIN |-----------");
    await aggregate();
    console.log("---------| MAIN |-----------");
    await scanPricingRecords();
    console.log("---------| MAIN |-----------");
    await scanTransactions();
    console.log("---------| MAIN |-----------");
    await aggregate();
    console.log("---------| MAIN |-----------");

    const scannerHeight = await getScannerHeight();

    if (scannerHeight >= VERSION_2_HF_V6_BLOCK_HEIGHT) {
      await determineHistoricalReturns();
      console.log("---------| MAIN |-----------");
      await determineProjectedReturns();
      console.log("---------| MAIN |-----------");
      await processZYSPriceHistory();
      console.log("---------| MAIN |-----------");
      await determineAPYHistory();
      console.log("---------| MAIN |-----------");
    } else {
      console.log("Skipping yield analytics – scanner height below V2 fork");
      console.log("---------| MAIN |-----------");
    }

    const totals = await getTotalsFromRedis();
    let totalsSummary: TotalsSummary | null = null;
    if (totals) {
      totalsSummary = logTotals(totals);
    }
    const latestScannerHeight = await getScannerHeight();
    const latestPricingHeight = await getPricingRecordHeight();
    const latestTxHeight = await getTransactionHeight();
    console.log(
      `[heights] scanner=${latestScannerHeight.toLocaleString("en-US")} | pricing=${latestPricingHeight.toLocaleString(
        "en-US"
      )} | tx=${latestTxHeight.toLocaleString("en-US")}`
    );
    const [latestStats, latestSnapshot] = await Promise.all([
      getLatestProtocolStats(),
      getLatestReserveSnapshot(),
    ]);
    await logScannerHealth(totalsSummary, latestStats, latestSnapshot);
    const refreshedLiveStats = await refreshLiveStatsCache();
    if (!refreshedLiveStats) {
      console.warn("runScannerCycle: Unable to refresh live stats cache");
    }
    await maybeAutoExport(latestScannerHeight);
    console.log("---------| MAIN |-----------");
  } catch (error) {
    console.error("Error running scanner cycle:", error);
  } finally {
    mainRunning = false;
  }
}

async function maybeAutoExport(currentHeight: number): Promise<void> {
  if (!AUTO_EXPORT_ENABLED) {
    return;
  }
  if (!Number.isFinite(currentHeight) || currentHeight <= 0) {
    return;
  }
  if (!Number.isFinite(AUTO_EXPORT_INTERVAL) || AUTO_EXPORT_INTERVAL <= 0) {
    return;
  }

  const highestMilestone = Math.floor(currentHeight / AUTO_EXPORT_INTERVAL) * AUTO_EXPORT_INTERVAL;
  if (highestMilestone === 0) {
    return;
  }

  const lastHeightRaw = await stores.scannerState.get(AUTO_EXPORT_KEY);
  let lastHeight = lastHeightRaw ? Number(lastHeightRaw) : 0;
  if (!Number.isFinite(lastHeight) || lastHeight < 0) {
    lastHeight = 0;
  }

  if (highestMilestone <= lastHeight) {
    return;
  }

  const milestone = highestMilestone;

  try {
    console.log(`${AUTO_EXPORT_LOG_PREFIX} Triggering export for block ${milestone}`);
    await runAutoExport(milestone);
    await stores.scannerState.set(AUTO_EXPORT_KEY, milestone.toString());
    console.log(`${AUTO_EXPORT_LOG_PREFIX} Completed export for block ${milestone}`);
  } catch (error) {
    console.error(`${AUTO_EXPORT_LOG_PREFIX} Failed to export at block ${milestone}`, error);
  }
}

async function runAutoExport(milestone: number): Promise<void> {
  const tasks: Array<Promise<void>> = [];
  if (usePostgres()) {
    tasks.push(runPostgresBackup(milestone));
  }
  if (useRedis()) {
    tasks.push(runRedisExport(milestone));
  }

  if (tasks.length === 0) {
    console.log(`${AUTO_EXPORT_LOG_PREFIX} No storage targets enabled, skipping export`);
    return;
  }

  await Promise.all(tasks);
}

async function runRedisExport(milestone: number): Promise<void> {
  const cliArgs = [TSX_CLI_PATH, EXPORT_SCRIPT_PATH];
  if (AUTO_EXPORT_DIR) {
    cliArgs.push("--dir", AUTO_EXPORT_DIR);
  }
  if (AUTO_EXPORT_PRETTY) {
    cliArgs.push("--pretty");
  }
  if (Number.isFinite(milestone) && milestone > 0) {
    cliArgs.push("--tag", `milestone-${milestone}`);
  }

  const child = spawn(process.execPath, cliArgs, {
    stdio: "inherit",
    env: { ...process.env },
  });

  await new Promise<void>((resolve, reject) => {
    child.on("exit", (code, signal) => {
      if (code === 0) {
        resolve();
      } else {
        reject(new Error(`Redis export exited with ${signal ?? `code ${code}`}`));
      }
    });
    child.on("error", reject);
  });
}

async function runPostgresBackup(milestone: number): Promise<void> {
  const cliArgs = [TSX_CLI_PATH, BACKUP_SCRIPT_PATH];
  if (Number.isFinite(milestone) && milestone > 0) {
    cliArgs.push("--tag", `milestone-${milestone}`);
  }

  const child = spawn(process.execPath, cliArgs, {
    stdio: "inherit",
    env: { ...process.env },
  });

  await new Promise<void>((resolve, reject) => {
    child.on("exit", (code, signal) => {
      if (code === 0) {
        resolve();
      } else {
        reject(new Error(`Postgres backup exited with ${signal ?? `code ${code}`}`));
      }
    });
    child.on("error", reject);
  });
}

export async function startScanner(): Promise<void> {
  if (WALKTHROUGH_MODE) {
    try {
      await runScannerCycle();
      console.log("[walkthrough] Main loop completed, exiting");
      process.exit(0);
    } catch (error) {
      console.error("[walkthrough] Error during main loop", error);
      process.exit(1);
    }
    return;
  }

  console.log("Starting scanner loop...");

  const invoke = async () => {
    try {
      await runScannerCycle();
    } catch (error) {
      console.error("Unhandled error in scanner cycle:", error);
    }
  };

  await invoke();

  setInterval(invoke, MAIN_SLEEP_MS);
}

process.on("SIGTERM", async () => {
  console.log("Scanner process received SIGTERM, shutting down gracefully...");
  await gracefulShutdown();
});

process.on("SIGINT", async () => {
  console.log("Scanner process received SIGINT, shutting down gracefully...");
  await gracefulShutdown();
});

let isShuttingDown = false;

async function gracefulShutdown(): Promise<void> {
  if (isShuttingDown) {
    console.log("Shutdown already in progress...");
    return;
  }
  isShuttingDown = true;

  try {
    // Close Prisma connection to prevent zombie PostgreSQL sessions
    console.log("Closing Prisma connection...");
    const { disconnectPrisma } = await import("./db");
    await disconnectPrisma();
    console.log("Prisma connection closed.");

    // Close Redis connection
    console.log("Closing Redis connection...");
    await redis.quit();
    console.log("Redis connection closed.");
  } catch (error) {
    console.error("Error during graceful shutdown:", error);
  }

  console.log("Graceful shutdown complete.");
  process.exit(0);
}

if (require.main === module) {
  startScanner().catch((error) => {
    console.error("Failed to start scanner", error);
    process.exit(1);
  });
}

