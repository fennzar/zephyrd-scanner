import dotenv from "dotenv";

import { aggregate } from "./aggregator";
import { getZYSPriceHistoryFromRedis, processZYSPriceHistory, scanPricingRecords } from "./pr";
import { scanTransactions } from "./tx";
import {
  getPricingRecordHeight,
  getRedisHeight,
  getTotalsFromRedis,
  getTransactionHeight,
} from "./utils";
import {
  determineAPYHistory,
  determineHistoricalReturns,
  determineProjectedReturns,
} from "./yield";
import { detectAndHandleReorg } from "./rollback";
import redis from "./redis";

dotenv.config();

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

    const scannerHeight = await getRedisHeight();

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
    console.log(totals);
    const latestScannerHeight = await getRedisHeight();
    const latestPricingHeight = await getPricingRecordHeight();
    const latestTxHeight = await getTransactionHeight();
    console.log("Scanner Height (protocol_stats/height_aggregator): ", latestScannerHeight);
    console.log("Pricing Records Height: ", latestPricingHeight);
    console.log("Transactions Height: ", latestTxHeight);
    console.log("---------| MAIN |-----------");
  } catch (error) {
    console.error("Error running scanner cycle:", error);
  } finally {
    mainRunning = false;
  }
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

process.on("SIGTERM", () => {
  console.log("Scanner process received SIGTERM, shutting down...");
  process.exit(0);
});

if (require.main === module) {
  startScanner().catch((error) => {
    console.error("Failed to start scanner", error);
    process.exit(1);
  });
}

