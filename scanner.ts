import express from "express";
import type { Request, Response } from "express";
import { aggregate } from "./aggregator";
import { getZYSPriceHistoryFromRedis, processZYSPriceHistory, scanPricingRecords } from "./pr";
import { scanTransactions } from "./tx";
import {
  AggregatedData,
  ProtocolStats,
  getAggregatedProtocolStatsFromRedis,
  getBlockProtocolStatsFromRedis,
  getLiveStats,
  getPricingRecordHeight,
  getRedisHeight,
  getTotalsFromRedis,
  getTransactionHeight,
  getReserveDiffs,
} from "./utils";
import {
  determineAPYHistory,
  determineHistoricalReturns,
  determineProjectedReturns,
  getAPYHistoryFromRedis,
  getHistoricalReturnsFromRedis,
  getProjectedReturnsFromRedis,
} from "./yield";
import { detectAndHandleReorg, resetScanner, retallyTotals, rollbackScanner } from "./rollback";
import dotenv from "dotenv";
import rateLimit from "express-rate-limit";
import redis from "./redis";

// Load environment variables from .env file
dotenv.config();

const VERSION_2_HF_V6_BLOCK_HEIGHT = 360000;
const WALKTHROUGH_MODE = process.env.WALKTHROUGH_MODE === "true";
const MAIN_SLEEP_MS = process.env.MAIN_SLEEP_MS ? parseInt(process.env.MAIN_SLEEP_MS) : 120000; // 2 min default;
const MAIN_PAUSE_MS = process.env.MAIN_PAUSE_MS ? parseInt(process.env.MAIN_PAUSE_MS) : 5000; // 5 sec default

let mainRunning = false;
async function main() {
  if (process.env.ONLINE !== "true") {
    console.log("ONLINE is set to false. Skipping main function execution.");
    return;
  }

  if (mainRunning) {
    console.log("Main already running, skipping this run");
    return;
  }
  console.log(`MAIN sleep is set to ${MAIN_SLEEP_MS} ms`);

  // wait for 5 seconds to allow for route calls default
  await new Promise((resolve) => setTimeout(resolve, MAIN_PAUSE_MS));
  mainRunning = true;

  // Check if the scanner is in a rollback state
  const isRollingBack = (await redis.get("scanner_rolling_back")) === "true";
  if (isRollingBack) {
    console.log("Scanner is rolling back, exiting main function");
    mainRunning = false;
    return;
  }

  await detectAndHandleReorg();
  console.log("---------| MAIN |-----------");
  await aggregate(); // needs to be first initially
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
  // get as of block height
  const latestScannerHeight = await getRedisHeight();
  const latestPricingHeight = await getPricingRecordHeight();
  const latestTxHeight = await getTransactionHeight();
  console.log("Scanner Height (protocol_stats/height_aggregator): ", latestScannerHeight);
  console.log("Pricing Records Height: ", latestPricingHeight);
  console.log("Transactions Height: ", latestTxHeight);
  console.log("---------| MAIN |-----------");
  mainRunning = false;
}

const app = express();
app.use(express.json());
const port = 4000;

// Apply rate limiter to all routes except "/rollback"
const rateLimiter = rateLimit({
  windowMs: 60 * 60 * 1000, // 1 hour
  max: 60, // Limit each IP to 60 requests per hour
  message: "Rate limited - Too many requests this hour, please try again later.",
});

// Middleware to allow local IPs to bypass rate limiting
app.use((req, res, next) => {
  const clientIp = req.ip;

  // Allow local IPs (IPv4 127.0.0.1, IPv6 ::1, or any local IPv4 mapped to IPv6 like ::ffff:127.0.0.1) to bypass rate limiting
  if (clientIp === "127.0.0.1" || clientIp === "::1" || clientIp === "::ffff:127.0.0.1") {
    return next();
  }

  // Apply rate limiting only to non-local IPs
  if (req.path !== "/rollback") {
    rateLimiter(req, res, next);
  } else {
    next();
  }
});

// Define routes
app.get("/", async (_, res: Response) => {
  res.send("zephyrdscanner reached");
});

app.get("/stats", async (req: Request, res: Response) => {
  console.log(`zephyrdscanner /stats called`);
  console.log(req.query);

  const scale = req.query.scale as "block" | "hour" | "day";
  const from = req.query.from as string;
  const to = req.query.to as string;

  let fields: string[] = [];

  // Only handle if fields is provided as a string
  if (typeof req.query.fields === "string") {
    fields = req.query.fields.split(","); // If it's a string, split it into an array
  }

  if (!scale) {
    res.status(400).send("scale query param is required");
    return;
  }

  if (scale !== "block" && scale !== "hour" && scale !== "day") {
    res.status(400).send("scale query param must be 'block', 'hour', or 'day'");
    return;
  }

  try {
    let result;

    if (scale === "block") {
      // Call getBlockProtocolStatsFromRedis for block-level data
      result = await getBlockProtocolStatsFromRedis(from, to, fields as (keyof ProtocolStats)[]);
    } else {
      // Call getAggregatedProtocolStatsFromRedis for hour or day-level data
      result = await getAggregatedProtocolStatsFromRedis(scale, from, to, fields as (keyof AggregatedData)[]);
    }

    res.status(200).json(result);
  } catch (error) {
    console.error("Error retrieving protocol stats:", error);
    res.status(500).send("Internal server error");
  }
});

app.get("/historicalreturns", async (req: Request, res: Response) => {
  console.log(`zephyrdscanner /historicalreturns called`);
  console.log(req.query);

  const test = !!req.query.test; // optional query param

  try {
    const result = await getHistoricalReturnsFromRedis(test);
    res.status(200).json(result);
  } catch (error) {
    console.error("Error retrieving historical returns:", error);
    res.status(500).send("Internal server error");
  }
});

app.get("/projectedreturns", async (req: Request, res: Response) => {
  console.log(`zephyrdscanner /projectedreturns called`);
  console.log(req.query);

  const test = !!req.query.test; // optional query param

  try {
    const result = await getProjectedReturnsFromRedis(test);
    res.status(200).json(result);
  } catch (error) {
    console.error("Error retrieving projected returns:", error);
    res.status(500).send("Internal server error");
  }
});

app.get("/livestats", async (req: Request, res: Response) => {
  const clientIp = req.ip;
  console.log(`zephyrdscanner /livestats called from ${clientIp}`);
  try {
    const result = await getLiveStats();
    res.status(200).json(result);
  } catch (error) {
    console.error("Error retrieving live stats:", error);
    res.status(500).send("Internal server error");
  }
});

app.get("/zyspricehistory", async (req, res) => {
  console.log(`zephyrdscanner /zyspricehistory called`);
  try {
    const result = await getZYSPriceHistoryFromRedis();
    res.status(200).json(result);
  } catch (error) {
    console.error("Error retrieving zys price history:", error);
    res.status(500).send("Internal server error");
  }
});

app.get("/apyhistory", async (req, res) => {
  console.log(`zephyrdscanner /apyhistory called`);
  try {
    const result = await getAPYHistoryFromRedis();
    res.status(200).json(result);
  } catch (error) {
    console.error("Error retrieving zys price history:", error);
    res.status(500).send("Internal server error");
  }
});

// Reserve vs stats diff
app.get("/reservediff", async (req: Request, res: Response) => {
  const clientIp = req.ip;

  if (clientIp !== "127.0.0.1" && clientIp !== "::1" && clientIp !== "::ffff:127.0.0.1") {
    console.log(`ip ${clientIp} tried to access /reservediff and was denied`);
    return res.status(403).send("Access denied to /reservediff. No Public Access.");
  }

  try {
    const result = await getReserveDiffs();
    res.status(200).json(result);
  } catch (error) {
    console.error("/reservediff - Error in getReserveDiffs:", error);
    res.status(500).send("Internal server error");
  }
});

// Protect /rollback route, make it non-public for non-local IPs
app.get("/rollback", async (req: Request, res: Response) => {
  const clientIp = req.ip;

  if (clientIp !== "127.0.0.1" && clientIp !== "::1" && clientIp !== "::ffff:127.0.0.1") {
    console.log(`ip ${clientIp} tried to access /rollback and was denied`);
    return res.status(403).send("Access denied to /rollback. No Public Access.");
  }

  mainRunning = true;
  console.log(`zephyrdscanner /rollback called`);
  console.log(req.query);

  const height = Number(req.query.height);
  if (!height) {
    res.status(400).send("height query param is required");
    return;
  }

  try {
    const result = await rollbackScanner(height);
    res.status(200).json(result);
  } catch (error) {
    console.error("/rollback - Error in rollbackScanner:", error);
    res.status(500).send("Internal server error");
  }

  mainRunning = false;
});

// Retally Totals
app.get("/retallytotals", async (req: Request, res: Response) => {
  const clientIp = req.ip;

  if (clientIp !== "127.0.0.1" && clientIp !== "::1" && clientIp !== "::ffff:127.0.0.1") {
    console.log(`ip ${clientIp} tried to access /retallytotals and was denied`);
    return res.status(403).send("Access denied to /retallytotals. No Public Access.");
  }

  console.log(`zephyrdscanner /retallytotals called`);

  try {
    await retallyTotals();
    res.status(200).send("Totals retallied successfully");
  } catch (error) {
    console.error("/retallytotals - Error in retallyTotals:", error);
    res.status(500).send("Internal server error");
  }
});

// Reset scanner
app.post("/reset", async (req: Request, res: Response) => {
  const clientIp = req.ip;

  if (clientIp !== "127.0.0.1" && clientIp !== "::1" && clientIp !== "::ffff:127.0.0.1") {
    console.log(`ip ${clientIp} tried to access /reset and was denied`);
    return res.status(403).send("Access denied to /reset. No Public Access.");
  }

  if (mainRunning) {
    return res.status(409).send("Scanner is busy. Try again later.");
  }

  mainRunning = true;
  const scope = req.query.scope === "full" ? "full" : "aggregation";

  console.log(`zephyrdscanner /reset called with scope=${scope}`);

  try {
    await resetScanner(scope);
    res.status(200).json({ status: "ok", scope });
  } catch (error) {
    console.error("/reset - Error in resetScanner:", error);
    res.status(500).send("Internal server error");
  }

  mainRunning = false;
});

// Redetermine APY History
app.get("/redetermineapyhistory", async (req: Request, res: Response) => {
  const clientIp = req.ip;

  if (clientIp !== "127.0.0.1" && clientIp !== "::1" && clientIp !== "::ffff:127.0.0.1") {
    console.log(`ip ${clientIp} tried to access /redetermineapyhistory and was denied`);
    return res.status(403).send("Access denied to /redetermineapyhistory. No Public Access.");
  }

  console.log(`zephyrdscanner /redetermineapyhistory called`);

  try {
    await determineAPYHistory(true); // true to reset all
    res.status(200).send("determineAPYHistory redetermined successfully");
  } catch (error) {
    console.error("/redetermineapyhistory - Error in retallyTotals:", error);
    res.status(500).send("Internal server error");
  }
});

if (!WALKTHROUGH_MODE) {
  app.listen(port, () => {
    console.log(`zephyrdscanner listening at http://localhost:${port} \n`);
  });

  // 2 min set interval for scanning default
  setInterval(async () => {
    await main();
  }, MAIN_SLEEP_MS);

  (async () => {
    await main();
  })();
} else {
  (async () => {
    await main();
    console.log("[walkthrough] Main loop completed, exiting");
    process.exit(0);
  })().catch((error) => {
    console.error("[walkthrough] Error during main loop", error);
    process.exit(1);
  });
}
