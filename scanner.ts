import express from "express";
import type { Request, Response } from "express";
import { aggregate } from "./aggregator";
import { getZYSPriceHistoryFromRedis, processZYSPriceHistory, scanPricingRecords } from "./pr";
import { scanTransactions } from "./tx";
import { getLiveStats, getProtocolStatsFromRedis, getTotalsFromRedis } from "./utils";
import { determineHistoricalReturns, determineProjectedReturns, getHistoricalReturnsFromRedis, getProjectedReturnsFromRedis } from "./yield";
import { detectAndHandleReorg, rollbackScanner } from "./rollback";
import dotenv from 'dotenv';
import rateLimit from 'express-rate-limit';

// Load environment variables from .env file
dotenv.config();

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

  mainRunning = true;
  // wait for 3 seconds to allow for route calls
  await new Promise((resolve) => setTimeout(resolve, 3000));
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
  await determineHistoricalReturns();
  console.log("---------| MAIN |-----------");
  await determineHistoricalReturns();
  console.log("---------| MAIN |-----------");
  await determineProjectedReturns();
  console.log("---------| MAIN |-----------");
  await processZYSPriceHistory();
  console.log("---------| MAIN |-----------");
  const totals = await getTotalsFromRedis();
  console.log(totals);
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

  // Allow local IPs (IPv4 127.0.0.1 or IPv6 ::1) to bypass rate limiting
  if (clientIp === '127.0.0.1' || clientIp === '::1' || clientIp !== '::ffff:127.0.0.1') {
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

  const scale = req.query.scale as string;
  const from = req.query.from as string;
  const to = req.query.to as string;

  let fields: string[] = [];

  // Only handle if fields is provided as a string
  if (typeof req.query.fields === 'string') {
    fields = req.query.fields.split(",");  // If it's a string, split it into an array
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
    const result = await getProtocolStatsFromRedis(scale, from, to, fields);
    res.status(200).json(result);
  } catch (error) {
    console.error("Error retrieving protocol stats:", error);
    res.status(500).send("Internal server error");
  }
});

app.get("/historicalreturns", async (req: Request, res: Response) => {
  console.log(`zephyrdscanner /historicalreturns called`);
  console.log(req.query);

  const test = !!req.query.test // optional query param

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

  const test = !!req.query.test // optional query param

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

// Protect /rollback route, make it non-public for non-local IPs
app.get("/rollback", async (req: Request, res: Response) => {
  const clientIp = req.ip;

  if (clientIp !== '127.0.0.1' && clientIp !== '::1' && clientIp !== '::ffff:127.0.0.1') {
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

app.listen(port, () => {
  console.log(`zephyrdscanner listening at http://localhost:${port} \n`);
});

// 5 min set interval for scanning
setInterval(async () => {
  await main();
}, 300000);

(async () => {
  main();
})();
