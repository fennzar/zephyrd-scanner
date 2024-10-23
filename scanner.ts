import express from "express";
import type { Request, Response } from "express";
import { aggregate } from "./aggregator";
import { getZYSPriceHistoryFromRedis, processZYSPriceHistory, scanPricingRecords } from "./pr";
import { scanTransactions } from "./tx";
import { getLiveStats, getProtocolStatsFromRedis, getTotalsFromRedis } from "./utils";
import { determineHistoricalReturns, determineProjectedReturns, getHistoricalReturnsFromRedis, getProjectedReturnsFromRedis } from "./yield";
import { detectAndHandleReorg, rollbackScanner } from "./rollback";
import dotenv from 'dotenv';

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
    console.error("Error retrieving historical returns:", error);
    res.status(500).send("Internal server error");
  }

});

app.get("/livestats", async (_, res: Response) => {
  console.log(`zephyrdscanner /livestats called`);
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

app.get("/rollback", async (req: Request, res: Response) => {
  mainRunning = true;
  console.log(`zephyrdscanner /rollback called`);
  console.log(req.query);

  const height = Number(req.query.height)
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
