import express from "express";
import type { Request, Response } from "express";
import { aggregate } from "./aggregator";
import { scanPricingRecords } from "./pr";
import { scanTransactions } from "./tx";
import { getProtocolStatsFromRedis, getTotalsFromRedis } from "./utils";
import { determineHistoricalReturns, determineProjectedReturns, getHistoricalReturnsFromRedis, getProjectedReturnsFromRedis } from "./yield";

let mainRunning = false;
async function main() {
  if (mainRunning) {
    console.log("Main already running, skipping this run");
    return;
  }


  mainRunning = true;
  await aggregate(); // needs to be first initally
  console.log("--------------------");
  await scanPricingRecords();
  console.log("--------------------");
  await scanTransactions();
  console.log("--------------------");
  await aggregate();
  console.log("--------------------");
  await determineHistoricalReturns();
  console.log("--------------------");
  const totals = await getTotalsFromRedis();
  console.log(totals);
  console.log("--------------------");
  await determineHistoricalReturns()
  await determineProjectedReturns();
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

  if (!scale) {
    res.status(400).send("scale query param is required");
    return;
  }

  if (scale != "block" && scale != "hour" && scale != "day") {
    res.status(400).send("scale query param must be 'block', 'hour', or 'day'");
    return;
  }

  try {
    const result = await getProtocolStatsFromRedis(scale, from, to);
    res.status(200).json(result);
    // res.status(200).json({ scale, from, to });
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
