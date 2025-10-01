import express, { type Request, type Response } from "express";
import rateLimit from "express-rate-limit";
import dotenv from "dotenv";
import type { Server } from "node:http";

import {
  AggregatedData,
  ProtocolStats,
  getAggregatedProtocolStatsFromRedis,
  getBlockProtocolStatsFromRedis,
  getLiveStats,
  getReserveDiffs,
} from "./utils";
import {
  determineAPYHistory,
  getAPYHistoryFromRedis,
  getHistoricalReturnsFromRedis,
  getProjectedReturnsFromRedis,
} from "./yield";
import { getZYSPriceHistoryFromRedis } from "./pr";
import { resetScanner, retallyTotals, rollbackScanner } from "./rollback";
import redis from "./redis";

dotenv.config();

const PORT = Number(process.env.PORT ?? 4000);
const WALKTHROUGH_MODE = process.env.WALKTHROUGH_MODE === "true";
const LOCAL_IPS = new Set(["127.0.0.1", "::1", "::ffff:127.0.0.1"]);
const SCANNER_PAUSE_KEY = "scanner_paused";

function isLocalIp(ip: string): boolean {
  return LOCAL_IPS.has(ip);
}

function getClientIp(req: Request): string {
  return req.ip ?? "";
}

function getRequestPath(req: Request): string {
  return req.path ?? req.originalUrl ?? req.url ?? "";
}

function ensureLocalAccess(req: Request, res: Response): boolean {
  const clientIp = getClientIp(req);
  if (isLocalIp(clientIp)) {
    return true;
  }

  const requestPath = getRequestPath(req);
  console.log(`ip ${clientIp} attempted to access ${requestPath} and was denied`);
  res.status(403).send(`Access denied to ${requestPath}. No Public Access.`);
  return false;
}

export function createApp() {
  const app = express();
  app.use(express.json());

  const rateLimiter = rateLimit({
    windowMs: 60 * 60 * 1000,
    max: 60,
    message: "Rate limited - Too many requests this hour, please try again later.",
  });

  app.use((req, res, next) => {
    const clientIp = getClientIp(req);
    const requestPath = getRequestPath(req);

    if (!isLocalIp(clientIp) && requestPath !== "/rollback") {
      rateLimiter(req, res, next);
      return;
    }
    next();
  });

  let exclusiveRouteInProgress = false;

  async function acquireExclusiveLock(res: Response): Promise<boolean> {
    if (exclusiveRouteInProgress) {
      res.status(409).send("Scanner is busy. Try again later.");
      return false;
    }

    try {
      await redis.set(SCANNER_PAUSE_KEY, "true");
    } catch (error) {
      console.error("Failed to pause scanner loop", error);
      res.status(500).send("Unable to pause scanner loop");
      return false;
    }

    exclusiveRouteInProgress = true;
    return true;
  }

  async function releaseExclusiveLock() {
    exclusiveRouteInProgress = false;
    try {
      await redis.del(SCANNER_PAUSE_KEY);
    } catch (error) {
      console.error("Failed to resume scanner loop", error);
    }
  }

  redis.del(SCANNER_PAUSE_KEY).catch((error) => {
    console.warn("Unable to clear scanner pause flag on server start", error);
  });

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
    if (typeof req.query.fields === "string") {
      fields = req.query.fields.split(",");
    }

    if (!scale) {
      res.status(400).send("scale query param is required");
      return;
    }

    if (!["block", "hour", "day"].includes(scale)) {
      res.status(400).send("scale query param must be 'block', 'hour', or 'day'");
      return;
    }

    try {
      const result =
        scale === "block"
          ? await getBlockProtocolStatsFromRedis(from, to, fields as (keyof ProtocolStats)[])
          : await getAggregatedProtocolStatsFromRedis(scale, from, to, fields as (keyof AggregatedData)[]);
      res.status(200).json(result);
    } catch (error) {
      console.error("Error retrieving protocol stats:", error);
      res.status(500).send("Internal server error");
    }
  });

  app.get("/historicalreturns", async (req: Request, res: Response) => {
    console.log(`zephyrdscanner /historicalreturns called`);
    console.log(req.query);

    const test = !!req.query.test;

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

    const test = !!req.query.test;

    try {
      const result = await getProjectedReturnsFromRedis(test);
      res.status(200).json(result);
    } catch (error) {
      console.error("Error retrieving projected returns:", error);
      res.status(500).send("Internal server error");
    }
  });

  app.get("/livestats", async (req: Request, res: Response) => {
    console.log(`zephyrdscanner /livestats called from ${getClientIp(req)}`);
    try {
      const result = await getLiveStats();
      res.status(200).json(result);
    } catch (error) {
      console.error("Error retrieving live stats:", error);
      res.status(500).send("Internal server error");
    }
  });

  app.get("/zyspricehistory", async (_req, res) => {
    console.log(`zephyrdscanner /zyspricehistory called`);
    try {
      const result = await getZYSPriceHistoryFromRedis();
      res.status(200).json(result);
    } catch (error) {
      console.error("Error retrieving zys price history:", error);
      res.status(500).send("Internal server error");
    }
  });

  app.get("/apyhistory", async (_req, res) => {
    console.log(`zephyrdscanner /apyhistory called`);
    try {
      const result = await getAPYHistoryFromRedis();
      res.status(200).json(result);
    } catch (error) {
      console.error("Error retrieving apy history:", error);
      res.status(500).send("Internal server error");
    }
  });

  app.get("/reservediff", async (req: Request, res: Response) => {
    if (!ensureLocalAccess(req, res)) {
      return;
    }

    try {
      const result = await getReserveDiffs();
      res.status(200).json(result);
    } catch (error) {
      console.error("/reservediff - Error in getReserveDiffs:", error);
      res.status(500).send("Internal server error");
    }
  });

  app.get("/rollback", async (req: Request, res: Response) => {
    if (!ensureLocalAccess(req, res)) {
      return;
    }

    if (!(await acquireExclusiveLock(res))) {
      return;
    }

    console.log(`zephyrdscanner /rollback called`);
    console.log(req.query);

    const height = Number(req.query.height);
    if (!height) {
      res.status(400).send("height query param is required");
      await releaseExclusiveLock();
      return;
    }

    try {
      const result = await rollbackScanner(height);
      res.status(200).json(result);
    } catch (error) {
      console.error("/rollback - Error in rollbackScanner:", error);
      res.status(500).send("Internal server error");
    } finally {
      await releaseExclusiveLock();
    }
  });

  app.get("/retallytotals", async (req: Request, res: Response) => {
    if (!ensureLocalAccess(req, res)) {
      return;
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

  app.post("/reset", async (req: Request, res: Response) => {
    if (!ensureLocalAccess(req, res)) {
      return;
    }

    if (!(await acquireExclusiveLock(res))) {
      return;
    }

    const scope = req.query.scope === "full" ? "full" : "aggregation";
    console.log(`zephyrdscanner /reset called with scope=${scope}`);

    try {
      await resetScanner(scope);
      res.status(200).json({ status: "ok", scope });
    } catch (error) {
      console.error("/reset - Error in resetScanner:", error);
      res.status(500).send("Internal server error");
    } finally {
      await releaseExclusiveLock();
    }
  });

  app.get("/redetermineapyhistory", async (req: Request, res: Response) => {
    if (!ensureLocalAccess(req, res)) {
      return;
    }

    if (!(await acquireExclusiveLock(res))) {
      return;
    }

    console.log(`zephyrdscanner /redetermineapyhistory called`);

    try {
      await determineAPYHistory(true);
      res.status(200).send("determineAPYHistory redetermined successfully");
    } catch (error) {
      console.error("/redetermineapyhistory - Error in determineAPYHistory:", error);
      res.status(500).send("Internal server error");
    } finally {
      await releaseExclusiveLock();
    }
  });

  return app;
}

export function startServer(): { app: ReturnType<typeof createApp>; server: Server | null } {
  const app = createApp();

  if (WALKTHROUGH_MODE) {
    console.log("Walkthrough mode enabled – HTTP server not started.");
    return { app, server: null };
  }

  const server = app.listen(PORT, () => {
    console.log(`zephyrdscanner listening at http://localhost:${PORT}`);
  });

  return { app, server };
}
