import fs from "node:fs";
import dotenv from "dotenv";
import redis from "./redis";
import { truncatePostgresData } from "./db/admin";
import { usePostgres } from "./config";

dotenv.config();

async function bootstrap() {
  const consoleLogPath = process.env.WALKTHROUGH_CONSOLE_LOG ?? "walkthrough_console.log";
  const consoleStream = fs.createWriteStream(consoleLogPath, { flags: "w" });
  const original = {
    log: console.log.bind(console),
    warn: console.warn.bind(console),
    error: console.error.bind(console),
    info: console.info ? console.info.bind(console) : console.log.bind(console),
  };

  const mirror = (level: keyof typeof original) =>
    (...args: unknown[]) => {
      original[level](...args);
      try {
        const line = args
          .map((arg) => {
            if (typeof arg === "string") return arg;
            try {
              return JSON.stringify(arg);
            } catch (e) {
              return String(arg);
            }
          })
          .join(" ");
        consoleStream.write(`[${level}] ${line}\n`);
      } catch (err) {
        original.error("[walkthrough] Failed to mirror console output", err);
      }
    };

  console.log = mirror("log");
  console.warn = mirror("warn");
  console.error = mirror("error");
  if (console.info) {
    console.info = mirror("info");
  }

  process.on("exit", () => {
    consoleStream.end();
  });

  console.log("[walkthrough] Flushing Redis database...");
  await redis.flushdb();
  console.log("[walkthrough] Redis flushed.");
  if (usePostgres()) {
    console.log("[walkthrough] Truncating Postgres tables...");
    await truncatePostgresData();
  }

  process.env.WALKTHROUGH_MODE = "true";
  if (!process.env.WALKTHROUGH_DIFF_THRESHOLD) {
    process.env.WALKTHROUGH_DIFF_THRESHOLD = "1";
  }
  if (process.env.ONLINE !== "true") {
    console.log("[walkthrough] ONLINE not set. Forcing ONLINE=true.");
    process.env.ONLINE = "true";
  }

  console.log("[walkthrough] Starting scanner in walkthrough mode...");
  const { startScanner } = await import("./scanner-runner");
  await startScanner();
}

bootstrap().catch((error) => {
  console.error("[walkthrough] Failed to start walkthrough mode", error);
  process.exit(1);
});
