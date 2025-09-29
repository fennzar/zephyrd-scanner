import dotenv from "dotenv";
import redis from "./redis";

dotenv.config();

async function bootstrap() {
  console.log("[walkthrough] Flushing Redis database...");
  await redis.flushdb();
  console.log("[walkthrough] Redis flushed.");

  process.env.WALKTHROUGH_MODE = "true";
  if (!process.env.WALKTHROUGH_DIFF_THRESHOLD) {
    process.env.WALKTHROUGH_DIFF_THRESHOLD = "1";
  }
  if (process.env.ONLINE !== "true") {
    console.log("[walkthrough] ONLINE not set. Forcing ONLINE=true.");
    process.env.ONLINE = "true";
  }

  console.log("[walkthrough] Starting scanner in walkthrough mode...");
  await import("./scanner");
}

bootstrap().catch((error) => {
  console.error("[walkthrough] Failed to start walkthrough mode", error);
  process.exit(1);
});
