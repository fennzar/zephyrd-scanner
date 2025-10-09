import dotenv from "dotenv";

import { createRedisClient } from "../redis";

dotenv.config();

type Mode = "soft" | "hard";

interface CliOptions {
  mode: Mode;
  sourceDb: number;
  targetDb: number;
}

const AGGREGATION_KEYS = [
  "protocol_stats",
  "protocol_stats_hourly",
  "protocol_stats_daily",
  "height_aggregator",
  "timestamp_aggregator_hourly",
  "timestamp_aggregator_daily",
  "historical_returns",
  "projected_returns",
  "apy_history",
];

const DEFAULT_SOURCE_DB = parseDb(process.env.REDIS_SOURCE_DB, 0);
const DEFAULT_TARGET_DB = parseDb(process.env.REDIS_DB, 1);

function parseDb(value: string | undefined, fallback: number): number {
  if (!value) {
    return fallback;
  }
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < 0) {
    return fallback;
  }
  return parsed;
}

function printHelp(): void {
  console.log(`prepareBackgroundDb.ts

Usage:
  npx tsx src/scripts/prepareBackgroundDb.ts [--mode=soft|hard] [--source-db=DB] [--target-db=DB]

Options:
  --mode        soft (default) copies source DB into target and drops aggregation keys.
                hard flushes the target DB so the scanner starts from scratch.
  --source-db   Source Redis DB index. Defaults to ${DEFAULT_SOURCE_DB}.
  --target-db   Target Redis DB index. Defaults to ${DEFAULT_TARGET_DB}.
`);
}

function parseCliArgs(): CliOptions {
  const args = process.argv.slice(2);
  let mode: Mode = "soft";
  let sourceDb = DEFAULT_SOURCE_DB;
  let targetDb = DEFAULT_TARGET_DB;

  for (const arg of args) {
    if (arg === "--help" || arg === "-h") {
      printHelp();
      process.exit(0);
    }

    const [flag, value] = arg.split("=");
    if (flag === "--mode") {
      if (value !== "soft" && value !== "hard") {
        throw new Error(`Invalid mode "${value}". Expected "soft" or "hard".`);
      }
      mode = value;
    } else if (flag === "--source-db") {
      const parsed = parseDb(value, sourceDb);
      sourceDb = parsed;
    } else if (flag === "--target-db") {
      const parsed = parseDb(value, targetDb);
      targetDb = parsed;
    } else {
      throw new Error(`Unknown argument "${arg}". Use --help for usage.`);
    }
  }

  if (sourceDb === targetDb) {
    throw new Error("Source and target DB must differ to avoid clobbering live data.");
  }

  return { mode, sourceDb, targetDb };
}

async function copyDb(sourceDb: number, targetDb: number): Promise<number> {
  const source = createRedisClient(sourceDb);
  const target = createRedisClient(targetDb);
  const skipKeys = new Set(AGGREGATION_KEYS);
  let copied = 0;
  let cursor = "0";

  try {
    do {
      const [nextCursor, keys] = await source.scan(cursor, "COUNT", "500");
      cursor = nextCursor;

      for (const key of keys) {
        if (skipKeys.has(key)) {
          continue;
        }

        try {
          const ttl = await source.pttl(key);
          if (ttl === -2) {
            continue;
          }

          const dump =
            typeof (source as any).dumpBuffer === "function"
              ? await (source as any).dumpBuffer(key)
              : await source.dump(key);

          if (!dump) {
            continue;
          }

          const expire = ttl > 0 ? ttl : 0;

          await target.restore(key, expire, dump, "REPLACE");
          copied += 1;
        } catch (error) {
          console.error(`[prepare-bg] Failed to copy key "${key}":`, error);
        }
      }
    } while (cursor !== "0");
  } finally {
    await source.quit();
    await target.quit();
  }

  return copied;
}

async function clearAggregationKeys(targetDb: number): Promise<void> {
  const target = createRedisClient(targetDb);
  try {
    if (AGGREGATION_KEYS.length === 0) {
      return;
    }
    await target.del(...AGGREGATION_KEYS);
  } finally {
    await target.quit();
  }
}

async function flushDb(targetDb: number): Promise<void> {
  const target = createRedisClient(targetDb);
  try {
    await target.flushdb();
  } finally {
    await target.quit();
  }
}

(async () => {
  const { mode, sourceDb, targetDb } = parseCliArgs();

  console.log(`[prepare-bg] mode=${mode} sourceDb=${sourceDb} targetDb=${targetDb}`);

  if (mode === "hard") {
    console.log("[prepare-bg] Flushing target database...");
    await flushDb(targetDb);
    console.log("[prepare-bg] Target database flushed. Ready for fresh scan.");
    return;
  }

  console.log("[prepare-bg] Resetting target database before copy...");
  await flushDb(targetDb);

  console.log("[prepare-bg] Copying keys from source to target (excluding aggregation keys)...");
  const copied = await copyDb(sourceDb, targetDb);
  console.log(`[prepare-bg] Copied ${copied} keys.`);

  console.log("[prepare-bg] Removing aggregation artefacts from target...");
  await clearAggregationKeys(targetDb);

  console.log("[prepare-bg] Soft preparation complete. Scanner can rebuild aggregation on the target DB.");
})().catch((error) => {
  console.error("[prepare-bg] Failed:", error);
  process.exit(1);
});
