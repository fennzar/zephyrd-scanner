import dotenv from "dotenv";
import fs from "node:fs/promises";
import path from "node:path";

import redis from "../redis";

dotenv.config();

interface RedisRecord {
  type: string;
  ttl: number;
  value: unknown;
}

interface CliOptions {
  filePath: string;
  flush: boolean;
  skipExisting: boolean;
}

function printHelp() {
  console.log(`Usage: npx tsx scripts/importRedisData.ts [options]

Options:
  --file, -f <path>    Import from JSON file (default: redis_export.json)
  --flush              Flush Redis before import
  --skip-existing      Do not overwrite keys that already exist
  --help, -h           Show this message
`);
}

function parseArgs(): CliOptions {
  const args = process.argv.slice(2);
  let filePath = "redis_export.json";
  let flush = false;
  let skipExisting = false;

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    switch (arg) {
      case "--file":
      case "-f": {
        const next = args[++i];
        if (!next) {
          throw new Error(`${arg} requires a file path`);
        }
        filePath = next;
        break;
      }
      case "--flush":
        flush = true;
        break;
      case "--skip-existing":
        skipExisting = true;
        break;
      case "--help":
      case "-h":
        printHelp();
        process.exit(0);
      default:
        console.warn(`Unrecognised argument: ${arg}`);
    }
  }

  return { filePath, flush, skipExisting };
}

async function loadDump(filePath: string): Promise<Record<string, RedisRecord>> {
  const resolved = path.resolve(process.cwd(), filePath);
  const data = await fs.readFile(resolved, "utf8");
  const parsed = JSON.parse(data);
  if (!parsed || typeof parsed !== "object") {
    throw new Error("Invalid dump file: expected JSON object at top level");
  }
  return parsed as Record<string, RedisRecord>;
}

async function restoreKey(key: string, record: RedisRecord, skipExisting: boolean) {
  if (skipExisting) {
    const exists = await redis.exists(key);
    if (exists) {
      console.warn(`Skipping key ${key} – already exists`);
      return;
    }
  }

  const { type, ttl, value } = record;

  switch (type) {
    case "string":
      await redis.set(key, value as string);
      break;
    case "hash":
      await redis.del(key);
      if (value && typeof value === "object") {
        await redis.hset(key, value as Record<string, string>);
      }
      break;
    case "list":
      await redis.del(key);
      if (Array.isArray(value) && value.length > 0) {
        await redis.rpush(key, ...(value as string[]));
      }
      break;
    case "set":
      await redis.del(key);
      if (Array.isArray(value) && value.length > 0) {
        await redis.sadd(key, ...(value as string[]));
      }
      break;
    case "zset":
      await redis.del(key);
      if (Array.isArray(value) && value.length > 0) {
        const entries = value as { member: string; score: number }[];
        const args: (string | number)[] = [];
        entries.forEach(({ member, score }) => {
          args.push(score);
          args.push(member);
        });
        if (args.length > 0) {
          await redis.zadd(key, ...(args as (string | number)[]));
        }
      }
      break;
    default:
      console.warn(`Unsupported type '${type}' for key ${key}; skipping`);
      return;
  }

  if (typeof ttl === "number" && ttl > 0) {
    await redis.expire(key, ttl);
  } else if (ttl === -1) {
    await redis.persist(key);
  }
}

async function importDump(options: CliOptions) {
  const dump = await loadDump(options.filePath);
  const keys = Object.keys(dump);

  if (options.flush) {
    console.warn("Flushing Redis database before import...");
    await redis.flushdb();
  }

  let imported = 0;
  for (const key of keys) {
    try {
      await restoreKey(key, dump[key], options.skipExisting);
      imported += 1;
    } catch (error) {
      console.error(`Failed to import key ${key}:`, error);
    }
  }

  console.log(`Imported ${imported} key${imported === 1 ? "" : "s"} from ${options.filePath}`);
}

async function main() {
  try {
    const options = parseArgs();
    await importDump(options);
  } catch (error) {
    console.error("Failed to import redis data:", error);
    process.exit(1);
  } finally {
    await redis.quit();
  }
}

void main();
