import dotenv from "dotenv";
import fs from "node:fs/promises";
import path from "node:path";

import redis from "../redis";
import { getRedisHeight } from "../utils";

dotenv.config();

interface RedisRecord {
  type: string;
  ttl: number;
  value: unknown;
}

interface CliOptions {
  outputDir: string;
  pretty: boolean;
}

function printHelp() {
  console.log(`Usage: npx tsx scripts/exportRedisData.ts [options]

Options:
  --dir, -d <path>     Directory to write export (default: exports/<version>)
  --pretty, -p         Pretty-print JSON output
  --help, -h           Show this help message
`);
}

function parseArgs(): CliOptions {
  const args = process.argv.slice(2);
  let outputDir = "";
  let pretty = false;

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    switch (arg) {
      case "--dir":
      case "-d": {
        const next = args[++i];
        if (!next) {
          throw new Error(`${arg} requires a directory path`);
        }
        outputDir = next;
        break;
      }
      case "--pretty":
      case "-p":
        pretty = true;
        break;
      case "--help":
      case "-h":
        printHelp();
        process.exit(0);
      default:
        console.warn(`Unrecognised argument: ${arg}`);
    }
  }

  return { outputDir, pretty };
}

async function scanKeys(): Promise<string[]> {
  const keys: string[] = [];
  let cursor = "0";
  do {
    const [nextCursor, batch] = await redis.scan(cursor, "COUNT", 1000);
    cursor = nextCursor;
    if (batch.length > 0) {
      keys.push(...batch);
    }
  } while (cursor !== "0");
  return keys;
}

async function fetchRecord(key: string): Promise<RedisRecord | null> {
  const type = await redis.type(key);
  if (!type || type === "none") {
    return null;
  }

  const ttl = await redis.ttl(key);

  switch (type) {
    case "string":
      return { type, ttl, value: await redis.get(key) };
    case "hash":
      return { type, ttl, value: await redis.hgetall(key) };
    case "list":
      return { type, ttl, value: await redis.lrange(key, 0, -1) };
    case "set":
      return { type, ttl, value: await redis.smembers(key) };
    case "zset": {
      const entries = await redis.zrange(key, 0, -1, "WITHSCORES");
      const pairs: { member: string; score: number }[] = [];
      for (let i = 0; i < entries.length; i += 2) {
        pairs.push({ member: entries[i], score: Number(entries[i + 1]) });
      }
      return { type, ttl, value: pairs };
    }
    default:
      console.warn(`Skipping unsupported redis type '${type}' for key ${key}`);
      return null;
  }
}

async function exportRedis(options: CliOptions) {
  const keys = await scanKeys();
  const snapshot: Record<string, RedisRecord> = {};

  for (const key of keys) {
    try {
      const record = await fetchRecord(key);
      if (record) {
        snapshot[key] = record;
      }
    } catch (error) {
      console.error(`Failed to export key ${key}:`, error);
    }
  }

  const json = options.pretty
    ? JSON.stringify(snapshot, null, 2)
    : JSON.stringify(snapshot);

  const pkg = JSON.parse(await fs.readFile(path.resolve(process.cwd(), "package.json"), "utf8"));
  const version = pkg?.version ?? "unknown";
  const height = await getRedisHeight();
  const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
  const targetDir = options.outputDir
    ? path.resolve(process.cwd(), options.outputDir)
    : path.resolve(process.cwd(), "exports", version);

  await fs.mkdir(targetDir, { recursive: true });
  const fileName = `redis_export_${version}_${height}_${timestamp}.json`;
  const outputPath = path.join(targetDir, fileName);

  await fs.writeFile(outputPath, json);
  console.log(
    `Exported ${Object.keys(snapshot).length} keys to ${outputPath} (version=${version}, height=${height})`
  );
}

async function main() {
  try {
    const options = parseArgs();
    await exportRedis(options);
  } catch (error) {
    console.error("Failed to export redis data:", error);
    process.exit(1);
  } finally {
    await redis.quit();
  }
}

void main();
