import dotenv from "dotenv";
import fs from "node:fs/promises";
import path from "node:path";

import redis from "../redis";
import { getRedisHeight } from "../utils";

dotenv.config();

interface CliOptions {
  outputDir: string;
  pretty: boolean;
  tag?: string;
}

function printHelp() {
  console.log(`Usage: npx tsx src/scripts/exportRedisData.ts [options]

Options:
  --dir, -d <path>     Directory root for export (default: exports/<version>)
  --pretty, -p         Pretty-print JSON output
  --tag <label>        Optional label appended to the export directory name
  --help, -h           Show this help message
`);
}

function parseArgs(): CliOptions {
  const args = process.argv.slice(2);
  let outputDir = "";
  let pretty = false;
  let tag: string | undefined;

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
      case "--tag": {
        const next = args[++i];
        if (!next) {
          throw new Error(`${arg} requires a label`);
        }
        tag = next;
        break;
      }
      case "--help":
      case "-h":
        printHelp();
        process.exit(0);
      default:
        console.warn(`Unrecognised argument: ${arg}`);
    }
  }

  return { outputDir, pretty, tag };
}

function sanitizeKey(key: string): string {
  return key.replace(/[^a-zA-Z0-9._-]/g, "_");
}

async function scanKeys(): Promise<string[]> {
  const keys: string[] = [];
  let cursor = "0";
  do {
    const [nextCursor, batch] = await redis.scan(cursor, "COUNT", 1000);
    cursor = nextCursor;
    if (batch.length > 0) {
      keys.push(...batch);
      console.log(`[export] Scanned ${keys.length} keys so far`);
    }
  } while (cursor !== "0");
  return keys;
}

async function exportString(dir: string, key: string, pretty: boolean) {
  const value = await redis.get(key);
  await fs.writeFile(
    path.join(dir, "value.json"),
    JSON.stringify({ value }, null, pretty ? 2 : undefined)
  );
}

async function exportHash(dir: string, key: string, pretty: boolean) {
  let cursor = "0";
  let part = 0;
  do {
    const [nextCursor, entries] = await redis.hscan(key, cursor, "COUNT", 5000);
    if (entries.length > 0) {
      const chunk: Record<string, string> = {};
      for (let i = 0; i < entries.length; i += 2) {
        chunk[entries[i]] = entries[i + 1];
      }
      const filePath = path.join(dir, `hash_part_${String(part).padStart(4, "0")}.json`);
      await fs.writeFile(filePath, JSON.stringify(chunk, null, pretty ? 2 : undefined));
      part += 1;
    }
    cursor = nextCursor;
  } while (cursor !== "0");
}

async function exportList(dir: string, key: string, pretty: boolean) {
  const length = await redis.llen(key);
  const chunkSize = 5000;
  let part = 0;
  for (let start = 0; start < length; start += chunkSize) {
    const end = Math.min(start + chunkSize - 1, length - 1);
    const items = await redis.lrange(key, start, end);
    if (items.length > 0) {
      const filePath = path.join(dir, `list_part_${String(part).padStart(4, "0")}.json`);
      await fs.writeFile(filePath, JSON.stringify(items, null, pretty ? 2 : undefined));
      part += 1;
    }
  }
}

async function exportSet(dir: string, key: string, pretty: boolean) {
  let cursor = "0";
  let part = 0;
  do {
    const [nextCursor, members] = await redis.sscan(key, cursor, "COUNT", 5000);
    if (members.length > 0) {
      const filePath = path.join(dir, `set_part_${String(part).padStart(4, "0")}.json`);
      await fs.writeFile(filePath, JSON.stringify(members, null, pretty ? 2 : undefined));
      part += 1;
    }
    cursor = nextCursor;
  } while (cursor !== "0");
}

async function exportZSet(dir: string, key: string, pretty: boolean) {
  let cursor = "0";
  let part = 0;
  do {
    const [nextCursor, entries] = await redis.zscan(key, cursor, "COUNT", 5000);
    if (entries.length > 0) {
      const chunk: { member: string; score: number }[] = [];
      for (let i = 0; i < entries.length; i += 2) {
        chunk.push({ member: entries[i], score: Number(entries[i + 1]) });
      }
      const filePath = path.join(dir, `zset_part_${String(part).padStart(4, "0")}.json`);
      await fs.writeFile(filePath, JSON.stringify(chunk, null, pretty ? 2 : undefined));
      part += 1;
    }
    cursor = nextCursor;
  } while (cursor !== "0");
}

async function exportKey(baseDir: string, key: string, pretty: boolean) {
  const type = await redis.type(key);
  if (!type || type === "none") {
    return;
  }

  const ttl = await redis.ttl(key);
  const safeKey = sanitizeKey(key);
  const keyDir = path.join(baseDir, safeKey);
  await fs.mkdir(keyDir, { recursive: true });

  await fs.writeFile(
    path.join(keyDir, "meta.json"),
    JSON.stringify({ key, type, ttl }, null, pretty ? 2 : undefined)
  );

  switch (type) {
    case "string":
      await exportString(keyDir, key, pretty);
      break;
    case "hash":
      await exportHash(keyDir, key, pretty);
      break;
    case "list":
      await exportList(keyDir, key, pretty);
      break;
    case "set":
      await exportSet(keyDir, key, pretty);
      break;
    case "zset":
      await exportZSet(keyDir, key, pretty);
      break;
    default:
      console.warn(`[export] Unsupported type '${type}' for key ${key}`);
  }
}

async function exportRedis(options: CliOptions) {
  const keys = await scanKeys();
  console.log(`[export] Found ${keys.length} keys – starting export`);

  const pkg = JSON.parse(await fs.readFile(path.resolve(process.cwd(), "package.json"), "utf8"));
  const version = pkg?.version ?? "unknown";
  const height = await getRedisHeight();
  const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
  const tagPart = options.tag ? `_${sanitizeKey(options.tag)}` : "";
  const baseName = `redis_export_${version}_h${height}${tagPart}_${timestamp}`;
  const exportRoot = options.outputDir
    ? path.resolve(process.cwd(), options.outputDir, baseName)
    : path.resolve(process.cwd(), "exports", version, baseName);

  await fs.mkdir(exportRoot, { recursive: true });

  let exported = 0;

  for (const key of keys) {
    try {
      await exportKey(exportRoot, key, options.pretty);
      exported += 1;
      if (exported % 100 === 0) {
        console.log(`[export] Processed ${exported} / ${keys.length} keys`);
      }
    } catch (error) {
      console.error(`[export] Failed to export key ${key}:`, error);
    }
  }

  await fs.writeFile(
    path.join(exportRoot, "summary.json"),
    JSON.stringify(
      {
        version,
        height,
        timestamp,
        keys: exported,
      },
      null,
      options.pretty ? 2 : undefined
    )
  );

  console.log(
    `[export] Completed export of ${exported} keys to ${exportRoot} (version=${version}, height=${height})`
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
