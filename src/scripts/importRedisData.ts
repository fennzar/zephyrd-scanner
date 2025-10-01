import dotenv from "dotenv";
import fs from "node:fs/promises";
import path from "node:path";

import redis from "../redis";

dotenv.config();

interface CliOptions {
  inputDir: string;
  flush: boolean;
  skipExisting: boolean;
}

interface MetaRecord {
  key: string;
  type: string;
  ttl: number;
}

function printHelp() {
  console.log(`Usage: npx tsx src/scripts/importRedisData.ts [options]

Options:
  --dir, -d <path>     Directory produced by exportRedisData.ts
  --flush              Flush Redis before import
  --skip-existing      Do not overwrite keys that already exist
  --help, -h           Show this message
`);
}

function parseArgs(): CliOptions {
  const args = process.argv.slice(2);
  let inputDir = "";
  let flush = false;
  let skipExisting = false;

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    switch (arg) {
      case "--dir":
      case "-d": {
        const next = args[++i];
        if (!next) {
          throw new Error(`${arg} requires a directory path`);
        }
        inputDir = next;
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

  if (!inputDir) {
    throw new Error("--dir is required");
  }

  return {
    inputDir: path.resolve(process.cwd(), inputDir),
    flush,
    skipExisting,
  };
}

async function loadMeta(dir: string): Promise<MetaRecord | null> {
  try {
    const metaPath = path.join(dir, "meta.json");
    const meta = JSON.parse(await fs.readFile(metaPath, "utf8"));
    if (!meta || typeof meta.key !== "string" || typeof meta.type !== "string") {
      return null;
    }
    return meta as MetaRecord;
  } catch (error) {
    console.error(`[import] Failed to read meta for ${dir}:`, error);
    return null;
  }
}

async function restoreString(key: string, dir: string) {
  const valuePath = path.join(dir, "value.json");
  const payload = JSON.parse(await fs.readFile(valuePath, "utf8"));
  await redis.set(key, payload?.value ?? "");
}

async function restoreHash(key: string, dir: string) {
  await redis.del(key);
  const files = await fs.readdir(dir);
  const parts = files.filter((name) => name.startsWith("hash_part_")).sort();
  for (const part of parts) {
    const chunk = JSON.parse(await fs.readFile(path.join(dir, part), "utf8"));
    const entries = Object.entries(chunk);
    if (entries.length > 0) {
      await redis.hset(key, Object.fromEntries(entries));
    }
  }
}

async function restoreList(key: string, dir: string) {
  await redis.del(key);
  const files = await fs.readdir(dir);
  const parts = files.filter((name) => name.startsWith("list_part_")).sort();
  for (const part of parts) {
    const chunk = JSON.parse(await fs.readFile(path.join(dir, part), "utf8"));
    if (Array.isArray(chunk) && chunk.length > 0) {
      await redis.rpush(key, ...chunk);
    }
  }
}

async function restoreSet(key: string, dir: string) {
  await redis.del(key);
  const files = await fs.readdir(dir);
  const parts = files.filter((name) => name.startsWith("set_part_")).sort();
  for (const part of parts) {
    const chunk = JSON.parse(await fs.readFile(path.join(dir, part), "utf8"));
    if (Array.isArray(chunk) && chunk.length > 0) {
      await redis.sadd(key, ...chunk);
    }
  }
}

async function restoreZSet(key: string, dir: string) {
  await redis.del(key);
  const files = await fs.readdir(dir);
  const parts = files.filter((name) => name.startsWith("zset_part_")).sort();
  for (const part of parts) {
    const chunk = JSON.parse(await fs.readFile(path.join(dir, part), "utf8")) as {
      member: string;
      score: number;
    }[];
    if (Array.isArray(chunk) && chunk.length > 0) {
      for (const { member, score } of chunk) {
        await redis.zadd(key, score, member);
      }
    }
  }
}

async function restoreKey(meta: MetaRecord, dir: string, skipExisting: boolean) {
  const { key, type, ttl } = meta;

  if (skipExisting) {
    const exists = await redis.exists(key);
    if (exists) {
      console.warn(`[import] Skipping ${key} – exists`);
      return;
    }
  }

  switch (type) {
    case "string":
      await restoreString(key, dir);
      break;
    case "hash":
      await restoreHash(key, dir);
      break;
    case "list":
      await restoreList(key, dir);
      break;
    case "set":
      await restoreSet(key, dir);
      break;
    case "zset":
      await restoreZSet(key, dir);
      break;
    default:
      console.warn(`[import] Unsupported type '${type}' for key ${key}`);
      return;
  }

  if (typeof ttl === "number" && ttl > 0) {
    await redis.expire(key, ttl);
  } else if (ttl === -1) {
    await redis.persist(key);
  }
}

async function importRedis(options: CliOptions) {
  const { inputDir, flush, skipExisting } = options;
  const entries = await fs.readdir(inputDir, { withFileTypes: true });
  const keyDirs = entries.filter((entry) => entry.isDirectory());

  if (flush) {
    console.warn("[import] Flushing Redis before import...");
    await redis.flushdb();
  }

  let imported = 0;

  for (const entry of keyDirs) {
    const dirPath = path.join(inputDir, entry.name);
    const meta = await loadMeta(dirPath);
    if (!meta) {
      continue;
    }

    try {
      await restoreKey(meta, dirPath, skipExisting);
      imported += 1;
      if (imported % 100 === 0) {
        console.log(`[import] Restored ${imported} / ${keyDirs.length} keys`);
      }
    } catch (error) {
      console.error(`[import] Failed to restore ${meta.key}:`, error);
    }
  }

  console.log(`[import] Imported ${imported} keys from ${inputDir}`);
}

async function main() {
  try {
    const options = parseArgs();
    await importRedis(options);
  } catch (error) {
    console.error("Failed to import redis data:", error);
    process.exit(1);
  } finally {
    await redis.quit();
  }
}

void main();
