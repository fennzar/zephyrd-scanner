import dotenv from "dotenv";
import fs from "node:fs/promises";
import path from "node:path";

import redis from "../redis";

dotenv.config();

const RESERVE_SNAPSHOT_HASH_KEY = process.env.RESERVE_SNAPSHOT_REDIS_KEY ?? "reserve_snapshots";
const DEFAULT_IMPORT_DIR = process.env.RESERVE_SNAPSHOT_DIR ?? "reserve_snapshots";

interface CliOptions {
  inputDir: string;
  overwrite: boolean;
}

function printHelp() {
  console.log(`Usage: npx tsx src/scripts/importReserveSnapshots.ts [options]

Options:
  --dir, --in, -d <path>    Directory containing snapshot JSON files (default: ${DEFAULT_IMPORT_DIR})
  --force, -f               Overwrite existing entries in Redis
  --help, -h                Show this message

Environment variables:
  RESERVE_SNAPSHOT_REDIS_KEY  Redis hash to write snapshots into (default: reserve_snapshots)
  RESERVE_SNAPSHOT_DIR        Default directory to read snapshots from
`);
}

function parseArgs(): CliOptions {
  const args = process.argv.slice(2);
  let inputDir = DEFAULT_IMPORT_DIR;
  let overwrite = false;

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    switch (arg) {
      case "--dir":
      case "--in":
      case "-d": {
        const next = args[++i];
        if (!next) {
          throw new Error(`${arg} requires a directory path`);
        }
        inputDir = next;
        break;
      }
      case "--force":
      case "-f":
        overwrite = true;
        break;
      case "--help":
      case "-h":
        printHelp();
        process.exit(0);
      default:
        console.warn(`Unrecognised argument: ${arg}`);
        break;
    }
  }

  return { inputDir, overwrite };
}

async function readSnapshotFiles(dir: string): Promise<string[]> {
  try {
    const entries = await fs.readdir(dir);
    return entries.filter((file) => file.endsWith(".json"));
  } catch (error) {
    console.error(`Failed to read directory '${dir}':`, error);
    return [];
  }
}

async function importSnapshots(options: CliOptions) {
  const resolvedDir = path.resolve(process.cwd(), options.inputDir);
  const files = await readSnapshotFiles(resolvedDir);

  if (files.length === 0) {
    console.warn(`No JSON snapshots found in ${resolvedDir}`);
    return;
  }

  let imported = 0;

  for (const file of files) {
    const filePath = path.join(resolvedDir, file);
    let snapshot: any;

    try {
      const content = await fs.readFile(filePath, "utf8");
      snapshot = JSON.parse(content);
    } catch (error) {
      console.error(`Skipping ${filePath} – failed to read/parse JSON:`, error);
      continue;
    }

    const previousHeight = snapshot?.previous_height;
    if (!Number.isFinite(previousHeight)) {
      console.error(`Skipping ${filePath} – missing previous_height`);
      continue;
    }

    const key = previousHeight.toString();

    if (!options.overwrite) {
      const exists = await redis.hexists(RESERVE_SNAPSHOT_HASH_KEY, key);
      if (exists) {
        console.warn(
          `Snapshot for previous height ${key} already exists in Redis – skipping (use --force to overwrite)`
        );
        continue;
      }
    }

    await redis.hset(RESERVE_SNAPSHOT_HASH_KEY, key, JSON.stringify(snapshot));
    await redis.set("reserve_snapshots:last_previous_height", key);
    imported += 1;
  }

  console.log(`Imported ${imported} snapshot${imported === 1 ? "" : "s"} from ${resolvedDir}`);
}

async function main() {
  try {
    const options = parseArgs();
    await importSnapshots(options);
  } catch (error) {
    console.error("Failed to import reserve snapshots:", error);
    process.exit(1);
  } finally {
    await redis.quit();
  }
}

void main();
