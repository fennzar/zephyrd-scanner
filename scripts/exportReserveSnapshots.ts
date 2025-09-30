import dotenv from "dotenv";
import fs from "node:fs/promises";
import path from "node:path";

import redis from "../redis";

dotenv.config();

const RESERVE_SNAPSHOT_HASH_KEY = process.env.RESERVE_SNAPSHOT_REDIS_KEY ?? "reserve_snapshots";
const DEFAULT_EXPORT_DIR = process.env.RESERVE_SNAPSHOT_DIR ?? "reserve_snapshots";

interface CliOptions {
  outputDir: string;
  overwrite: boolean;
  indexBy: "reserve" | "previous";
}

function printHelp() {
  console.log(`Usage: npx tsx scripts/exportReserveSnapshots.ts [options]

Options:
  --dir, --out, -d <path>   Directory to write exported snapshots (default: ${DEFAULT_EXPORT_DIR})
  --force, -f               Overwrite existing files
  --index <reserve|previous>  Use reserve height (default) or previous height for filenames
  --help, -h                Show this message

Environment variables:
  RESERVE_SNAPSHOT_REDIS_KEY  Redis hash to read snapshots from (default: reserve_snapshots)
  RESERVE_SNAPSHOT_DIR        Default export directory override
`);
}

function parseArgs(): CliOptions {
  const args = process.argv.slice(2);
  let outputDir = DEFAULT_EXPORT_DIR;
  let overwrite = false;
  let indexBy: "reserve" | "previous" = "reserve";

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    switch (arg) {
      case "--dir":
      case "--out":
      case "-d": {
        const next = args[++i];
        if (!next) {
          throw new Error(`${arg} requires a directory`);
        }
        outputDir = next;
        break;
      }
      case "--force":
      case "-f":
        overwrite = true;
        break;
      case "--index":
      case "-i": {
        const next = args[++i];
        if (!next || (next !== "reserve" && next !== "previous")) {
          throw new Error(`${arg} requires either 'reserve' or 'previous'`);
        }
        indexBy = next;
        break;
      }
      case "--help":
      case "-h":
        printHelp();
        process.exit(0);
      default:
        console.warn(`Unrecognised argument: ${arg}`);
        break;
    }
  }

  return { outputDir, overwrite, indexBy };
}

async function ensureDir(dir: string) {
  await fs.mkdir(dir, { recursive: true });
}

async function exportSnapshots(options: CliOptions) {
  const snapshots = await redis.hgetall(RESERVE_SNAPSHOT_HASH_KEY);
  const keys = Object.keys(snapshots);

  if (keys.length === 0) {
    console.warn(`No entries found in redis hash '${RESERVE_SNAPSHOT_HASH_KEY}'.`);
    return;
  }

  const resolvedDir = path.resolve(process.cwd(), options.outputDir);
  await ensureDir(resolvedDir);

  let written = 0;

  for (const hashKey of keys) {
    const payload = snapshots[hashKey];
    if (!payload) {
      continue;
    }

    let snapshot: unknown;
    try {
      snapshot = JSON.parse(payload);
    } catch (error) {
      console.error(`Skipping malformed snapshot at hash key ${hashKey}:`, error);
      continue;
    }

    if (typeof snapshot !== "object" || snapshot === null) {
      console.error(`Skipping snapshot at hash key ${hashKey}: not an object`);
      continue;
    }

    const record = snapshot as { reserve_height?: number; previous_height?: number };
    const targetHeight = options.indexBy === "previous" ? record.previous_height : record.reserve_height;

    if (!Number.isFinite(targetHeight)) {
      console.error(`Skipping snapshot at hash key ${hashKey}: missing reserve/previous height`);
      continue;
    }

    const filePath = path.join(resolvedDir, `${targetHeight}.json`);

    if (!options.overwrite) {
      try {
        await fs.access(filePath);
        console.warn(`File ${filePath} exists – skipping (use --force to overwrite).`);
        continue;
      } catch {
        // file does not exist, proceed
      }
    }

    await fs.writeFile(filePath, JSON.stringify(snapshot, null, 2));
    written += 1;
  }

  console.log(`Exported ${written} snapshot${written === 1 ? "" : "s"} to ${resolvedDir}`);
}

async function main() {
  try {
    const options = parseArgs();
    await exportSnapshots(options);
  } catch (error) {
    console.error("Failed to export reserve snapshots:", error);
    process.exit(1);
  } finally {
    await redis.quit();
  }
}

void main();
