import fs from "node:fs/promises";
import path from "node:path";
import fetch from "node-fetch";

const RPC_URL = process.env.ZEPHYR_RPC_URL ?? "http://127.0.0.1:17767";
const DEFAULT_OUTPUT_DIR = "reserve_snapshots";
const HEADERS = {
  "Content-Type": "application/json",
};
const DEATOMIZE = 10 ** -12;

interface ReserveInfoResult {
  assets: string;
  assets_ma: string;
  equity: string;
  equity_ma: string;
  height: number;
  hf_version: number;
  liabilities: string;
  num_reserves: string;
  num_stables: string;
  num_zyield: string;
  pr: {
    moving_average: number;
    reserve: number;
    reserve_ma: number;
    reserve_ratio: number;
    reserve_ratio_ma: number;
    signature: string;
    spot: number;
    stable: number;
    stable_ma: number;
    timestamp: number;
    yield_price: number;
  };
  reserve_ratio: string;
  reserve_ratio_ma: string;
  status: string;
  zeph_reserve: string;
  zyield_reserve: string;
}

interface ReserveInfoResponse {
  id: string;
  jsonrpc: string;
  result: ReserveInfoResult;
}

interface CliOptions {
  outputDir: string;
  force: boolean;
}

async function fetchReserveInfo(): Promise<ReserveInfoResult> {
  const response = await fetch(`${RPC_URL}/json_rpc`, {
    method: "POST",
    headers: HEADERS,
    body: JSON.stringify({
      jsonrpc: "2.0",
      id: "0",
      method: "get_reserve_info",
    }),
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Failed to fetch reserve info (${response.status} ${response.statusText}): ${text}`);
  }

  const payload = (await response.json()) as ReserveInfoResponse;
  if (!payload?.result) {
    throw new Error("Reserve info response missing result payload");
  }

  return payload.result;
}

function parseArgs(): CliOptions {
  const args = process.argv.slice(2);
  let outputDir = process.env.RESERVE_SNAPSHOT_DIR ?? DEFAULT_OUTPUT_DIR;
  let force = false;

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === "--dir" || arg === "--out" || arg === "-d") {
      const next = args[i + 1];
      if (!next) {
        throw new Error(`${arg} requires a directory argument`);
      }
      outputDir = next;
      i += 1;
      continue;
    }

    if (arg === "--force" || arg === "-f") {
      force = true;
      continue;
    }

    if (arg === "--help" || arg === "-h") {
      printHelp();
      process.exit(0);
    }

    console.warn(`Unrecognised argument: ${arg}`);
  }

  return {
    outputDir,
    force,
  };
}

function printHelp() {
  console.log(
    `Usage: npx tsx scripts/reserveSnapshot.ts [options]\n\nOptions:\n  --dir, --out, -d <path>   Directory to write reserve snapshot files (default: ${DEFAULT_OUTPUT_DIR})\n  --force, -f               Overwrite existing snapshot for the same height\n  --help, -h                Show this help message\n\nEnvironment variables:\n  ZEPHYR_RPC_URL            Override the daemon RPC URL (default: http://127.0.0.1:17767)\n  RESERVE_SNAPSHOT_DIR      Default output directory override`
  );
}

function parseAtomic(value: string): number {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return Number.NaN;
  }
  return numeric * DEATOMIZE;
}

async function ensureDirectory(dir: string) {
  await fs.mkdir(dir, { recursive: true });
}

async function writeSnapshot(result: ReserveInfoResult, options: CliOptions) {
  const reserveHeight = result.height;
  const previousHeight = reserveHeight - 1;
  const outputDir = path.resolve(process.cwd(), options.outputDir);

  await ensureDirectory(outputDir);

  const filePath = path.join(outputDir, `${reserveHeight}.json`);

  if (!options.force) {
    try {
      await fs.access(filePath);
      console.error(`Snapshot ${filePath} already exists. Use --force to overwrite.`);
      process.exit(1);
    } catch (error) {
      // file does not exist - proceed
    }
  }

  const snapshot = {
    captured_at: new Date().toISOString(),
    reserve_height: reserveHeight,
    previous_height: previousHeight,
    hf_version: result.hf_version,
    on_chain: {
      zeph_reserve_atoms: result.zeph_reserve,
      zeph_reserve: parseAtomic(result.zeph_reserve),
      zsd_circ_atoms: result.num_stables,
      zsd_circ: parseAtomic(result.num_stables),
      zrs_circ_atoms: result.num_reserves,
      zrs_circ: parseAtomic(result.num_reserves),
      zyield_circ_atoms: result.num_zyield,
      zyield_circ: parseAtomic(result.num_zyield),
      zsd_yield_reserve_atoms: result.zyield_reserve,
      zsd_yield_reserve: parseAtomic(result.zyield_reserve),
      reserve_ratio_atoms: result.reserve_ratio,
      reserve_ratio: Number(result.reserve_ratio),
      reserve_ratio_ma_atoms: result.reserve_ratio_ma,
      reserve_ratio_ma: Number(result.reserve_ratio_ma),
    },
    pricing_record: result.pr,
    raw: result,
  };

  await fs.writeFile(filePath, JSON.stringify(snapshot, null, 2));

  console.log(`Saved reserve snapshot for height ${reserveHeight} (prev ${previousHeight}) to ${filePath}`);
}

async function main() {
  try {
    const options = parseArgs();
    const reserveInfo = await fetchReserveInfo();
    await writeSnapshot(reserveInfo, options);
  } catch (error) {
    console.error("Failed to save reserve snapshot:", error);
    process.exit(1);
  }
}

void main();
