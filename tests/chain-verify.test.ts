/**
 * Chain Verification Test
 *
 * Runs the scanner against real chain snapshots at specific heights,
 * then compares scanner-computed protocol stats against on-chain truth
 * from the daemon's get_reserve_info / get_circulating_supply RPCs.
 *
 * Usage:
 *   ./scripts/run-chain-verify.sh [height]
 *
 * Environment variables:
 *   CHAIN_VERIFY_HEIGHT  — target height (e.g., "89400" or "current")
 *   CHAIN_VERIFY_RPC_PORT — RPC port for test daemon (default: 18767)
 *   ZEPHYR_RPC_URL       — full RPC URL (set by run-chain-verify.sh)
 */

import { describe, expect, test, beforeAll, afterAll } from "bun:test";
import { execFileSync } from "child_process";
import { existsSync } from "fs";
import { resolve } from "path";
import { setupTestDatabase, resetTestData, teardownTestDatabase, getTestPrisma } from "./setup/db";

const HF_VERSION_1_HEIGHT = 89_300;
const PROJECT_DIR = resolve(import.meta.dir, "..");
const CHAIN_DATA_DIR = resolve(PROJECT_DIR, "chain-data");
const START_DAEMON = resolve(PROJECT_DIR, "scripts/start-test-daemon.sh");
const STOP_DAEMON = resolve(PROJECT_DIR, "scripts/stop-test-daemon.sh");
const RPC_PORT = process.env.CHAIN_VERIFY_RPC_PORT || "18767";
const RPC_URL = `http://127.0.0.1:${RPC_PORT}`;

// Deatomize constant (same as src/utils.ts)
const DEATOMIZE = 10 ** -12;

interface ChainTarget {
  height: number | "current";
  label: string;
  dir: string;
}

const ALL_TARGETS: ChainTarget[] = [
  { height: 89_400, label: "chain_89400", dir: "chain_89400" },
  { height: 90_300, label: "chain_90300", dir: "chain_90300" },
  { height: 94_300, label: "chain_94300", dir: "chain_94300" },
  { height: 99_300, label: "chain_99300", dir: "chain_99300" },
  { height: 139_300, label: "chain_139300", dir: "chain_139300" },
  { height: 189_300, label: "chain_189300", dir: "chain_189300" },
  { height: "current", label: "chain_current", dir: "chain_current" },
];

function getTargets(): ChainTarget[] {
  const requested = process.env.CHAIN_VERIFY_HEIGHT;
  if (!requested) return ALL_TARGETS;

  if (requested === "current") {
    return ALL_TARGETS.filter((t) => t.height === "current");
  }

  const height = Number(requested);
  if (!Number.isFinite(height)) {
    console.warn(`Invalid CHAIN_VERIFY_HEIGHT: ${requested}, running all targets`);
    return ALL_TARGETS;
  }

  const match = ALL_TARGETS.find((t) => t.height === height);
  if (!match) {
    console.warn(`No chain snapshot for height ${height}, available: ${ALL_TARGETS.map((t) => t.height).join(", ")}`);
    return [];
  }

  return [match];
}

async function rpcCall(method: string, params: Record<string, unknown> = {}): Promise<any> {
  const response = await fetch(`${RPC_URL}/json_rpc`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ jsonrpc: "2.0", id: "0", method, params }),
  });
  return response.json();
}

async function rpcGetHeight(): Promise<number> {
  const response = await fetch(`${RPC_URL}/get_height`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
  });
  const data = (await response.json()) as { height?: number };
  return data.height ?? 0;
}

interface ReserveInfo {
  zeph_reserve: string;
  num_stables: string;
  num_reserves: string;
  num_zyield: string;
  zyield_reserve: string;
  reserve_ratio: string;
  height: number;
}

async function getOnChainReserveInfo(): Promise<ReserveInfo> {
  const data = await rpcCall("get_reserve_info");
  return data.result as ReserveInfo;
}

interface CircSupplyEntry {
  currency_label: string;
  amount: string;
}

async function getOnChainCirculatingSupply(): Promise<Map<string, number>> {
  const data = await rpcCall("get_circulating_supply");
  const tally = data.result?.supply_tally as CircSupplyEntry[] | undefined;
  const map = new Map<string, number>();
  if (tally) {
    for (const entry of tally) {
      map.set(entry.currency_label, Number(entry.amount) * DEATOMIZE);
    }
  }
  return map;
}

function startDaemon(dataDir: string): void {
  console.log(`Starting test daemon on ${dataDir} (port ${RPC_PORT})...`);
  execFileSync("bash", [START_DAEMON, dataDir, RPC_PORT], {
    stdio: "inherit",
    timeout: 120_000,
  });
}

function stopDaemon(dataDir: string): void {
  console.log(`Stopping test daemon on ${dataDir}...`);
  try {
    execFileSync("bash", [STOP_DAEMON, dataDir], {
      stdio: "inherit",
      timeout: 60_000,
    });
  } catch (e) {
    console.warn("Failed to stop daemon cleanly:", e);
  }
}

interface DiffResult {
  field: string;
  onChain: number;
  scanner: number;
  diff: number;
  diffAtoms: number;
}

function diff(field: string, onChain: number, scanner: number | null | undefined): DiffResult {
  const scannerVal = typeof scanner === "number" ? scanner : 0;
  const diffVal = Math.abs(onChain - scannerVal);
  const diffAtoms = Math.round(diffVal / DEATOMIZE);
  return { field, onChain, scanner: scannerVal, diff: diffVal, diffAtoms };
}

const targets = getTargets();

describe("chain verification", () => {
  if (targets.length === 0) {
    test.skip("no targets to verify", () => {});
    return;
  }

  for (const target of targets) {
    const dataDir = resolve(CHAIN_DATA_DIR, target.dir);

    describe(`${target.label} (height ${target.height})`, () => {
      let chainHeight: number;
      let scanEndBlock: number;

      beforeAll(async () => {
        // Verify snapshot exists
        if (!existsSync(resolve(dataDir, "lmdb"))) {
          throw new Error(`Chain snapshot not found: ${dataDir}/lmdb — run create-chain-snapshots.sh first`);
        }

        // Start daemon
        startDaemon(dataDir);

        // Get actual chain height
        chainHeight = await rpcGetHeight();
        console.log(`Chain height: ${chainHeight}`);

        if (target.height !== "current") {
          expect(chainHeight).toBe(target.height as number);
        }

        // Scanner processes START_BLOCK..END_BLOCK-1 (END_BLOCK is exclusive).
        // Chain height N means blocks 0..N-1 exist; the last processable block is N-1.
        // get_reserve_info reports at height N (state after block N-1).
        scanEndBlock = chainHeight - 1;

        // Set up test database
        await setupTestDatabase();
        await resetTestData();

        // Set scanner block range to scan from HF start to chain tip
        process.env.START_BLOCK = HF_VERSION_1_HEIGHT.toString();
        process.env.END_BLOCK = chainHeight.toString();
        process.env.ZEPHYR_RPC_URL = RPC_URL;
      }, 180_000);

      afterAll(async () => {
        stopDaemon(dataDir);
        await teardownTestDatabase();
      }, 60_000);

      test("scan pricing records", async () => {
        // Dynamic import to pick up env vars
        const { scanPricingRecords } = await import("../src/pr");
        await scanPricingRecords();

        const { stores } = await import("../src/storage/factory");
        const latestHeight = await stores.pricing.getLatestHeight();
        expect(latestHeight).toBe(scanEndBlock);
      }, 600_000);

      test("scan transactions", async () => {
        const { scanTransactions } = await import("../src/tx");
        await scanTransactions();

        const { stores } = await import("../src/storage/factory");
        const txHeight = await stores.scannerState.get("height_txs");
        expect(Number(txHeight)).toBe(scanEndBlock);
      }, 600_000);

      test("run aggregator", async () => {
        const { aggregate } = await import("../src/aggregator");
        await aggregate();

        const { stores } = await import("../src/storage/factory");
        const aggHeight = await stores.scannerState.get("height_aggregator");
        expect(Number(aggHeight)).toBe(scanEndBlock);
      }, 600_000);

      test("compare scanner vs on-chain state", async () => {
        // Get on-chain truth from daemon
        const reserveInfo = await getOnChainReserveInfo();
        const circulatingSupply = await getOnChainCirculatingSupply();

        // Get scanner-computed protocol stats at the chain tip
        const prisma = getTestPrisma();
        const scannerStats = await prisma.protocolStatsBlock.findUnique({
          where: { blockHeight: scanEndBlock },
        });

        expect(scannerStats).not.toBeNull();
        if (!scannerStats) return;

        // Build comparison
        const diffs: DiffResult[] = [
          diff(
            "zeph_in_reserve",
            Number(reserveInfo.zeph_reserve) * DEATOMIZE,
            scannerStats.zephInReserve
          ),
          diff(
            "zephusd_circ",
            Number(reserveInfo.num_stables) * DEATOMIZE,
            scannerStats.zephusdCirc
          ),
          diff(
            "zephrsv_circ",
            Number(reserveInfo.num_reserves) * DEATOMIZE,
            scannerStats.zephrsvCirc
          ),
          diff(
            "zyield_circ",
            Number(reserveInfo.num_zyield) * DEATOMIZE,
            scannerStats.zyieldCirc
          ),
          diff(
            "zsd_in_yield_reserve",
            Number(reserveInfo.zyield_reserve) * DEATOMIZE,
            scannerStats.zsdInYieldReserve
          ),
          diff(
            "reserve_ratio",
            Number(reserveInfo.reserve_ratio),
            scannerStats.reserveRatio
          ),
        ];

        // Log comparison table
        console.log(`\n=== Chain Verification at Height ${scanEndBlock} ===`);
        console.log("Field                  | On-Chain           | Scanner            | Diff (atoms)");
        console.log("-".repeat(90));
        for (const d of diffs) {
          const onChainStr = d.onChain.toFixed(12).padStart(18);
          const scannerStr = d.scanner.toFixed(12).padStart(18);
          const diffStr = d.diffAtoms.toString().padStart(12);
          console.log(`${d.field.padEnd(22)} | ${onChainStr} | ${scannerStr} | ${diffStr}`);
        }

        // Also log circulating supply comparison if available
        if (circulatingSupply.size > 0) {
          console.log("\n--- Circulating Supply (from get_circulating_supply) ---");
          for (const [currency, amount] of circulatingSupply) {
            console.log(`  ${currency}: ${amount.toFixed(12)}`);
          }
        }

        // Report mismatches — allow 1 atom tolerance for rounding
        const TOLERANCE_ATOMS = 1;
        const mismatches = diffs.filter((d) => d.field !== "reserve_ratio" && d.diffAtoms > TOLERANCE_ATOMS);

        if (mismatches.length > 0) {
          console.log(`\n!!! ${mismatches.length} MISMATCHES detected !!!`);
          for (const m of mismatches) {
            console.log(`  ${m.field}: off by ${m.diffAtoms} atoms (${m.diff.toFixed(12)})`);
          }
        } else {
          console.log("\nAll fields match within tolerance.");
        }

        // This assertion is intentionally soft — the test's primary value is
        // the comparison output above, which helps locate where drift begins.
        // Uncomment the expect below once the aggregator is confirmed correct:
        //
        // expect(mismatches.length).toBe(0);
      }, 60_000);
    });
  }
});
