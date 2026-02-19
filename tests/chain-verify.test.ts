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
 *   CHAIN_VERIFY_DELTA   — if set, skip DB reset and start scanning from this height
 *                          (use after a previous run to continue from a checkpoint)
 */

import { describe, expect, test, beforeAll, afterAll } from "bun:test";
import { execFileSync } from "child_process";
import { existsSync } from "fs";
import { resolve } from "path";
import { setupTestDatabase, resetTestData, teardownTestDatabase, getTestPrisma } from "./setup/db";

const HF_VERSION_1_HEIGHT = 89_300;  // Used for pre-V1 detection
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
  { height: 100, label: "chain_100", dir: "chain_100" },  // Pre-V1 (genesis scan)
  { height: 89_400, label: "chain_89400", dir: "chain_89400" },
  { height: 90_300, label: "chain_90300", dir: "chain_90300" },
  { height: 94_300, label: "chain_94300", dir: "chain_94300" },
  { height: 99_300, label: "chain_99300", dir: "chain_99300" },
  { height: 139_300, label: "chain_139300", dir: "chain_139300" },
  { height: 189_300, label: "chain_189300", dir: "chain_189300" },
  { height: 295_100, label: "chain_295100", dir: "chain_295100" },  // ARTEMIS V5
  { height: 360_100, label: "chain_360100", dir: "chain_360100" },  // V6/YIELD boundary
  { height: 481_600, label: "chain_481600", dir: "chain_481600" },  // AUDIT V8
  { height: 536_000, label: "chain_536000", dir: "chain_536000" },  // V11: blocks 0..535999 (pre-V11)
  { height: 536_001, label: "chain_536001", dir: "chain_536001" },  // V11: blocks 0..536000 (V11 fork block)
  { height: 536_002, label: "chain_536002", dir: "chain_536002" },  // V11: blocks 0..536001 (reset at 536001)
  { height: 536_100, label: "chain_536100", dir: "chain_536100" },  // V11 +100
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

async function getOnChainReserveInfo(): Promise<ReserveInfo | null> {
  const data = await rpcCall("get_reserve_info");
  return (data.result as ReserveInfo) ?? null;
}

async function getOnChainCirculatingSupply(): Promise<Map<string, number>> {
  const data = await rpcCall("get_circulating_supply");
  const tally = data.result?.supply_tally as Array<{ currency_label: string; amount: string }> | undefined;
  const map = new Map<string, number>();
  if (tally) {
    for (const entry of tally) {
      map.set(entry.currency_label.toUpperCase(), Number(entry.amount) * DEATOMIZE);
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

// Tolerance for atom-level comparisons.
// ZEPH circ: pre-V1 base_reward = governance * 20 loses up to 19 atoms/block (avg ~9).
// Other fields: minor rounding drift (ZSD ~31K atoms, ZRS ~16K at full chain).
function zephToleranceAtoms(scanEndBlock: number): number {
  return Math.max(1_000_000, scanEndBlock * 10);
}
const DEFAULT_TOLERANCE_ATOMS = 100_000;
// At V11 (block 536,000), circ values are reset to audited amounts. Summing many
// floating-point audit tx amounts introduces drift vs the daemon's exact integer math.
// Known post-V11 drifts: ZSD ~0.92, ZRS ~2.9, ZYS ~0.28 display units.
const HF_V11_HEIGHT = 536_000;
const V11_TOLERANCE_ATOMS = 5_000_000_000_000; // 5 display units covers all known V11 drift
// ZEPH in reserve: daemon has a hardcoded correction at height 274,662 (~0.625 ZEPH).
const ZEPH_RESERVE_TOLERANCE_ATOMS = 1_000_000_000_000;
function assetToleranceAtoms(scanEndBlock: number): number {
  return scanEndBlock > HF_V11_HEIGHT ? V11_TOLERANCE_ATOMS : DEFAULT_TOLERANCE_ATOMS;
}

function expectWithinTolerance(d: DiffResult, tolerance: number): void {
  if (d.diffAtoms > tolerance) {
    console.log(`  FAIL: ${d.field}: off by ${d.diffAtoms} atoms (${d.diff.toFixed(12)}) > tolerance ${tolerance}`);
  }
  expect(d.diffAtoms).toBeLessThanOrEqual(tolerance);
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
      const isPreV1 = typeof target.height === "number" && target.height < HF_VERSION_1_HEIGHT;

      // Shared comparison data (populated by "prepare comparison data" test)
      let scannerStats: any;
      let reserveInfo: ReserveInfo | null;
      let circulatingSupply: Map<string, number>;

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

        // Delta mode: skip DB reset and continue from a previous checkpoint.
        const deltaStart = process.env.CHAIN_VERIFY_DELTA;
        if (deltaStart) {
          console.log(`Delta mode: continuing from height ${deltaStart} (DB state preserved)`);
          process.env.START_BLOCK = deltaStart;
        } else {
          await setupTestDatabase();
          await resetTestData();
          process.env.START_BLOCK = "0";
        }

        process.env.END_BLOCK = chainHeight.toString();
        process.env.ZEPHYR_RPC_URL = RPC_URL;
      }, 180_000);

      afterAll(async () => {
        stopDaemon(dataDir);
        await teardownTestDatabase();
      }, 60_000);

      test("scan pricing records", async () => {
        if (process.env.UNIFIED_SCAN === "true") {
          const { scanBlocksUnified } = await import("../src/scan-unified");
          await scanBlocksUnified();
        } else {
          const { scanPricingRecords } = await import("../src/pr");
          await scanPricingRecords();
        }

        const { stores } = await import("../src/storage/factory");
        const latestHeight = await stores.pricing.getLatestHeight();
        if (!isPreV1) {
          expect(latestHeight).toBe(scanEndBlock);
        }
      }, 3_600_000);

      test("scan transactions", async () => {
        if (process.env.UNIFIED_SCAN === "true") {
          const { stores } = await import("../src/storage/factory");
          const txHeight = await stores.scannerState.get("height_txs");
          expect(Number(txHeight)).toBe(scanEndBlock);
          return;
        }
        const { scanTransactions } = await import("../src/tx");
        await scanTransactions();

        const { stores } = await import("../src/storage/factory");
        const txHeight = await stores.scannerState.get("height_txs");
        expect(Number(txHeight)).toBe(scanEndBlock);
        const prsHeight = await stores.pricing.getLatestHeight();
        expect(prsHeight).toBe(scanEndBlock);
      }, 3_600_000);

      test("run aggregator", async () => {
        const { aggregate } = await import("../src/aggregator");
        await aggregate();

        const { stores } = await import("../src/storage/factory");
        const aggHeight = await stores.scannerState.get("height_aggregator");
        expect(Number(aggHeight)).toBe(scanEndBlock);
      }, 3_600_000);

      test("prepare comparison data", async () => {
        // Compute totals (same as production scanner)
        const { calculateTotalsFromPostgres, setTotals } = await import("../src/db/totals");
        const totals = await calculateTotalsFromPostgres();
        await setTotals(totals);

        // Build totals summary and log health table
        const { getTotalsSummaryData, getLatestProtocolStats, getLatestReserveSnapshot } = await import("../src/utils");
        const { logTotals, logScannerHealth } = await import("../src/logger");
        const rawTotals = await getTotalsSummaryData();
        const totalsSummary = rawTotals ? logTotals(rawTotals as Record<string, unknown>, scanEndBlock) : null;

        const [latestStats, latestSnapshot] = await Promise.all([
          getLatestProtocolStats(),
          getLatestReserveSnapshot(),
        ]);
        await logScannerHealth(totalsSummary, latestStats, latestSnapshot);

        // Get scanner stats at the final block
        const prisma = getTestPrisma();
        scannerStats = await prisma.protocolStatsBlock.findUnique({
          where: { blockHeight: scanEndBlock },
        });
        expect(scannerStats).not.toBeNull();

        // Get on-chain data from daemon RPCs
        circulatingSupply = await getOnChainCirculatingSupply();
        reserveInfo = await getOnChainReserveInfo();
      }, 60_000);

      // --- 11 checks: scanner vs daemon, mirroring the health table ---
      //
      // Net totals (tx scanner sums):  mint/redeem volumes summed across all blocks
      // Per-block (aggregator state):  running protocol state at the final block
      //
      // These are two independent code paths that should agree with each other
      // and with the daemon. Net totals come from the tx scanner; per-block state
      // comes from the aggregator.

      // Helper: resolve on-chain value for a given supply key
      const supplyOnChain = (key: string, fallbackKey: string, reserveFallback?: string) => {
        return circulatingSupply.get(key) ?? circulatingSupply.get(fallbackKey)
          ?? (reserveFallback !== undefined ? Number(reserveFallback) * DEATOMIZE : 0);
      };

      // ── Net Totals (tx scanner sums vs daemon) ──

      test("net totals: ZEPH circ vs daemon", () => {
        // get_circulating_supply ZPH/ZEPH = m_coinbase (total emission, patched in RPC handler)
        const zephCircOnChain = circulatingSupply.get("ZPH") ?? circulatingSupply.get("ZEPH");
        expect(zephCircOnChain).toBeDefined();
        expectWithinTolerance(
          diff("ZEPH circ", zephCircOnChain!, scannerStats.zephCirc),
          zephToleranceAtoms(scanEndBlock),
        );
      });

      test("net totals: ZSD circ vs daemon", () => {
        if (isPreV1) { expect(scannerStats.zephusdCirc).toBe(0); return; }
        // Health table uses running state (accounts for V11 reset), not mint-redeem sums
        const onChain = supplyOnChain("ZSD", "ZEPHUSD", reserveInfo?.num_stables);
        expectWithinTolerance(diff("ZSD circ [net totals]", onChain, scannerStats.zephusdCirc), assetToleranceAtoms(scanEndBlock));
      });

      test("net totals: ZRS circ vs daemon", () => {
        if (isPreV1) { expect(scannerStats.zephrsvCirc).toBe(0); return; }
        const onChain = supplyOnChain("ZRS", "ZEPHRSV", reserveInfo?.num_reserves);
        expectWithinTolerance(diff("ZRS circ [net totals]", onChain, scannerStats.zephrsvCirc), assetToleranceAtoms(scanEndBlock));
      });

      test("net totals: ZYS circ vs daemon", () => {
        if (isPreV1) { expect(scannerStats.zyieldCirc).toBe(0); return; }
        const onChain = supplyOnChain("ZYS", "ZYIELD", reserveInfo?.num_zyield);
        expectWithinTolerance(diff("ZYS circ [net totals]", onChain, scannerStats.zyieldCirc), assetToleranceAtoms(scanEndBlock));
      });

      // ── Per-Block State (aggregator vs daemon) ──

      test("per-block: ZEPH circ vs daemon", () => {
        const zephCircOnChain = circulatingSupply.get("ZPH") ?? circulatingSupply.get("ZEPH");
        expect(zephCircOnChain).toBeDefined();
        expectWithinTolerance(
          diff("ZEPH circ [per-block]", zephCircOnChain!, scannerStats.zephCirc),
          zephToleranceAtoms(scanEndBlock),
        );
      });

      test("per-block: ZEPH in reserve vs daemon", () => {
        if (isPreV1) { expect(scannerStats.zephInReserve).toBe(0); return; }
        expect(reserveInfo).not.toBeNull();
        expectWithinTolerance(
          diff("ZEPH in reserve", Number(reserveInfo!.zeph_reserve) * DEATOMIZE, scannerStats.zephInReserve),
          ZEPH_RESERVE_TOLERANCE_ATOMS,
        );
      });

      test("per-block: ZSD circ vs daemon", () => {
        if (isPreV1) { expect(scannerStats.zephusdCirc).toBe(0); return; }
        expect(reserveInfo).not.toBeNull();
        expectWithinTolerance(
          diff("ZSD circ [per-block]", Number(reserveInfo!.num_stables) * DEATOMIZE, scannerStats.zephusdCirc),
          assetToleranceAtoms(scanEndBlock),
        );
      });

      test("per-block: ZSD in yield reserve vs daemon", () => {
        if (isPreV1) { expect(scannerStats.zsdInYieldReserve).toBe(0); return; }
        expect(reserveInfo).not.toBeNull();
        expectWithinTolerance(
          diff("ZSD in yield reserve", Number(reserveInfo!.zyield_reserve) * DEATOMIZE, scannerStats.zsdInYieldReserve),
          assetToleranceAtoms(scanEndBlock),
        );
      });

      test("per-block: ZRS circ vs daemon", () => {
        if (isPreV1) { expect(scannerStats.zephrsvCirc).toBe(0); return; }
        expect(reserveInfo).not.toBeNull();
        expectWithinTolerance(
          diff("ZRS circ [per-block]", Number(reserveInfo!.num_reserves) * DEATOMIZE, scannerStats.zephrsvCirc),
          assetToleranceAtoms(scanEndBlock),
        );
      });

      test("per-block: ZYS circ vs daemon", () => {
        if (isPreV1) { expect(scannerStats.zyieldCirc).toBe(0); return; }
        expect(reserveInfo).not.toBeNull();
        expectWithinTolerance(
          diff("ZYS circ [per-block]", Number(reserveInfo!.num_zyield) * DEATOMIZE, scannerStats.zyieldCirc),
          assetToleranceAtoms(scanEndBlock),
        );
      });

      test("per-block: reserve ratio vs daemon", () => {
        if (isPreV1) {
          expect(scannerStats.reserveRatio === null || scannerStats.reserveRatio === 0).toBe(true);
          return;
        }
        expect(reserveInfo).not.toBeNull();
        const rrOnChain = Number(reserveInfo!.reserve_ratio);
        const rrScanner = scannerStats.reserveRatio;
        expect(Number.isFinite(rrOnChain)).toBe(true);
        expect(rrScanner).not.toBeNull();
        const rrDiff = Math.abs(rrOnChain - rrScanner!);
        if (rrDiff > 1) {
          console.log(`  FAIL: Reserve ratio: ${rrScanner} vs on-chain ${rrOnChain} (diff ${rrDiff.toFixed(4)}%)`);
        }
        expect(rrDiff).toBeLessThanOrEqual(1);
      });
    });
  }
});
