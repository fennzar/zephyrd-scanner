import dotenv from "dotenv";
import fetch from "node-fetch";
import { Agent } from "http"; // or 'https' for secure connections
import fs from "node:fs/promises";
import path from "node:path";

dotenv.config();

// Create a global agent
const agent = new Agent({ keepAlive: true });
import redis from "./redis";
const RPC_URL = "http://127.0.0.1:17767";
const HEADERS = {
  "Content-Type": "application/json",
};
const DEATOMIZE = 10 ** -12;
const RESERVE_SNAPSHOT_DIR = process.env.RESERVE_SNAPSHOT_DIR ?? "reserve_snapshots";
export const RESERVE_SNAPSHOT_INTERVAL_BLOCKS = Number(process.env.RESERVE_SNAPSHOT_INTERVAL_BLOCKS ?? "100");
export const RESERVE_SNAPSHOT_START_HEIGHT = Number(process.env.RESERVE_SNAPSHOT_START_HEIGHT ?? "89300");
const RESERVE_SNAPSHOT_REDIS_KEY = "reserve_snapshots";
export const HOURLY_PENDING_KEY = "protocol_stats_hourly_pending";
export const DAILY_PENDING_KEY = "protocol_stats_daily_pending";
const RESERVE_SNAPSHOT_LAST_KEY = "reserve_snapshots:last_previous_height";
export const RESERVE_SNAPSHOT_SOURCE = (process.env.RESERVE_SNAPSHOT_SOURCE ?? "redis").toLowerCase();
export const WALKTHROUGH_SNAPSHOT_SOURCE = (
  process.env.WALKTHROUGH_SNAPSHOT_SOURCE ?? RESERVE_SNAPSHOT_SOURCE
).toLowerCase();
export const RESERVE_DIFF_TOLERANCE = Number(process.env.RESERVE_DIFF_TOLERANCE ?? "1");
const RESERVE_MISMATCH_REDIS_KEY = "reserve_mismatch_heights";

export async function getCurrentBlockHeight(): Promise<number> {
  try {
    const response = await fetch(`${RPC_URL}/get_height`, {
      method: "POST",
      headers: HEADERS,
    });

    const responseData = await response.json();

    // Check if responseData is an object and has the 'height' property
    if (responseData && typeof responseData === "object" && "height" in responseData) {
      if (typeof responseData.height === "number") {
        return responseData.height;
      } else {
        return 0;
      }
    } else {
      return 0;
    }
  } catch (e) {
    console.log(e);
    return 0;
  }
}

interface GetBlockResponse {
  id: string;
  jsonrpc: string;
  result: {
    blob: string;
    block_header: {
      block_size: number;
      block_weight: number;
      cumulative_difficulty: number;
      cumulative_difficulty_top64: number;
      depth: number;
      difficulty: number;
      difficulty_top64: number;
      hash: string;
      height: number;
      long_term_weight: number;
      major_version: number;
      miner_tx_hash: string;
      minor_version: number;
      nonce: number;
      num_txes: number;
      orphan_status: boolean;
      pow_hash: string;
      prev_hash: string;
      pricing_record: {
        moving_average: number;
        reserve: number;
        reserve_ma: number;
        signature: string;
        spot: number;
        stable: number;
        stable_ma: number;
        yield_price?: number;
        timestamp: number;
      };
      reward: number;
      timestamp: number;
      wide_cumulative_difficulty: string;
      wide_difficulty: string;
    };
    credits: number;
    json: string;
    miner_tx_hash: string;
    status: string;
    top_hash: string;
    tx_hashes: string[];
    untrusted: boolean;
  };
}

export async function getBlock(height: number) {
  try {
    const response = await fetch(`${RPC_URL}/json_rpc`, {
      method: "POST",
      headers: HEADERS,
      body: JSON.stringify({
        jsonrpc: "2.0",
        id: "0",
        method: "get_block",
        params: { height },
      }),
    });

    return (await response.json()) as GetBlockResponse;
  } catch (e) {
    console.log(e);
    console.log(`getBlock rpc no response - Daemon could be down - waiting 1 second...`);
    await new Promise((r) => setTimeout(r, 1000));
    return;
  }
}

interface ReserveInfoResponse {
  id: string;
  jsonrpc: string;
  result: {
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
    pr?: {
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
  };
}

export interface ReserveSnapshot {
  captured_at: string;
  reserve_height: number;
  previous_height: number;
  hf_version: number;
  on_chain: {
    zeph_reserve_atoms: string;
    zeph_reserve: number;
    zsd_circ_atoms: string;
    zsd_circ: number;
    zrs_circ_atoms: string;
    zrs_circ: number;
    zyield_circ_atoms: string;
    zyield_circ: number;
    zsd_yield_reserve_atoms: string;
    zsd_yield_reserve: number;
    reserve_ratio_atoms: string;
    reserve_ratio: number | null;
    reserve_ratio_ma_atoms?: string;
    reserve_ratio_ma?: number | null;
  };
  pricing_record?: ReserveInfoResponse["result"]["pr"];
  raw?: ReserveInfoResponse["result"];
}

interface ReserveSnapshotWithPath {
  snapshot: ReserveSnapshot;
  filePath: string;
}

function atomsToNumberFromString(value?: string): number {
  if (!value) {
    return 0;
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return 0;
  }
  return numeric * DEATOMIZE;
}

function ratioStringToNumber(value?: string): number | null {
  if (!value) {
    return 0;
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return null;
  }
  return numeric;
}

function buildReserveSnapshot(reserveInfo: ReserveInfoResponse): ReserveSnapshot | null {
  const result = reserveInfo?.result;
  if (!result || typeof result.height !== "number") {
    return null;
  }

  const reserveHeight = result.height;
  const previousHeight = reserveHeight - 1;

  const snapshot: ReserveSnapshot = {
    captured_at: new Date().toISOString(),
    reserve_height: reserveHeight,
    previous_height: previousHeight,
    hf_version: result.hf_version ?? 0,
    on_chain: {
      zeph_reserve_atoms: result.zeph_reserve ?? "0",
      zeph_reserve: atomsToNumberFromString(result.zeph_reserve),
      zsd_circ_atoms: result.num_stables ?? "0",
      zsd_circ: atomsToNumberFromString(result.num_stables),
      zrs_circ_atoms: result.num_reserves ?? "0",
      zrs_circ: atomsToNumberFromString(result.num_reserves),
      zyield_circ_atoms: result.num_zyield ?? "0",
      zyield_circ: atomsToNumberFromString(result.num_zyield),
      zsd_yield_reserve_atoms: result.zyield_reserve ?? "0",
      zsd_yield_reserve: atomsToNumberFromString(result.zyield_reserve),
      reserve_ratio_atoms: result.reserve_ratio ?? "0",
      reserve_ratio: ratioStringToNumber(result.reserve_ratio),
      reserve_ratio_ma_atoms: result.reserve_ratio_ma,
      reserve_ratio_ma: ratioStringToNumber(result.reserve_ratio_ma),
    },
    pricing_record: result.pr,
    raw: result,
  };

  return snapshot;
}

export async function saveReserveSnapshotToRedis(reserveInfo: ReserveInfoResponse): Promise<ReserveSnapshot | null> {
  const snapshot = buildReserveSnapshot(reserveInfo);
  if (!snapshot) {
    return null;
  }

  const key = snapshot.previous_height.toString();
  await redis.hset(RESERVE_SNAPSHOT_REDIS_KEY, key, JSON.stringify(snapshot));
  await redis.set(RESERVE_SNAPSHOT_LAST_KEY, key);
  return snapshot;
}

export async function getLastReserveSnapshotPreviousHeight(): Promise<number | null> {
  const height = await redis.get(RESERVE_SNAPSHOT_LAST_KEY);
  if (!height) {
    return null;
  }
  const parsed = Number(height);
  return Number.isFinite(parsed) ? parsed : null;
}

async function loadReserveSnapshotByPreviousHeightFromRedis(
  previousHeight: number
): Promise<ReserveSnapshotWithPath | null> {
  const json = await redis.hget(RESERVE_SNAPSHOT_REDIS_KEY, previousHeight.toString());
  if (!json) {
    return null;
  }
  try {
    const snapshot = JSON.parse(json) as ReserveSnapshot;
  return { snapshot, filePath: `redis:${previousHeight}` };
  } catch (error) {
    console.error(`Failed to parse reserve snapshot from redis for height ${previousHeight}:`, error);
    return null;
  }
}

export async function getLatestReserveSnapshot(): Promise<ReserveSnapshot | null> {
  const previousHeight = await getLastReserveSnapshotPreviousHeight();
  if (previousHeight === null) {
    return null;
  }

  const json = await redis.hget(RESERVE_SNAPSHOT_REDIS_KEY, previousHeight.toString());
  if (!json) {
    return null;
  }

  try {
    return JSON.parse(json) as ReserveSnapshot;
  } catch (error) {
    console.error(`Failed to parse latest reserve snapshot (previous_height=${previousHeight}):`, error);
    return null;
  }
}

export async function getReserveInfo() {
  try {
    const response = await fetch(`${RPC_URL}/json_rpc`, {
      method: "POST",
      headers: HEADERS,
      body: JSON.stringify({
        jsonrpc: "2.0",
        id: "0",
        method: "get_reserve_info",
      }),
    });

    return (await response.json()) as ReserveInfoResponse;
  } catch (e) {
    console.log(e);
    return;
  }
}

export async function getLatestProtocolStats(): Promise<ProtocolStats | null> {
  const allStats = await redis.hgetall("protocol_stats");
  if (!allStats) {
    return null;
  }

  let latest: ProtocolStats | null = null;
  let maxHeight = -1;

  for (const [heightStr, statsJson] of Object.entries(allStats)) {
    const height = Number(heightStr);
    if (Number.isNaN(height)) {
      continue;
    }
    if (height > maxHeight) {
      try {
        latest = JSON.parse(statsJson) as ProtocolStats;
        maxHeight = height;
      } catch (error) {
        console.error(`Failed to parse protocol stats for height ${heightStr}:`, error);
      }
    }
  }

  return latest;
}

export async function setProtocolStats(height: number, stats: ProtocolStats): Promise<void> {
  await redis
    .pipeline()
    .hset("protocol_stats", height.toString(), JSON.stringify(stats))
    .set("height_aggregator", height.toString())
    .exec();
}

function quantizeToAtoms(value: number) {
  if (!Number.isFinite(value)) {
    return { atoms: 0n, quantized: value };
  }
  const atoms = BigInt(Math.round(value / DEATOMIZE));
  const quantized = Number(atoms) * DEATOMIZE;
  return { atoms, quantized };
}

export interface ReserveDiffEntry {
  field: string;
  on_chain: number;
  cached: number;
  difference: number;
  difference_atoms?: number;
  note?: string;
}

function diffField(name: string, onChain: number, cached: number): ReserveDiffEntry {
  const bothNotFinite = !Number.isFinite(onChain) && !Number.isFinite(cached);
  if (bothNotFinite) {
    return { field: name, on_chain: onChain, cached, difference: 0, note: "non-finite" };
  }

  if (name !== "reserve_ratio") {
    const onChainQuant = quantizeToAtoms(onChain);
    const cachedQuant = quantizeToAtoms(cached);
    const diffAtoms = onChainQuant.atoms - cachedQuant.atoms;
    const difference = Math.abs(Number(diffAtoms)) * DEATOMIZE;

    return {
      field: name,
      on_chain: onChainQuant.quantized,
      cached: cachedQuant.quantized,
      difference,
      difference_atoms: Number(diffAtoms),
    };
  }

  const rawDiff = onChain - cached;
  const diffAtoms = Number.isFinite(rawDiff) ? Math.round(rawDiff / DEATOMIZE) : 0;
  const difference = Math.abs(rawDiff);
  return { field: name, on_chain: onChain, cached, difference, difference_atoms: diffAtoms };
}

async function readReserveSnapshotFile(filePath: string): Promise<ReserveSnapshot | null> {
  try {
    const raw = await fs.readFile(filePath, "utf8");
    return JSON.parse(raw) as ReserveSnapshot;
  } catch (error) {
    const err = error as NodeJS.ErrnoException;
    if (err.code === "ENOENT") {
      return null;
    }
    throw error;
  }
}

async function loadReserveSnapshotByReserveHeight(
  reserveHeight: number,
  snapshotDir = RESERVE_SNAPSHOT_DIR
): Promise<ReserveSnapshotWithPath | null> {
  const resolvedDir = path.resolve(process.cwd(), snapshotDir);
  const filePath = path.join(resolvedDir, `${reserveHeight}.json`);
  const snapshot = await readReserveSnapshotFile(filePath);
  if (!snapshot) {
    return null;
  }
  return { snapshot, filePath };
}

async function loadReserveSnapshotByPreviousHeight(
  previousHeight: number,
  snapshotDir = RESERVE_SNAPSHOT_DIR
): Promise<ReserveSnapshotWithPath | null> {
  const direct = await loadReserveSnapshotByReserveHeight(previousHeight + 1, snapshotDir);
  if (direct && direct.snapshot.previous_height === previousHeight) {
    return direct;
  }

  const resolvedDir = path.resolve(process.cwd(), snapshotDir);
  let files: string[] = [];
  try {
    files = await fs.readdir(resolvedDir);
  } catch (error) {
    const err = error as NodeJS.ErrnoException;
    if (err.code === "ENOENT") {
      return null;
    }
    throw error;
  }

  for (const file of files) {
    if (!file.endsWith(".json")) {
      continue;
    }
    const filePath = path.join(resolvedDir, file);
    if (direct && direct.filePath === filePath) {
      continue;
    }
    const snapshot = await readReserveSnapshotFile(filePath);
    if (snapshot && snapshot.previous_height === previousHeight) {
      return { snapshot, filePath };
    }
  }

  return null;
}

interface ReserveDiffOptions {
  targetHeight?: number;
  allowSnapshots?: boolean;
  snapshotDir?: string;
  snapshotSource?: "redis" | "file";
}

export interface ReserveDiffReport {
  block_height: number;
  reserve_height: number;
  diffs: ReserveDiffEntry[];
  mismatch: boolean;
  source: "rpc" | "snapshot";
  source_height?: number;
  snapshot_path?: string;
}

export async function recordReserveMismatch(height: number, report: ReserveDiffReport): Promise<void> {
  await redis.hset(RESERVE_MISMATCH_REDIS_KEY, height.toString(), JSON.stringify(report));
}

export async function clearReserveMismatch(height: number): Promise<void> {
  await redis.hdel(RESERVE_MISMATCH_REDIS_KEY, height.toString());
}

export async function getReserveDiffs(options: ReserveDiffOptions = {}): Promise<ReserveDiffReport> {
  const latestStats = await getLatestProtocolStats();
  if (!latestStats) {
    throw new Error("No protocol stats in cache");
  }

  const targetHeight = options.targetHeight ?? latestStats.block_height;
  const reserveInfo = await getReserveInfo();

  type OnChainMetrics = {
    zeph_in_reserve: number;
    zephusd_circ: number;
    zephrsv_circ: number;
    zyield_circ: number;
    zsd_in_yield_reserve: number;
    reserve_ratio: number;
  };

  const buildDiffs = (onChain: OnChainMetrics): ReserveDiffEntry[] => {
    const diffs: ReserveDiffEntry[] = [];
    diffs.push(diffField("zeph_in_reserve", onChain.zeph_in_reserve, latestStats.zeph_in_reserve));
    diffs.push(diffField("zephusd_circ", onChain.zephusd_circ, latestStats.zephusd_circ));
    diffs.push(diffField("zephrsv_circ", onChain.zephrsv_circ, latestStats.zephrsv_circ));
    diffs.push(diffField("zyield_circ", onChain.zyield_circ, latestStats.zyield_circ));
    diffs.push(diffField("zsd_in_yield_reserve", onChain.zsd_in_yield_reserve, latestStats.zsd_in_yield_reserve));
    diffs.push(diffField("reserve_ratio", onChain.reserve_ratio, latestStats.reserve_ratio));
    return diffs;
  };

  let reserveHeight = targetHeight;
  let sourceHeight: number | undefined;
  let source: "rpc" | "snapshot" = "rpc";
  let diffs: ReserveDiffEntry[] = [];
  let mismatch = false;
  let snapshotPath: string | undefined;

  const reserveInfoResult = reserveInfo?.result;
  if (reserveInfoResult) {
    sourceHeight = reserveInfoResult.height - 1;
  }

  if (reserveInfoResult && sourceHeight === targetHeight) {
    reserveHeight = sourceHeight;
    diffs = buildDiffs({
      zeph_in_reserve: Number(reserveInfoResult.zeph_reserve ?? 0) * DEATOMIZE,
      zephusd_circ: Number(reserveInfoResult.num_stables ?? 0) * DEATOMIZE,
      zephrsv_circ: Number(reserveInfoResult.num_reserves ?? 0) * DEATOMIZE,
      zyield_circ: Number(reserveInfoResult.num_zyield ?? 0) * DEATOMIZE,
      zsd_in_yield_reserve: Number(reserveInfoResult.zyield_reserve ?? 0) * DEATOMIZE,
      reserve_ratio: Number(reserveInfoResult.reserve_ratio ?? 0),
    });
  } else if (options.allowSnapshots) {
    const requestedSource = (options.snapshotSource ?? WALKTHROUGH_SNAPSHOT_SOURCE).toLowerCase();
    let snapshotResult: ReserveSnapshotWithPath | null = null;
    if (requestedSource === "redis") {
      snapshotResult = await loadReserveSnapshotByPreviousHeightFromRedis(targetHeight);
      if (!snapshotResult) {
        snapshotResult = await loadReserveSnapshotByPreviousHeight(targetHeight, options.snapshotDir);
      }
    } else {
      snapshotResult = await loadReserveSnapshotByPreviousHeight(targetHeight, options.snapshotDir);
      if (!snapshotResult) {
        snapshotResult = await loadReserveSnapshotByPreviousHeightFromRedis(targetHeight);
      }
    }

    if (snapshotResult) {
      const { snapshot, filePath } = snapshotResult;
      source = "snapshot";
      reserveHeight = snapshot.previous_height;
      sourceHeight = snapshot.previous_height;
      snapshotPath = filePath;
      diffs = buildDiffs({
        zeph_in_reserve: snapshot.on_chain.zeph_reserve ?? 0,
        zephusd_circ: snapshot.on_chain.zsd_circ ?? 0,
        zephrsv_circ: snapshot.on_chain.zrs_circ ?? 0,
        zyield_circ: snapshot.on_chain.zyield_circ ?? 0,
        zsd_in_yield_reserve: snapshot.on_chain.zsd_yield_reserve ?? 0,
        reserve_ratio: snapshot.on_chain.reserve_ratio ?? 0,
      });
    } else if (reserveInfoResult) {
      reserveHeight = sourceHeight ?? targetHeight;
      diffs = buildDiffs({
        zeph_in_reserve: Number(reserveInfoResult.zeph_reserve ?? 0) * DEATOMIZE,
        zephusd_circ: Number(reserveInfoResult.num_stables ?? 0) * DEATOMIZE,
        zephrsv_circ: Number(reserveInfoResult.num_reserves ?? 0) * DEATOMIZE,
        zyield_circ: Number(reserveInfoResult.num_zyield ?? 0) * DEATOMIZE,
        zsd_in_yield_reserve: Number(reserveInfoResult.zyield_reserve ?? 0) * DEATOMIZE,
        reserve_ratio: Number(reserveInfoResult.reserve_ratio ?? 0),
      });
      mismatch = true;
    } else {
      mismatch = true;
    }
  } else if (reserveInfoResult) {
    reserveHeight = sourceHeight ?? targetHeight;
    diffs = buildDiffs({
      zeph_in_reserve: Number(reserveInfoResult.zeph_reserve ?? 0) * DEATOMIZE,
      zephusd_circ: Number(reserveInfoResult.num_stables ?? 0) * DEATOMIZE,
      zephrsv_circ: Number(reserveInfoResult.num_reserves ?? 0) * DEATOMIZE,
      zyield_circ: Number(reserveInfoResult.num_zyield ?? 0) * DEATOMIZE,
      zsd_in_yield_reserve: Number(reserveInfoResult.zyield_reserve ?? 0) * DEATOMIZE,
      reserve_ratio: Number(reserveInfoResult.reserve_ratio ?? 0),
    });
    mismatch = true;
  } else {
    mismatch = true;
  }

  return {
    block_height: latestStats.block_height,
    reserve_height: reserveHeight,
    diffs,
    mismatch,
    source,
    source_height: sourceHeight,
    snapshot_path: snapshotPath,
  };
}
export async function getPricingRecordFromBlock(height: number) {
  const blockData = await getBlock(height);
  if (!blockData) {
    return;
  }

  const pricingRecord = blockData.result.block_header.pricing_record;
  return pricingRecord;
}

export async function readTx(hash: string) {
  try {
    const response = await fetch(`${RPC_URL}/get_transactions`, {
      method: "POST",
      headers: HEADERS,
      body: JSON.stringify({
        txs_hashes: [hash],
        decode_as_json: true,
      }),
      agent: agent, // Use the agent in your fetch request
    });

    if (!response.ok) {
      console.error(`HTTP error! status: ${response.status}`);
      return null;
    }

    return await response.json();
  } catch (error) {
    console.error("Error in fetching transaction:", error);
    return null;
  }
}

export async function getTotalsFromRedis() {
  const totals = await redis.hgetall("totals");
  if (!totals) {
    return null;
  }
  return totals;
}
// Interface for block-level protocol stats response with generic type
export interface BlockProtocolStatsResponse<T extends keyof ProtocolStats> {
  block_height: number;
  data: Pick<ProtocolStats, T>;
}

// Interface for hour or day-level protocol stats response with generic type
export interface AggregatedProtocolStatsResponse<T extends keyof AggregatedData> {
  timestamp: number; // UNIX timestamp
  data: Pick<AggregatedData, T>;
}

// Function for block-level stats
export async function getBlockProtocolStatsFromRedis<T extends keyof ProtocolStats>(
  from?: string,
  to?: string,
  fields?: T[]
): Promise<BlockProtocolStatsResponse<T>[]> {
  let redisKey = "protocol_stats";
  let start = from ? parseInt(from) : 0;
  let end = to ? parseInt(to) : Number(await redis.get("height_aggregator"));
  let blockData: BlockProtocolStatsResponse<T>[] = [];

  for (let i = start; i <= end; i++) {
    const statsJson = await redis.hget(redisKey, i.toString());
    if (statsJson) {
      let stats = JSON.parse(statsJson);

      // If specific fields are requested, filter the data
      if (fields && fields.length > 0) {
        stats = filterFields(stats, fields);
      }

      blockData.push({ block_height: i, data: stats });
    }
  }
  return blockData;
}

// Function for hour/day-level stats
export async function getAggregatedProtocolStatsFromRedis<T extends keyof AggregatedData>(
  scale: "hour" | "day",
  from?: string,
  to?: string,
  fields?: T[]
): Promise<AggregatedProtocolStatsResponse<T>[]> {
  const redisKey = scale === "hour" ? "protocol_stats_hourly" : "protocol_stats_daily";
  const pendingKey = scale === "hour" ? HOURLY_PENDING_KEY : DAILY_PENDING_KEY;
  const startScore = from ? parseInt(from) : "-inf";
  const endScore = to ? parseInt(to) : "+inf";
  const results = await redis.zrangebyscore(redisKey, startScore, endScore, "WITHSCORES");

  const formatted = formatZrangeResults(results, fields);
  const existingTimestamps = new Set(formatted.map((entry) => entry.timestamp));

  const pendingJson = await redis.get(pendingKey);
  if (pendingJson) {
    try {
      let data = JSON.parse(pendingJson);
      const timestamp = Number(data?.window_start);
      if (!existingTimestamps.has(timestamp)) {
        if (fields && fields.length > 0) {
          data = filterFields(data, fields);
        }
        formatted.push({ timestamp, data });
        existingTimestamps.add(timestamp);
      }
    } catch (error) {
      console.error(`Failed to parse pending aggregated stats for ${pendingKey}:`, error);
    }
  }

  formatted.sort((a, b) => a.timestamp - b.timestamp);
  return formatted;
}

// Helper function for formatting results
function formatZrangeResults<T extends keyof AggregatedData>(
  results: any,
  fields?: T[]
): AggregatedProtocolStatsResponse<T>[] {
  let formattedResults: AggregatedProtocolStatsResponse<T>[] = [];
  for (let i = 0; i < results.length; i += 2) {
    let data = JSON.parse(results[i]);

    // If specific fields are requested, filter the data
    if (fields && fields.length > 0) {
      data = filterFields(data, fields);
    }

    formattedResults.push({ timestamp: Number(results[i + 1]), data });
  }
  return formattedResults;
}

// Helper function to filter data points based on requested fields
function filterFields<T extends string>(data: any, fields: T[]): Pick<typeof data, T> {
  let filteredData: Partial<typeof data> = {};
  for (const field of fields) {
    if (data && data.hasOwnProperty(field)) {
      filteredData[field] = data[field];
    }
  }
  return filteredData as Pick<typeof data, T>;
}

export interface ProtocolStats {
  block_height: number;
  block_timestamp: number;
  spot: number;
  moving_average: number;
  reserve: number;
  reserve_ma: number;
  stable: number;
  stable_ma: number;
  yield_price: number;
  zeph_in_reserve: number;
  zeph_in_reserve_atoms?: string;
  zsd_in_yield_reserve: number;
  zeph_circ: number;
  zephusd_circ: number;
  zephrsv_circ: number;
  zyield_circ: number;
  assets: number;
  assets_ma: number;
  liabilities: number;
  equity: number;
  equity_ma: number;
  reserve_ratio: number;
  reserve_ratio_ma: number;
  zsd_accrued_in_yield_reserve_from_yield_reward: number;
  zsd_minted_for_yield: number;
  conversion_transactions_count: number;
  yield_conversion_transactions_count: number;
  mint_reserve_count: number;
  mint_reserve_volume: number;
  fees_zephrsv: number;
  redeem_reserve_count: number;
  redeem_reserve_volume: number;
  fees_zephusd: number;
  mint_stable_count: number;
  mint_stable_volume: number;
  redeem_stable_count: number;
  redeem_stable_volume: number;
  fees_zeph: number;
  mint_yield_count: number;
  mint_yield_volume: number;
  redeem_yield_count: number;
  redeem_yield_volume: number;
  fees_zephusd_yield: number;
  fees_zyield: number;
}

export interface AggregatedData {
  // Prices
  spot_open: number;
  spot_close: number;
  spot_high: number;
  spot_low: number;
  moving_average_open: number;
  moving_average_close: number;
  moving_average_high: number;
  moving_average_low: number;
  reserve_open: number;
  reserve_close: number;
  reserve_high: number;
  reserve_low: number;
  reserve_ma_open: number;
  reserve_ma_close: number;
  reserve_ma_high: number;
  reserve_ma_low: number;
  stable_open: number;
  stable_close: number;
  stable_high: number;
  stable_low: number;
  stable_ma_open: number;
  stable_ma_close: number;
  stable_ma_high: number;
  stable_ma_low: number;
  zyield_price_open: number;
  zyield_price_close: number;
  zyield_price_high: number;
  zyield_price_low: number;

  // Circulating Reserve Amounts
  zeph_in_reserve_open: number;
  zeph_in_reserve_close: number;
  zeph_in_reserve_high: number;
  zeph_in_reserve_low: number;
  zsd_in_yield_reserve_open: number;
  zsd_in_yield_reserve_close: number;
  zsd_in_yield_reserve_high: number;
  zsd_in_yield_reserve_low: number;

  // Circulating Supply
  zeph_circ_open: number;
  zeph_circ_close: number;
  zeph_circ_high: number;
  zeph_circ_low: number;
  zephusd_circ_open: number;
  zephusd_circ_close: number;
  zephusd_circ_high: number;
  zephusd_circ_low: number;
  zephrsv_circ_open: number;
  zephrsv_circ_close: number;
  zephrsv_circ_high: number;
  zephrsv_circ_low: number;
  zyield_circ_open: number;
  zyield_circ_close: number;
  zyield_circ_high: number;
  zyield_circ_low: number;

  // Djed Mechanics Stats
  assets_open: number;
  assets_close: number;
  assets_high: number;
  assets_low: number;
  assets_ma_open: number;
  assets_ma_close: number;
  assets_ma_high: number;
  assets_ma_low: number;
  liabilities_open: number;
  liabilities_close: number;
  liabilities_high: number;
  liabilities_low: number;
  equity_open: number;
  equity_close: number;
  equity_high: number;
  equity_low: number;
  equity_ma_open: number;
  equity_ma_close: number;
  equity_ma_high: number;
  equity_ma_low: number;
  reserve_ratio_open: number;
  reserve_ratio_close: number;
  reserve_ratio_high: number;
  reserve_ratio_low: number;
  reserve_ratio_ma_open: number;
  reserve_ratio_ma_close: number;
  reserve_ratio_ma_high: number;
  reserve_ratio_ma_low: number;

  // Conversion Stats
  conversion_transactions_count: number;
  yield_conversion_transactions_count: number;
  mint_reserve_count: number;
  mint_reserve_volume: number;
  fees_zephrsv: number;
  redeem_reserve_count: number;
  redeem_reserve_volume: number;
  fees_zephusd: number;
  mint_stable_count: number;
  mint_stable_volume: number;
  redeem_stable_count: number;
  redeem_stable_volume: number;
  fees_zeph: number;
  mint_yield_count: number;
  mint_yield_volume: number;
  fees_zyield: number;
  redeem_yield_count: number;
  redeem_yield_volume: number;
  fees_zephusd_yield: number;
  pending?: boolean;
  window_start?: number;
  window_end?: number;
}

export async function getRedisHeight() {
  const height = await redis.get("height_aggregator");
  if (!height) {
    return 0;
  }
  return parseInt(height);
}

export async function getPricingRecordHeight() {
  const height = await redis.get("height_prs");
  if (!height) {
    return 0;
  }
  return parseInt(height);
}

export async function getTransactionHeight() {
  const height = await redis.get("height_txs");
  if (!height) {
    return 0;
  }
  return parseInt(height);
}

export async function getRedisTimestampHourly() {
  const height = await redis.get("timestamp_aggregator_hourly");
  if (!height) {
    return 0;
  }
  return parseInt(height);
}

export async function getRedisTimestampDaily() {
  const height = await redis.get("timestamp_aggregator_daily");
  if (!height) {
    return 0;
  }
  return parseInt(height);
}

export async function setRedisHeight(height: number) {
  await redis.set("height_aggregator", height);
}

export async function getRedisPricingRecord(height: number) {
  const pr = await redis.hget("pricing_records", height.toString());
  if (!pr) {
    return null;
  }
  return JSON.parse(pr);
}

export async function getRedisBlockRewardInfo(height: number) {
  const bri = await redis.hget("block_rewards", height.toString());
  if (!bri) {
    return null;
  }
  return JSON.parse(bri);
}

export async function getRedisTransaction(hash: string) {
  const txs = await redis.hget("txs", hash);
  if (!txs) {
    return null;
  }
  return JSON.parse(txs);
}

async function getCirculatingSuppliesFromExplorer() {
  try {
    // Fetch Zeph circulating supply
    const zephResponse = await fetch("https://explorer.zephyrprotocol.com/api/circulating");
    const zeph_circ = Number(await zephResponse.json());

    // WE CAN GET THIS FROM /get_reserve_info RPC COMMAND

    // // Fetch ZSD circulating supply
    // const zsdResponse = await fetch('https://explorer.zephyrprotocol.com/api/circulating/zsd');
    // const zsd_circ = Number(await zsdResponse.json());

    // // Fetch ZRS circulating supply
    // const zrsResponse = await fetch('https://explorer.zephyrprotocol.com/api/circulating/zrs');
    // const zrs_circ = Number(await zrsResponse.json());

    // // Fetch ZYS circulating supply
    // const zysResponse = await fetch('https://explorer.zephyrprotocol.com/api/circulating/zys');
    // const zys_circ = Number(await zysResponse.json());

    // Logging all the results
    // console.log('Zeph Circulating Supply:', zeph_circ);
    // console.log('ZSD Circulating Supply:', zsd_circ);
    // console.log('ZRS Circulating Supply:', zrs_circ);
    // console.log('ZYS Circulating Supply:', zys_circ);

    return {
      zeph_circ,
      // zsd_circ,
      // zrs_circ,
      // zys_circ
    };
  } catch (error) {
    console.error("Error fetching circulating supplies:", error);
    return {
      zeph_circ: 0,
      // zsd_circ: 0,
      // zrs_circ: 0,
      // zys_circ: 0
    };
  }
}

export async function getLiveStats() {
  try {
    // we can get prices from the pr?
    // get accurate circ from the explorer api
    // we can get circulating supply for all assets except ZEPH from the reserve info call.
    // we can get current and previous circ amounts from the aggregated data to get 24h change

    const currentBlockHeight = await getRedisHeight();
    console.log(`getLiveStats: Current Block Height: ${currentBlockHeight}`);

    const reserveInfo = await getReserveInfo();

    if (!reserveInfo || !reserveInfo.result) {
      console.log(`getLiveStats: No reserve info returned from daemon`);
      return;
    }

    const pricingRecord = reserveInfo.result.pr;
    const needsFallback =
      !pricingRecord ||
      typeof pricingRecord.spot !== "number" ||
      typeof pricingRecord.stable !== "number" ||
      typeof pricingRecord.reserve !== "number";

    const latestStats = needsFallback ? await getLatestProtocolStats() : null;

    if (needsFallback) {
      if (!latestStats) {
        console.log(`getLiveStats: Pricing record missing from daemon and no cached stats available`);
        return;
      }
      console.log(`getLiveStats: Pricing record missing from daemon, using cached protocol stats`);
    }

    const spotAtoms =
      typeof pricingRecord?.spot === "number" ? pricingRecord.spot : latestStats?.spot;
    const stableAtoms =
      typeof pricingRecord?.stable === "number" ? pricingRecord.stable : latestStats?.stable;
    const reserveAtoms =
      typeof pricingRecord?.reserve === "number" ? pricingRecord.reserve : latestStats?.reserve;
    const yieldPriceAtoms =
      typeof pricingRecord?.yield_price === "number"
        ? pricingRecord.yield_price
        : latestStats?.yield_price;

    if (
      typeof spotAtoms !== "number" ||
      typeof stableAtoms !== "number" ||
      typeof reserveAtoms !== "number" ||
      !Number.isFinite(spotAtoms) ||
      !Number.isFinite(stableAtoms) ||
      !Number.isFinite(reserveAtoms)
    ) {
      console.log(`getLiveStats: Unable to determine pricing metrics from daemon or cache`);
      return;
    }

    const zeph_price = Number((spotAtoms * DEATOMIZE).toFixed(4));
    const zsd_rate = Number((stableAtoms * DEATOMIZE).toFixed(4));
    const zsd_price = Number((zsd_rate * zeph_price).toFixed(4));
    const zrs_rate = Number((reserveAtoms * DEATOMIZE).toFixed(4));
    const zrs_price = Number((zrs_rate * zeph_price).toFixed(4));
    const zys_price =
      typeof yieldPriceAtoms === "number" && Number.isFinite(yieldPriceAtoms) && yieldPriceAtoms > 0
        ? Number((yieldPriceAtoms * DEATOMIZE).toFixed(4))
        : 1;

    const zsd_circ = Number(reserveInfo.result.num_stables) * DEATOMIZE;
    const zrs_circ = Number(reserveInfo.result.num_reserves) * DEATOMIZE;
    const zys_circ = Number(reserveInfo.result.num_zyield) * DEATOMIZE;

    // to calcuate the 24hr circulating supply change we can use the aggregated data, most recent protocol stats and 720 records ago
    // Fetch previous block's data for initialization
    const currentBlockProtocolStatsData = await redis.hget("protocol_stats", currentBlockHeight.toString());
    const currentBlockProtocolStats: ProtocolStats | null = currentBlockProtocolStatsData
      ? JSON.parse(currentBlockProtocolStatsData)
      : null;

    const onedayagoBlockProtocolStatsData = await redis.hget("protocol_stats", (currentBlockHeight - 720).toString());
    const onedayagoBlockProtocolStats: ProtocolStats | null = onedayagoBlockProtocolStatsData
      ? JSON.parse(onedayagoBlockProtocolStatsData)
      : null;

    if (!onedayagoBlockProtocolStats || !currentBlockProtocolStats) {
      console.log(
        `getLiveStats: No currentBlockProtocolStats or onedayagoBlockProtocolStats found for blocks: ${currentBlockHeight} & ${
          currentBlockHeight - 720
        }`
      );
      return;
    }

    // DEBUG print out the current and previous block protocol stats
    // console.log(`getLiveStats: currentBlockProtocolStats:`, currentBlockProtocolStats);
    // console.log(`getLiveStats: onedayagoBlockProtocolStats:`, onedayagoBlockProtocolStats);

    const zeph_circ_from_explorer = (await getCirculatingSuppliesFromExplorer()).zeph_circ; // Source of "Truth"
    const zeph_circ_currentBlockProtocolStatus = currentBlockProtocolStats.zeph_circ; // Fallback

    // warn if explorer and currentBlockProtocolStats differ by more than 1000
    if (Math.abs(zeph_circ_from_explorer - zeph_circ_currentBlockProtocolStatus) > 1000) {
      console.warn(
        `getLiveStats: Zeph circulating supply from explorer (${zeph_circ_from_explorer}) does not match currentBlockProtocolStats (${zeph_circ_currentBlockProtocolStatus}) significantly || difference: ${Math.abs(
          zeph_circ_from_explorer - zeph_circ_currentBlockProtocolStatus
        )}`
      );
    }
    const zeph_circ = zeph_circ_from_explorer || zeph_circ_currentBlockProtocolStatus; // Use explorer data if available, otherwise fallback to currentBlockProtocolStats

    // We don't use the accurate current circulating supply from the explorer api to comapre to as there may be an issue with the aggregated data
    const zeph_circ_daily_change = currentBlockProtocolStats.zeph_circ - onedayagoBlockProtocolStats.zeph_circ;
    const zsd_circ_daily_change = currentBlockProtocolStats.zephusd_circ - onedayagoBlockProtocolStats.zephusd_circ;
    const zrs_circ_daily_change = currentBlockProtocolStats.zephrsv_circ - onedayagoBlockProtocolStats.zephrsv_circ;
    const zys_circ_daily_change = currentBlockProtocolStats.zyield_circ - onedayagoBlockProtocolStats.zyield_circ;

    if (zeph_circ_daily_change < 0) {
      console.log(`getLiveStats: zeph_circ_daily_change is negative: ${zeph_circ_daily_change}`);
    }

    const zeph_in_reserve = Number(reserveInfo.result.zeph_reserve) * DEATOMIZE;
    const zeph_in_reserve_value = zeph_in_reserve * zeph_price;

    const zsd_in_yield_reserve = Number(reserveInfo.result.zyield_reserve) * DEATOMIZE;
    const reserve_ratio_value_raw = reserveInfo.result.reserve_ratio ?? latestStats?.reserve_ratio ?? 0;
    const reserve_ratio_value = Number(reserve_ratio_value_raw);

    const reserve_ratio_ma_value_raw = reserveInfo.result.reserve_ratio_ma ?? latestStats?.reserve_ratio_ma ?? null;
    const reserve_ratio_ma_value =
      typeof reserve_ratio_ma_value_raw === "number"
        ? reserve_ratio_ma_value_raw
        : reserve_ratio_ma_value_raw == null
        ? null
        : Number(reserve_ratio_ma_value_raw);
    const reserve_ratio_ma = reserve_ratio_ma_value != null && Number.isFinite(reserve_ratio_ma_value)
      ? reserve_ratio_ma_value
      : null;

    const zeph_in_reserve_percent = zeph_in_reserve / zeph_circ;
    const zsd_in_yield_reserve_percent = zsd_in_yield_reserve / zsd_circ;

    let zysCurrentVariableApy: number | null = null;

    const projectedReturnsRaw = await redis.get("projected_returns");
    if (projectedReturnsRaw) {
      try {
        const projectedReturns = JSON.parse(projectedReturnsRaw) as {
          oneYear?: { simple?: { return?: number } };
        };
        const simpleReturn = projectedReturns.oneYear?.simple?.return;
        if (typeof simpleReturn === "number" && Number.isFinite(simpleReturn)) {
          zysCurrentVariableApy = Number(simpleReturn.toFixed(4));
        }
      } catch (error) {
        console.warn("getLiveStats: Unable to parse projected returns for current variable APY", error);
      }
    }

    const liveStats = {
      zeph_price,
      zsd_rate,
      zsd_price,
      zrs_rate,
      zrs_price,
      zys_price,
      zeph_circ,
      zsd_circ,
      zrs_circ,
      zys_circ,
      zeph_circ_daily_change,
      zsd_circ_daily_change,
      zrs_circ_daily_change,
      zys_circ_daily_change,
      zeph_in_reserve,
      zeph_in_reserve_value,
      zeph_in_reserve_percent,
      zsd_in_yield_reserve,
      zsd_in_yield_reserve_percent,
      zys_current_variable_apy: zysCurrentVariableApy,
      reserve_ratio: Number.isFinite(reserve_ratio_value) ? reserve_ratio_value : 0,
      reserve_ratio_ma,
    };

    // save all these to redis to be called back in case of daemon/explorer api failure
    await redis.set("live_stats", JSON.stringify(liveStats));

    return liveStats;
  } catch (error) {
    console.error("Error fetching live stats:", error);
    console.log(`getLiveStats: Error fetching live stats - using redis data instead`);

    // Fetch from Redis as a fallback
    const liveStatsString = await redis.get("live_stats");
    const liveStats = liveStatsString ? JSON.parse(liveStatsString) : null;

    // Handle case when Redis might return null or empty object
    if (!liveStats || Object.keys(liveStats).length === 0) {
      console.error("Failed to retrieve live stats from both API and Redis.");
      return;
    }

    return liveStats;
  }
}

// Example usage

// (async () => {
//   const height = await getCurrentBlockHeight();
//   console.log("Current Block Height:", height);
// })();
