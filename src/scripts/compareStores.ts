/* eslint-disable no-console */
process.env.DATA_STORE = process.env.DATA_STORE ?? "hybrid";

import dotenv from "dotenv";
import { randomInt } from "node:crypto";

import redis from "../redis";
import { stores } from "../storage/factory";
import { fetchBlockProtocolStats } from "../db/protocolStats";
import { queryTransactions } from "../db/transactions";
import { getTotals as getSqlTotals } from "../db/totals";
import { getReserveSnapshotByPreviousHeight } from "../db/reserveSnapshots";
import { fetchZysPriceHistory } from "../db/yieldAnalytics";
import {
  mapDbTransaction,
  sanitizeTransactionRecord,
  type ProtocolStats,
  type ReserveSnapshot,
  type TransactionRecord,
} from "../utils";
import type { PricingRecordResult } from "../storage/types";
import type { TotalsRecord } from "../db/totals";
import type { ZysPriceHistoryEntry } from "../pr";

dotenv.config();

interface PricingRedisRecord {
  height: number;
  timestamp: number;
  spot: number;
  moving_average: number;
  reserve: number;
  reserve_ma: number;
  stable: number;
  stable_ma: number;
  yield_price: number;
}

type TotalsHashShape = Record<string, string | undefined>;

interface TotalsFieldMapping {
  redisKey: keyof TotalsHashShape;
  sqlKey: keyof TotalsRecord;
}

const PRICING_SAMPLES = Math.max(Number(process.env.COMPARE_PRICING_SAMPLES ?? "5"), 1);
const PROTOCOL_SAMPLES = Math.max(Number(process.env.COMPARE_PROTOCOL_SAMPLES ?? "5"), 1);
const TRANSACTION_SAMPLES = Math.max(Number(process.env.COMPARE_TRANSACTION_SAMPLES ?? "20"), 1);
const SNAPSHOT_SAMPLES = Math.max(Number(process.env.COMPARE_SNAPSHOT_SAMPLES ?? "3"), 1);
const ZYS_SAMPLES = Math.max(Number(process.env.COMPARE_ZYS_SAMPLES ?? "10"), 1);
const PRICING_MIN_HEIGHT = Number(process.env.COMPARE_PRICING_MIN_HEIGHT ?? "100000");
const PRICING_MAX_HEIGHT = Number(process.env.COMPARE_PRICING_MAX_HEIGHT ?? "600000");
const PROTOCOL_LOOKBACK = Math.max(Number(process.env.COMPARE_PROTOCOL_LOOKBACK ?? "2000"), 1);
const PROTOCOL_MIN_HEIGHT = Number(process.env.COMPARE_PROTOCOL_MIN_HEIGHT ?? "100000");
const SNAPSHOT_HASH_KEY = process.env.RESERVE_SNAPSHOT_REDIS_KEY ?? "reserve_snapshots";
const ZYS_HISTORY_KEY = "zys_price_history";
const PROTOCOL_RATIO_TOLERANCE = 1e-4;
const TRANSACTION_TOLERANCE = 1e-9;
const PRICING_TOLERANCE = 1e-9;

const totalsFieldMap: TotalsFieldMapping[] = [
  { redisKey: "conversion_transactions", sqlKey: "conversionTransactions" },
  { redisKey: "yield_conversion_transactions", sqlKey: "yieldConversionTransactions" },
  { redisKey: "mint_reserve_count", sqlKey: "mintReserveCount" },
  { redisKey: "mint_reserve_volume", sqlKey: "mintReserveVolume" },
  { redisKey: "fees_zephrsv", sqlKey: "feesZephrsv" },
  { redisKey: "redeem_reserve_count", sqlKey: "redeemReserveCount" },
  { redisKey: "redeem_reserve_volume", sqlKey: "redeemReserveVolume" },
  { redisKey: "fees_zephusd", sqlKey: "feesZephusd" },
  { redisKey: "mint_stable_count", sqlKey: "mintStableCount" },
  { redisKey: "mint_stable_volume", sqlKey: "mintStableVolume" },
  { redisKey: "redeem_stable_count", sqlKey: "redeemStableCount" },
  { redisKey: "redeem_stable_volume", sqlKey: "redeemStableVolume" },
  { redisKey: "fees_zeph", sqlKey: "feesZeph" },
  { redisKey: "mint_yield_count", sqlKey: "mintYieldCount" },
  { redisKey: "mint_yield_volume", sqlKey: "mintYieldVolume" },
  { redisKey: "fees_zyield", sqlKey: "feesZyield" },
  { redisKey: "redeem_yield_count", sqlKey: "redeemYieldCount" },
  { redisKey: "redeem_yield_volume", sqlKey: "redeemYieldVolume" },
  { redisKey: "fees_zephusd_yield", sqlKey: "feesZephusdYield" },
  { redisKey: "miner_reward", sqlKey: "minerReward" },
  { redisKey: "governance_reward", sqlKey: "governanceReward" },
  { redisKey: "reserve_reward", sqlKey: "reserveReward" },
  { redisKey: "yield_reward", sqlKey: "yieldReward" },
];

interface ComparisonResult {
  label: string;
  issues: string[];
}

function sampleRange(count: number, min: number, maxExclusive: number): number[] {
  const safeMin = Number.isFinite(min) ? Math.floor(min) : 0;
  const safeMax = Number.isFinite(maxExclusive) ? Math.floor(maxExclusive) : safeMin + 1;
  if (safeMax <= safeMin) {
    return [safeMin];
  }
  const picks = new Set<number>();
  while (picks.size < count) {
    picks.add(randomInt(safeMin, safeMax));
  }
  return Array.from(picks);
}

function parseJsonOrIssue<T>(value: string, label: string, issues: string[]): T | null {
  try {
    return JSON.parse(value) as T;
  } catch (error) {
    issues.push(`${label}: unable to parse redis payload (${(error as Error).message})`);
    return null;
  }
}

function normalizeNumber(value: unknown): number | null {
  if (typeof value === "number") {
    if (Number.isNaN(value)) {
      return null;
    }
    return value;
  }
  if (typeof value === "string" && value.trim().length > 0) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function compareNumericField(label: string, redisValue: unknown, sqlValue: unknown, tolerance = 1e-6): string | null {
  const redisNumber = normalizeNumber(redisValue);
  const sqlNumber = normalizeNumber(sqlValue);
  if (redisNumber === null && sqlNumber === null) {
    return null;
  }
  if (redisNumber === null || sqlNumber === null) {
    return `${label}: redis=${redisNumber ?? "null"} postgres=${sqlNumber ?? "null"}`;
  }
  if (Math.abs(redisNumber - sqlNumber) > tolerance) {
    return `${label}: redis=${redisNumber} postgres=${sqlNumber}`;
  }
  return null;
}

function compareStringField(label: string, redisValue: unknown, sqlValue: unknown): string | null {
  const normalizedRedis = redisValue ?? null;
  const normalizedSql = sqlValue ?? null;
  if (normalizedRedis === normalizedSql) {
    return null;
  }
  return `${label}: redis=${normalizedRedis ?? "null"} postgres=${normalizedSql ?? "null"}`;
}

function normalizeJson(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(normalizeJson);
  }
  if (value && typeof value === "object") {
    const entries = Object.entries(value as Record<string, unknown>).sort(([a], [b]) => a.localeCompare(b));
    const normalized: Record<string, unknown> = {};
    for (const [key, entryValue] of entries) {
      normalized[key] = normalizeJson(entryValue ?? null);
    }
    return normalized;
  }
  if (value === undefined) {
    return null;
  }
  return value;
}

function deepEqualNormalized(a: unknown, b: unknown): boolean {
  return JSON.stringify(normalizeJson(a)) === JSON.stringify(normalizeJson(b));
}

function shuffleInPlace<T>(items: T[]): void {
  for (let i = items.length - 1; i > 0; i--) {
    const j = randomInt(0, i + 1);
    [items[i], items[j]] = [items[j], items[i]];
  }
}

function pickRandomElements<T>(items: T[], count: number): T[] {
  if (items.length <= count) {
    return items.slice();
  }
  const copy = items.slice();
  shuffleInPlace(copy);
  return copy.slice(0, count);
}

async function sampleHashFields(hashKey: string, desired: number): Promise<string[]> {
  const captured = new Set<string>();
  let cursor = "0";
  const countHint = Math.max(desired * 20, 100);
  let guard = 0;

  do {
    const [nextCursor, chunk] = await redis.hscan(hashKey, cursor, "COUNT", countHint);
    cursor = nextCursor;
    const entries = chunk as string[];
    for (let i = 0; i < entries.length; i += 2) {
      captured.add(entries[i]);
      if (captured.size >= desired * 3) {
        break;
      }
    }
    guard++;
    if (cursor === "0" || captured.size >= desired * 3 || guard > 1000) {
      break;
    }
  } while (true);

  const fields = Array.from(captured);
  if (fields.length <= desired) {
    return fields;
  }
  shuffleInPlace(fields);
  return fields.slice(0, desired);
}

async function fetchRedisZysHistory(): Promise<ZysPriceHistoryEntry[]> {
  const rows = await redis.zrangebyscore(ZYS_HISTORY_KEY, "-inf", "+inf", "WITHSCORES");
  const entries: ZysPriceHistoryEntry[] = [];
  for (let i = 0; i < rows.length; i += 2) {
    try {
      const payload = JSON.parse(rows[i]) as { block_height: number; zys_price: number };
      const timestamp = Number(rows[i + 1]);
      entries.push({
        timestamp,
        block_height: payload.block_height,
        zys_price: payload.zys_price,
      });
    } catch (error) {
      console.warn(`[compare] Failed to parse zys history entry: ${(error as Error).message}`);
    }
  }
  return entries;
}

async function getLatestAggregatorHeight(): Promise<number> {
  const raw = await redis.get("height_aggregator");
  const parsed = raw ? Number(raw) : NaN;
  if (Number.isFinite(parsed)) {
    return parsed;
  }
  return PRICING_MAX_HEIGHT;
}

async function comparePricingRecords(): Promise<ComparisonResult> {
  const issues: string[] = [];
  const latestHeight = await stores.pricing.getLatestHeight();
  const fallbackMax = Math.max(PRICING_MAX_HEIGHT, PRICING_MIN_HEIGHT + 1);
  const maxExclusive = Math.max(latestHeight + 1, fallbackMax);
  const minHeight = Math.max(Math.min(PRICING_MIN_HEIGHT, maxExclusive - 1), 0);
  const heights = sampleRange(PRICING_SAMPLES, minHeight, maxExclusive);

  const fieldMap: Array<{
    label: string;
    redisKey: keyof PricingRedisRecord;
    sql: (record: PricingRecordResult) => number;
  }> = [
    { label: "timestamp", redisKey: "timestamp", sql: (record) => record.timestamp },
    { label: "spot", redisKey: "spot", sql: (record) => record.spot },
    { label: "moving_average", redisKey: "moving_average", sql: (record) => record.movingAverage },
    { label: "reserve", redisKey: "reserve", sql: (record) => record.reserve },
    { label: "reserve_ma", redisKey: "reserve_ma", sql: (record) => record.reserveMa },
    { label: "stable", redisKey: "stable", sql: (record) => record.stable },
    { label: "stable_ma", redisKey: "stable_ma", sql: (record) => record.stableMa },
    { label: "yield_price", redisKey: "yield_price", sql: (record) => record.yieldPrice },
  ];

  for (const height of heights) {
    const [redisValue, sqlRecord] = await Promise.all([
      redis.hget("pricing_records", height.toString()),
      stores.pricing.get(height),
    ]);

    if (!redisValue) {
      issues.push(`pricing ${height}: missing in redis`);
      continue;
    }
    if (!sqlRecord) {
      issues.push(`pricing ${height}: missing in postgres`);
      continue;
    }

    const redisRecord = parseJsonOrIssue<PricingRedisRecord>(redisValue, `pricing ${height}`, issues);
    if (!redisRecord) {
      continue;
    }

    if (redisRecord.height !== sqlRecord.blockHeight) {
      issues.push(`pricing ${height}: height mismatch redis=${redisRecord.height} postgres=${sqlRecord.blockHeight}`);
    }

    for (const field of fieldMap) {
      const diff = compareNumericField(
        `pricing ${height}.${field.label}`,
        redisRecord[field.redisKey],
        field.sql(sqlRecord),
        field.label === "timestamp" ? 0 : PRICING_TOLERANCE
      );
      if (diff) {
        issues.push(diff);
      }
    }
  }

  return { label: "pricing_records", issues };
}

async function compareProtocolStats(): Promise<ComparisonResult> {
  const issues: string[] = [];
  const latestHeight = await getLatestAggregatorHeight();
  const maxHeight = Math.max(latestHeight, PROTOCOL_MIN_HEIGHT + 1);
  const minHeight = Math.max(maxHeight - PROTOCOL_LOOKBACK, PROTOCOL_MIN_HEIGHT);
  const heights = sampleRange(PROTOCOL_SAMPLES, minHeight, maxHeight + 1);

  const numericFields: Array<keyof ProtocolStats> = [
    "block_height",
    "block_timestamp",
    "spot",
    "moving_average",
    "reserve",
    "reserve_ma",
    "stable",
    "stable_ma",
    "yield_price",
    "zeph_in_reserve",
    "zsd_in_yield_reserve",
    "zeph_circ",
    "zephusd_circ",
    "zephrsv_circ",
    "zyield_circ",
    "assets",
    "assets_ma",
    "liabilities",
    "equity",
    "equity_ma",
    "zsd_accrued_in_yield_reserve_from_yield_reward",
    "zsd_minted_for_yield",
    "conversion_transactions_count",
    "yield_conversion_transactions_count",
    "mint_reserve_count",
    "mint_reserve_volume",
    "fees_zephrsv",
    "redeem_reserve_count",
    "redeem_reserve_volume",
    "fees_zephusd",
    "mint_stable_count",
    "mint_stable_volume",
    "redeem_stable_count",
    "redeem_stable_volume",
    "fees_zeph",
    "mint_yield_count",
    "mint_yield_volume",
    "redeem_yield_count",
    "redeem_yield_volume",
    "fees_zephusd_yield",
    "fees_zyield",
  ];
  const ratioFields: Array<keyof ProtocolStats> = ["reserve_ratio", "reserve_ratio_ma"];
  const stringFields: Array<keyof ProtocolStats> = ["zeph_in_reserve_atoms"];

  for (const height of heights) {
    const [redisValue, sqlRows] = await Promise.all([
      redis.hget("protocol_stats", height.toString()),
      fetchBlockProtocolStats(height, height),
    ]);
    const sqlRecord = sqlRows[0];

    if (!redisValue) {
      issues.push(`protocol_stats ${height}: missing in redis`);
      continue;
    }
    if (!sqlRecord) {
      issues.push(`protocol_stats ${height}: missing in postgres`);
      continue;
    }

    const redisRecord = parseJsonOrIssue<ProtocolStats>(redisValue, `protocol_stats ${height}`, issues);
    if (!redisRecord) {
      continue;
    }

    for (const field of numericFields) {
      const diff = compareNumericField(`protocol_stats ${height}.${field}`, redisRecord[field], sqlRecord[field]);
      if (diff) {
        issues.push(diff);
      }
    }

    for (const field of ratioFields) {
      const diff = compareNumericField(
        `protocol_stats ${height}.${field}`,
        redisRecord[field],
        sqlRecord[field],
        PROTOCOL_RATIO_TOLERANCE
      );
      if (diff) {
        issues.push(diff);
      }
    }

    for (const field of stringFields) {
      const diff = compareStringField(`protocol_stats ${height}.${field}`, redisRecord[field], sqlRecord[field]);
      if (diff) {
        issues.push(diff);
      }
    }
  }

  return { label: "protocol_stats", issues };
}

async function compareTransactions(): Promise<ComparisonResult> {
  const issues: string[] = [];
  const baseResult = await queryTransactions({ limit: 1, order: "desc" });
  const total = baseResult.total;
  const redisCount = await redis.hlen("txs");

  if (typeof redisCount === "number" && redisCount !== total) {
    issues.push(`transactions total mismatch: redis=${redisCount} postgres=${total}`);
  }

  if (total === 0) {
    return { label: "transactions", issues };
  }

  const sampleSize = Math.min(TRANSACTION_SAMPLES, total);
  const maxOffset = Math.max(total - sampleSize, 0);
  const offset = maxOffset > 0 ? randomInt(0, maxOffset + 1) : 0;
  const sampleResult = await queryTransactions({ limit: sampleSize, offset, order: "desc" });
  const sqlTransactions = sampleResult.results.map(mapDbTransaction);

  const numericFields: Array<keyof TransactionRecord> = [
    "block_height",
    "block_timestamp",
    "conversion_rate",
    "from_amount",
    "to_amount",
    "conversion_fee_amount",
    "tx_fee_amount",
  ];
  const stringFields: Array<keyof TransactionRecord> = [
    "conversion_type",
    "from_asset",
    "from_amount_atoms",
    "to_asset",
    "to_amount_atoms",
    "conversion_fee_asset",
    "tx_fee_asset",
    "tx_fee_atoms",
  ];

  for (const tx of sqlTransactions) {
    const redisRaw = await redis.hget("txs", tx.hash);
    if (!redisRaw) {
      issues.push(`transaction ${tx.hash}: missing in redis`);
      continue;
    }
    const parsed = parseJsonOrIssue<unknown>(redisRaw, `transaction ${tx.hash}`, issues);
    if (!parsed) {
      continue;
    }
    const redisRecord = sanitizeTransactionRecord(parsed);
    if (!redisRecord) {
      issues.push(`transaction ${tx.hash}: invalid redis payload`);
      continue;
    }

    for (const field of numericFields) {
      const diff = compareNumericField(
        `transaction ${tx.hash}.${field}`,
        redisRecord[field],
        tx[field],
        TRANSACTION_TOLERANCE
      );
      if (diff) {
        issues.push(diff);
      }
    }

    for (const field of stringFields) {
      const diff = compareStringField(`transaction ${tx.hash}.${field}`, redisRecord[field], tx[field]);
      if (diff) {
        issues.push(diff);
      }
    }
  }

  return { label: "transactions", issues };
}

async function compareTotals(): Promise<ComparisonResult> {
  const issues: string[] = [];
  const redisTotals = (await redis.hgetall("totals")) as TotalsHashShape;
  const sqlTotals = await getSqlTotals();

  if (!redisTotals || Object.keys(redisTotals).length === 0) {
    issues.push("totals: missing in redis");
  }
  if (!sqlTotals) {
    issues.push("totals: missing in postgres");
    return { label: "totals", issues };
  }

  for (const field of totalsFieldMap) {
    const diff = compareNumericField(
      `totals.${field.redisKey}`,
      redisTotals?.[field.redisKey],
      sqlTotals[field.sqlKey],
      PROTOCOL_RATIO_TOLERANCE
    );
    if (diff) {
      issues.push(diff);
    }
  }

  return { label: "totals", issues };
}

async function compareSnapshots(): Promise<ComparisonResult> {
  const issues: string[] = [];
  const keys = await sampleHashFields(SNAPSHOT_HASH_KEY, SNAPSHOT_SAMPLES);

  if (keys.length === 0) {
    issues.push("reserve_snapshots: no redis entries found");
    return { label: "reserve_snapshots", issues };
  }

  for (const key of keys) {
    const redisValue = await redis.hget(SNAPSHOT_HASH_KEY, key);
    if (!redisValue) {
      issues.push(`reserve_snapshots ${key}: missing payload in redis`);
      continue;
    }
    const redisSnapshot = parseJsonOrIssue<ReserveSnapshot>(redisValue, `reserve_snapshots ${key}`, issues);
    if (!redisSnapshot) {
      continue;
    }
    const previousHeight = Number(redisSnapshot.previous_height ?? Number(key));
    const sqlSnapshot = await getReserveSnapshotByPreviousHeight(previousHeight);
    if (!sqlSnapshot) {
      issues.push(`reserve_snapshots ${key}: missing in postgres`);
      continue;
    }

    const topLevel = [
      ["reserve_height", redisSnapshot.reserve_height, sqlSnapshot.reserve_height],
      ["previous_height", redisSnapshot.previous_height, sqlSnapshot.previous_height],
      ["hf_version", redisSnapshot.hf_version, sqlSnapshot.hf_version],
    ] as const;

    for (const [label, redisValueTop, sqlValueTop] of topLevel) {
      const diff = compareNumericField(`reserve_snapshots ${key}.${label}`, redisValueTop, sqlValueTop);
      if (diff) {
        issues.push(diff);
      }
    }

    const timestampDiff = compareStringField(
      `reserve_snapshots ${key}.captured_at`,
      redisSnapshot.captured_at,
      sqlSnapshot.captured_at
    );
    if (timestampDiff) {
      issues.push(timestampDiff);
    }

    const redisOnChain = redisSnapshot.on_chain;
    const sqlOnChain = sqlSnapshot.on_chain;

    if (!redisOnChain || !sqlOnChain) {
      issues.push(`reserve_snapshots ${key}: missing on_chain payload`);
    } else {
      const numberFields = [
        "zeph_reserve",
        "zsd_circ",
        "zrs_circ",
        "zyield_circ",
        "zsd_yield_reserve",
        "reserve_ratio",
        "reserve_ratio_ma",
      ] as const;
      const stringFields = [
        "zeph_reserve_atoms",
        "zsd_circ_atoms",
        "zrs_circ_atoms",
        "zyield_circ_atoms",
        "zsd_yield_reserve_atoms",
        "reserve_ratio_atoms",
        "reserve_ratio_ma_atoms",
      ] as const;

      for (const field of numberFields) {
        const diff = compareNumericField(
          `reserve_snapshots ${key}.on_chain.${field}`,
          redisOnChain[field],
          sqlOnChain[field]
        );
        if (diff) {
          issues.push(diff);
        }
      }

      for (const field of stringFields) {
        const diff = compareStringField(
          `reserve_snapshots ${key}.on_chain.${field}`,
          redisOnChain[field],
          sqlOnChain[field]
        );
        if (diff) {
          issues.push(diff);
        }
      }
    }

    if (!deepEqualNormalized(redisSnapshot.pricing_record, sqlSnapshot.pricing_record)) {
      issues.push(`reserve_snapshots ${key}.pricing_record: mismatch`);
    }
    if (!deepEqualNormalized(redisSnapshot.raw, sqlSnapshot.raw)) {
      issues.push(`reserve_snapshots ${key}.raw: mismatch`);
    }
  }

  return { label: "reserve_snapshots", issues };
}

async function compareZysPriceHistory(): Promise<ComparisonResult> {
  const issues: string[] = [];
  const [redisHistory, sqlHistory] = await Promise.all([fetchRedisZysHistory(), fetchZysPriceHistory()]);

  if (redisHistory.length !== sqlHistory.length) {
    issues.push(`zys_price_history length mismatch: redis=${redisHistory.length} postgres=${sqlHistory.length}`);
  }

  if (redisHistory.length === 0 || sqlHistory.length === 0) {
    return { label: "zys_price_history", issues };
  }

  const redisMap = new Map<string, ZysPriceHistoryEntry>();
  for (const entry of redisHistory) {
    redisMap.set(`${entry.timestamp}:${entry.block_height}`, entry);
  }

  const samples = pickRandomElements(sqlHistory, Math.min(ZYS_SAMPLES, sqlHistory.length));

  for (const entry of samples) {
    const key = `${entry.timestamp}:${entry.block_height}`;
    const redisEntry = redisMap.get(key);
    if (!redisEntry) {
      issues.push(`zys_price_history ${key}: missing in redis`);
      continue;
    }
    const timestampDiff = compareNumericField(
      `zys_price_history ${key}.timestamp`,
      redisEntry.timestamp,
      entry.timestamp,
      0
    );
    if (timestampDiff) {
      issues.push(timestampDiff);
    }
    const heightDiff = compareNumericField(
      `zys_price_history ${key}.block_height`,
      redisEntry.block_height,
      entry.block_height,
      0
    );
    if (heightDiff) {
      issues.push(heightDiff);
    }
    const priceDiff = compareNumericField(
      `zys_price_history ${key}.zys_price`,
      redisEntry.zys_price,
      entry.zys_price,
      PROTOCOL_RATIO_TOLERANCE
    );
    if (priceDiff) {
      issues.push(priceDiff);
    }
  }

  return { label: "zys_price_history", issues };
}

async function main() {
  const comparisons = await Promise.all([
    comparePricingRecords(),
    compareProtocolStats(),
    compareTransactions(),
    compareTotals(),
    compareSnapshots(),
    compareZysPriceHistory(),
  ]);

  console.log("=== Redis vs Postgres Comparison ===");
  for (const { label, issues } of comparisons) {
    if (issues.length === 0) {
      console.log(`✔ ${label} OK`);
    } else {
      console.log(`✖ ${label}`);
      for (const issue of issues) {
        console.log(`  - ${issue}`);
      }
    }
  }

  await redis.quit();
}

main().catch((error) => {
  console.error("[compare] failed", error);
  process.exit(1);
});

