// Canonical output schema for the public scanner API.
//
// Internal storage/DB names are legacy (zephusd/zephrsv/zyield, spot/stable/reserve).
// Public API output uses modern names (zsd/zrs/zys, zeph_price/zsd_rate/zrs_rate).
// Input params accept both legacy and canonical (alias → legacy for filtering).
//
// Single source of truth for field/asset/conversion-type renames.

import type {
  ProtocolStats,
  AggregatedData,
  PricingRecord,
  TransactionRecord,
  ReserveSnapshot,
} from "./utils";

// ---------- Canonical types ----------

export interface CanonicalProtocolStats {
  block_height: number;
  block_timestamp: number;
  zeph_price: number;
  zeph_price_ma: number;
  zrs_rate: number;
  zrs_rate_ma: number;
  zsd_rate: number;
  zsd_rate_ma: number;
  zys_price: number;
  zeph_in_reserve: number;
  zeph_in_reserve_atoms?: string;
  zsd_in_yield_reserve: number;
  zeph_circ: number;
  zsd_circ: number;
  zrs_circ: number;
  zys_circ: number;
  assets: number;
  assets_ma: number;
  liabilities: number;
  equity: number;
  equity_ma: number;
  reserve_ratio: number | null;
  reserve_ratio_ma: number | null;
  zsd_accrued_in_yield_reserve_from_yield_reward: number;
  zsd_minted_for_yield: number;
  conversion_transactions_count: number;
  yield_conversion_transactions_count: number;
  mint_zrs_count: number;
  mint_zrs_volume: number;
  fees_zrs: number;
  redeem_zrs_count: number;
  redeem_zrs_volume: number;
  fees_zsd: number;
  mint_zsd_count: number;
  mint_zsd_volume: number;
  redeem_zsd_count: number;
  redeem_zsd_volume: number;
  fees_zeph: number;
  mint_zys_count: number;
  mint_zys_volume: number;
  redeem_zys_count: number;
  redeem_zys_volume: number;
  fees_zsd_yield: number;
  fees_zys: number;
}

export interface CanonicalAggregatedData {
  // Prices
  zeph_price_open: number;
  zeph_price_close: number;
  zeph_price_high: number;
  zeph_price_low: number;
  zeph_price_ma_open: number;
  zeph_price_ma_close: number;
  zeph_price_ma_high: number;
  zeph_price_ma_low: number;
  zrs_rate_open: number;
  zrs_rate_close: number;
  zrs_rate_high: number;
  zrs_rate_low: number;
  zrs_rate_ma_open: number;
  zrs_rate_ma_close: number;
  zrs_rate_ma_high: number;
  zrs_rate_ma_low: number;
  zsd_rate_open: number;
  zsd_rate_close: number;
  zsd_rate_high: number;
  zsd_rate_low: number;
  zsd_rate_ma_open: number;
  zsd_rate_ma_close: number;
  zsd_rate_ma_high: number;
  zsd_rate_ma_low: number;
  zys_price_open: number;
  zys_price_close: number;
  zys_price_high: number;
  zys_price_low: number;
  // Reserves
  zeph_in_reserve_open: number;
  zeph_in_reserve_close: number;
  zeph_in_reserve_high: number;
  zeph_in_reserve_low: number;
  zsd_in_yield_reserve_open: number;
  zsd_in_yield_reserve_close: number;
  zsd_in_yield_reserve_high: number;
  zsd_in_yield_reserve_low: number;
  // Supplies
  zeph_circ_open: number;
  zeph_circ_close: number;
  zeph_circ_high: number;
  zeph_circ_low: number;
  zsd_circ_open: number;
  zsd_circ_close: number;
  zsd_circ_high: number;
  zsd_circ_low: number;
  zrs_circ_open: number;
  zrs_circ_close: number;
  zrs_circ_high: number;
  zrs_circ_low: number;
  zys_circ_open: number;
  zys_circ_close: number;
  zys_circ_high: number;
  zys_circ_low: number;
  // DJED mechanics
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
  // Activity (no OHLC)
  conversion_transactions_count: number;
  yield_conversion_transactions_count: number;
  mint_zrs_count: number;
  mint_zrs_volume: number;
  fees_zrs: number;
  redeem_zrs_count: number;
  redeem_zrs_volume: number;
  fees_zsd: number;
  mint_zsd_count: number;
  mint_zsd_volume: number;
  redeem_zsd_count: number;
  redeem_zsd_volume: number;
  fees_zeph: number;
  mint_zys_count: number;
  mint_zys_volume: number;
  fees_zys: number;
  redeem_zys_count: number;
  redeem_zys_volume: number;
  fees_zsd_yield: number;
  pending?: boolean;
  window_start?: number;
  window_end?: number;
}

export interface CanonicalBlockStatsRow {
  block_height: number;
  data: Partial<CanonicalProtocolStats>;
}

export interface CanonicalAggregatedStatsRow {
  timestamp: number;
  data: Partial<CanonicalAggregatedData>;
}

export interface CanonicalPricingRecord {
  block_height: number;
  timestamp?: number;
  zeph_price?: number;
  zeph_price_ma?: number;
  zrs_rate?: number;
  zrs_rate_ma?: number;
  zsd_rate?: number;
  zsd_rate_ma?: number;
  zys_price?: number;
}

export interface CanonicalTransactionRecord {
  hash: string;
  block_height: number;
  block_timestamp: number;
  conversion_type: string;
  conversion_rate?: number | null;
  from_asset?: string | null;
  from_amount?: number | null;
  from_amount_atoms?: string;
  to_asset?: string | null;
  to_amount?: number | null;
  to_amount_atoms?: string;
  conversion_fee_asset?: string | null;
  conversion_fee_amount?: number | null;
  tx_fee_asset?: string | null;
  tx_fee_amount?: number | null;
  tx_fee_atoms?: string;
}

export interface CanonicalReserveSnapshot {
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
    zys_circ_atoms: string;
    zys_circ: number;
    zsd_yield_reserve_atoms: string;
    zsd_yield_reserve: number;
    reserve_ratio_atoms: string;
    reserve_ratio: number | null;
    reserve_ratio_ma_atoms?: string;
    reserve_ratio_ma?: number | null;
  };
  pricing_record?: ReserveSnapshot["pricing_record"];
  raw?: ReserveSnapshot["raw"];
}

// ---------- Rename tables ----------

const OHLC_SUFFIXES = ["open", "close", "high", "low"] as const;

// Pricing fields (OHLC-expanded in aggregate scale)
const PRICING_RENAMES: Record<string, string> = {
  spot: "zeph_price",
  moving_average: "zeph_price_ma",
  stable: "zsd_rate",
  stable_ma: "zsd_rate_ma",
  reserve: "zrs_rate",
  reserve_ma: "zrs_rate_ma",
};

// Circ-supply fields (OHLC-expanded in aggregate scale)
const CIRC_RENAMES: Record<string, string> = {
  zephusd_circ: "zsd_circ",
  zephrsv_circ: "zrs_circ",
  zyield_circ: "zys_circ",
};

// Conversion activity fields (no OHLC — they are per-interval counts/volumes/fees).
// Applied identically in both block and aggregate scales.
const ACTIVITY_RENAMES: Record<string, string> = {
  fees_zephusd: "fees_zsd",
  fees_zephrsv: "fees_zrs",
  fees_zyield: "fees_zys",
  fees_zephusd_yield: "fees_zsd_yield",
  mint_stable_count: "mint_zsd_count",
  mint_stable_volume: "mint_zsd_volume",
  redeem_stable_count: "redeem_zsd_count",
  redeem_stable_volume: "redeem_zsd_volume",
  mint_reserve_count: "mint_zrs_count",
  mint_reserve_volume: "mint_zrs_volume",
  redeem_reserve_count: "redeem_zrs_count",
  redeem_reserve_volume: "redeem_zrs_volume",
  mint_yield_count: "mint_zys_count",
  mint_yield_volume: "mint_zys_volume",
  redeem_yield_count: "redeem_zys_count",
  redeem_yield_volume: "redeem_zys_volume",
};

function expandOhlc(base: Record<string, string>): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [legacy, canonical] of Object.entries(base)) {
    for (const suffix of OHLC_SUFFIXES) {
      out[`${legacy}_${suffix}`] = `${canonical}_${suffix}`;
    }
  }
  return out;
}

// ProtocolStats (block scale): singular field names, no OHLC.
export const BLOCK_FIELD_RENAMES: Record<string, string> = {
  ...PRICING_RENAMES,
  ...CIRC_RENAMES,
  ...ACTIVITY_RENAMES,
  yield_price: "zys_price",
};

// AggregatedData (hour/day scale): OHLC for pricing/circ/yield_price, activity unchanged.
export const AGGREGATE_FIELD_RENAMES: Record<string, string> = {
  ...expandOhlc(PRICING_RENAMES),
  ...expandOhlc(CIRC_RENAMES),
  ...expandOhlc({ zyield_price: "zys_price" }),
  ...ACTIVITY_RENAMES,
};

// /pricingrecords shape uses `height` not `block_height`; normalize to block_height.
export const PRICING_RECORD_RENAMES: Record<string, string> = {
  height: "block_height",
  spot: "zeph_price",
  moving_average: "zeph_price_ma",
  stable: "zsd_rate",
  stable_ma: "zsd_rate_ma",
  reserve: "zrs_rate",
  reserve_ma: "zrs_rate_ma",
  yield_price: "zys_price",
};

export const ASSET_ALIAS: Record<string, string> = {
  ZEPHUSD: "ZSD",
  ZEPHRSV: "ZRS",
  ZYIELD: "ZYS",
};

export const CONVERSION_TYPE_ALIAS: Record<string, string> = {
  mint_stable: "mint_zsd",
  redeem_stable: "redeem_zsd",
  mint_reserve: "mint_zrs",
  redeem_reserve: "redeem_zrs",
  mint_yield: "mint_zys",
  redeem_yield: "redeem_zys",
};

function reverse(map: Record<string, string>): Record<string, string> {
  return Object.fromEntries(Object.entries(map).map(([k, v]) => [v, k]));
}

const BLOCK_FIELD_REVERSE = reverse(BLOCK_FIELD_RENAMES);
const AGGREGATE_FIELD_REVERSE = reverse(AGGREGATE_FIELD_RENAMES);
const ASSET_REVERSE = reverse(ASSET_ALIAS);
const CONVERSION_TYPE_REVERSE = reverse(CONVERSION_TYPE_ALIAS);

// ---------- Translators (output: legacy → canonical) ----------

function renameKeys<T extends Record<string, unknown>>(
  row: T,
  renames: Record<string, string>
): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(row)) {
    out[renames[key] ?? key] = value;
  }
  return out;
}

export function toCanonicalProtocolStats(
  row: Partial<ProtocolStats>
): Partial<CanonicalProtocolStats> {
  return renameKeys(row as Record<string, unknown>, BLOCK_FIELD_RENAMES) as Partial<CanonicalProtocolStats>;
}

export function toCanonicalAggregated(
  row: Partial<AggregatedData>
): Partial<CanonicalAggregatedData> {
  return renameKeys(row as Record<string, unknown>, AGGREGATE_FIELD_RENAMES) as Partial<CanonicalAggregatedData>;
}

export function toCanonicalBlockStatsRows(
  rows: Array<{ block_height: number; data: Partial<ProtocolStats> }>
): CanonicalBlockStatsRow[] {
  return rows.map((row) => ({
    block_height: row.block_height,
    data: toCanonicalProtocolStats(row.data),
  }));
}

export function toCanonicalAggregatedStatsRows(
  rows: Array<{ timestamp: number; data: Partial<AggregatedData> }>
): CanonicalAggregatedStatsRow[] {
  return rows.map((row) => ({
    timestamp: row.timestamp,
    data: toCanonicalAggregated(row.data),
  }));
}

export function toCanonicalPricing(row: PricingRecord): CanonicalPricingRecord {
  return renameKeys(row as unknown as Record<string, unknown>, PRICING_RECORD_RENAMES) as unknown as CanonicalPricingRecord;
}

export function toCanonicalAsset(asset: string | null | undefined): string | null | undefined {
  if (asset == null) return asset;
  return ASSET_ALIAS[asset] ?? asset;
}

export function toCanonicalConversionType(type: string): string {
  return CONVERSION_TYPE_ALIAS[type] ?? type;
}

export function toCanonicalTx(row: TransactionRecord): CanonicalTransactionRecord {
  return {
    ...row,
    conversion_type: toCanonicalConversionType(row.conversion_type),
    from_asset: toCanonicalAsset(row.from_asset),
    to_asset: toCanonicalAsset(row.to_asset),
    conversion_fee_asset: toCanonicalAsset(row.conversion_fee_asset),
    tx_fee_asset: toCanonicalAsset(row.tx_fee_asset),
  };
}

export function toCanonicalReserveSnapshot(row: ReserveSnapshot): CanonicalReserveSnapshot {
  const { zyield_circ, zyield_circ_atoms, ...rest } = row.on_chain;
  return {
    ...row,
    on_chain: {
      ...rest,
      zys_circ: zyield_circ,
      zys_circ_atoms: zyield_circ_atoms,
    },
  };
}

// ---------- Input resolvers (accept canonical or legacy → legacy internal) ----------

export function resolveInputFieldBlock(name: string): string {
  return BLOCK_FIELD_REVERSE[name] ?? name;
}

export function resolveInputFieldAggregated(name: string): string {
  return AGGREGATE_FIELD_REVERSE[name] ?? name;
}

export function resolveInputAsset(name: string): string {
  return ASSET_REVERSE[name] ?? name;
}

export function resolveInputConversionType(name: string): string {
  return CONVERSION_TYPE_REVERSE[name] ?? name;
}
