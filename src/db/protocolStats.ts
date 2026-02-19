import { Prisma, ProtocolStatsBlock as ProtocolStatsBlockModel } from "@prisma/client";

import { getPrismaClient } from "./index";
import { AggregatedData, ProtocolStats } from "../utils";

function finiteOrNull(value: number | null | undefined): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function toBlockStatsInput(stats: ProtocolStats): Prisma.ProtocolStatsBlockUpsertArgs["create"] {
  const reserveRatio = finiteOrNull(stats.reserve_ratio ?? null);
  const reserveRatioMa = finiteOrNull(stats.reserve_ratio_ma ?? null);
  return {
    blockHeight: stats.block_height,
    blockTimestamp: stats.block_timestamp,
    spot: stats.spot,
    movingAverage: stats.moving_average,
    reserve: stats.reserve,
    reserveMa: stats.reserve_ma,
    stable: stats.stable,
    stableMa: stats.stable_ma,
    yieldPrice: stats.yield_price,
    zephInReserve: stats.zeph_in_reserve,
    zephInReserveAtoms: stats.zeph_in_reserve_atoms,
    zsdInYieldReserve: stats.zsd_in_yield_reserve,
    zephCirc: stats.zeph_circ,
    zephusdCirc: stats.zephusd_circ,
    zephrsvCirc: stats.zephrsv_circ,
    zyieldCirc: stats.zyield_circ,
    assets: stats.assets,
    assetsMa: stats.assets_ma,
    liabilities: stats.liabilities,
    equity: stats.equity,
    equityMa: stats.equity_ma,
    reserveRatio: reserveRatio ?? undefined,
    reserveRatioMa: reserveRatioMa ?? undefined,
    zsdAccruedInYieldReserve: stats.zsd_accrued_in_yield_reserve_from_yield_reward,
    zsdMintedForYield: stats.zsd_minted_for_yield,
    conversionTransactionsCount: stats.conversion_transactions_count,
    yieldConversionTransactionsCount: stats.yield_conversion_transactions_count,
    mintReserveCount: stats.mint_reserve_count,
    mintReserveVolume: stats.mint_reserve_volume,
    feesZephrsv: stats.fees_zephrsv,
    redeemReserveCount: stats.redeem_reserve_count,
    redeemReserveVolume: stats.redeem_reserve_volume,
    feesZephusd: stats.fees_zephusd,
    mintStableCount: stats.mint_stable_count,
    mintStableVolume: stats.mint_stable_volume,
    redeemStableCount: stats.redeem_stable_count,
    redeemStableVolume: stats.redeem_stable_volume,
    feesZeph: stats.fees_zeph,
    mintYieldCount: stats.mint_yield_count,
    mintYieldVolume: stats.mint_yield_volume,
    redeemYieldCount: stats.redeem_yield_count,
    redeemYieldVolume: stats.redeem_yield_volume,
    feesZephusdYield: stats.fees_zephusd_yield,
    feesZyield: stats.fees_zyield,
  };
}

function mapBlockRow(row: ProtocolStatsBlockModel): ProtocolStats {
  return {
    block_height: row.blockHeight,
    block_timestamp: row.blockTimestamp,
    spot: row.spot,
    moving_average: row.movingAverage,
    reserve: row.reserve,
    reserve_ma: row.reserveMa,
    stable: row.stable,
    stable_ma: row.stableMa,
    yield_price: row.yieldPrice,
    zeph_in_reserve: row.zephInReserve,
    zeph_in_reserve_atoms: row.zephInReserveAtoms ?? undefined,
    zsd_in_yield_reserve: row.zsdInYieldReserve,
    zeph_circ: row.zephCirc,
    zephusd_circ: row.zephusdCirc,
    zephrsv_circ: row.zephrsvCirc,
    zyield_circ: row.zyieldCirc,
    assets: row.assets,
    assets_ma: row.assetsMa,
    liabilities: row.liabilities,
    equity: row.equity,
    equity_ma: row.equityMa,
    reserve_ratio: row.reserveRatio ?? null,
    reserve_ratio_ma: row.reserveRatioMa ?? null,
    zsd_accrued_in_yield_reserve_from_yield_reward: row.zsdAccruedInYieldReserve,
    zsd_minted_for_yield: row.zsdMintedForYield,
    conversion_transactions_count: row.conversionTransactionsCount,
    yield_conversion_transactions_count: row.yieldConversionTransactionsCount,
    mint_reserve_count: row.mintReserveCount,
    mint_reserve_volume: row.mintReserveVolume,
    fees_zephrsv: row.feesZephrsv,
    redeem_reserve_count: row.redeemReserveCount,
    redeem_reserve_volume: row.redeemReserveVolume,
    fees_zephusd: row.feesZephusd,
    mint_stable_count: row.mintStableCount,
    mint_stable_volume: row.mintStableVolume,
    redeem_stable_count: row.redeemStableCount,
    redeem_stable_volume: row.redeemStableVolume,
    fees_zeph: row.feesZeph,
    mint_yield_count: row.mintYieldCount,
    mint_yield_volume: row.mintYieldVolume,
    redeem_yield_count: row.redeemYieldCount,
    redeem_yield_volume: row.redeemYieldVolume,
    fees_zephusd_yield: row.feesZephusdYield,
    fees_zyield: row.feesZyield,
  };
}

function toAggregatedInput(
  windowStart: number,
  windowEnd: number | undefined,
  data: AggregatedData,
  pending: boolean
): Prisma.ProtocolStatsHourlyUpsertArgs["create"] {
  return {
    windowStart,
    windowEnd,
    pending,
    spotOpen: data.spot_open,
    spotClose: data.spot_close,
    spotHigh: data.spot_high,
    spotLow: data.spot_low,
    movingAverageOpen: data.moving_average_open,
    movingAverageClose: data.moving_average_close,
    movingAverageHigh: data.moving_average_high,
    movingAverageLow: data.moving_average_low,
    reserveOpen: data.reserve_open,
    reserveClose: data.reserve_close,
    reserveHigh: data.reserve_high,
    reserveLow: data.reserve_low,
    reserveMaOpen: data.reserve_ma_open,
    reserveMaClose: data.reserve_ma_close,
    reserveMaHigh: data.reserve_ma_high,
    reserveMaLow: data.reserve_ma_low,
    stableOpen: data.stable_open,
    stableClose: data.stable_close,
    stableHigh: data.stable_high,
    stableLow: data.stable_low,
    stableMaOpen: data.stable_ma_open,
    stableMaClose: data.stable_ma_close,
    stableMaHigh: data.stable_ma_high,
    stableMaLow: data.stable_ma_low,
    zyieldPriceOpen: data.zyield_price_open,
    zyieldPriceClose: data.zyield_price_close,
    zyieldPriceHigh: data.zyield_price_high,
    zyieldPriceLow: data.zyield_price_low,
    zephInReserveOpen: data.zeph_in_reserve_open,
    zephInReserveClose: data.zeph_in_reserve_close,
    zephInReserveHigh: data.zeph_in_reserve_high,
    zephInReserveLow: data.zeph_in_reserve_low,
    zsdInYieldReserveOpen: data.zsd_in_yield_reserve_open,
    zsdInYieldReserveClose: data.zsd_in_yield_reserve_close,
    zsdInYieldReserveHigh: data.zsd_in_yield_reserve_high,
    zsdInYieldReserveLow: data.zsd_in_yield_reserve_low,
    zephCircOpen: data.zeph_circ_open,
    zephCircClose: data.zeph_circ_close,
    zephCircHigh: data.zeph_circ_high,
    zephCircLow: data.zeph_circ_low,
    zephusdCircOpen: data.zephusd_circ_open,
    zephusdCircClose: data.zephusd_circ_close,
    zephusdCircHigh: data.zephusd_circ_high,
    zephusdCircLow: data.zephusd_circ_low,
    zephrsvCircOpen: data.zephrsv_circ_open,
    zephrsvCircClose: data.zephrsv_circ_close,
    zephrsvCircHigh: data.zephrsv_circ_high,
    zephrsvCircLow: data.zephrsv_circ_low,
    zyieldCircOpen: data.zyield_circ_open,
    zyieldCircClose: data.zyield_circ_close,
    zyieldCircHigh: data.zyield_circ_high,
    zyieldCircLow: data.zyield_circ_low,
    assetsOpen: data.assets_open,
    assetsClose: data.assets_close,
    assetsHigh: data.assets_high,
    assetsLow: data.assets_low,
    assetsMaOpen: data.assets_ma_open,
    assetsMaClose: data.assets_ma_close,
    assetsMaHigh: data.assets_ma_high,
    assetsMaLow: data.assets_ma_low,
    liabilitiesOpen: data.liabilities_open,
    liabilitiesClose: data.liabilities_close,
    liabilitiesHigh: data.liabilities_high,
    liabilitiesLow: data.liabilities_low,
    equityOpen: data.equity_open,
    equityClose: data.equity_close,
    equityHigh: data.equity_high,
    equityLow: data.equity_low,
    equityMaOpen: data.equity_ma_open,
    equityMaClose: data.equity_ma_close,
    equityMaHigh: data.equity_ma_high,
    equityMaLow: data.equity_ma_low,
    reserveRatioOpen: data.reserve_ratio_open,
    reserveRatioClose: data.reserve_ratio_close,
    reserveRatioHigh: data.reserve_ratio_high,
    reserveRatioLow: data.reserve_ratio_low,
    reserveRatioMaOpen: data.reserve_ratio_ma_open,
    reserveRatioMaClose: data.reserve_ratio_ma_close,
    reserveRatioMaHigh: data.reserve_ratio_ma_high,
    reserveRatioMaLow: data.reserve_ratio_ma_low,
    conversionTransactionsCount: data.conversion_transactions_count,
    yieldConversionTransactionsCount: data.yield_conversion_transactions_count,
    mintReserveCount: data.mint_reserve_count,
    mintReserveVolume: data.mint_reserve_volume,
    feesZephrsv: data.fees_zephrsv,
    redeemReserveCount: data.redeem_reserve_count,
    redeemReserveVolume: data.redeem_reserve_volume,
    feesZephusd: data.fees_zephusd,
    mintStableCount: data.mint_stable_count,
    mintStableVolume: data.mint_stable_volume,
    redeemStableCount: data.redeem_stable_count,
    redeemStableVolume: data.redeem_stable_volume,
    feesZeph: data.fees_zeph,
    mintYieldCount: data.mint_yield_count,
    mintYieldVolume: data.mint_yield_volume,
    feesZyield: data.fees_zyield,
    redeemYieldCount: data.redeem_yield_count,
    redeemYieldVolume: data.redeem_yield_volume,
    feesZephusdYield: data.fees_zephusd_yield,
  };
}

export async function saveBlockProtocolStats(stats: ProtocolStats): Promise<void> {
  const prisma = getPrismaClient();
  const data = toBlockStatsInput(stats);
  await prisma.protocolStatsBlock.upsert({
    where: { blockHeight: stats.block_height },
    update: data,
    create: data,
  });
}

export async function saveBlockProtocolStatsBatch(statsBatch: ProtocolStats[]): Promise<void> {
  if (statsBatch.length === 0) return;
  const prisma = getPrismaClient();

  const cols = [
    "block_height", "block_timestamp", "spot", "moving_average", "reserve", "reserve_ma",
    "stable", "stable_ma", "yield_price", "zeph_in_reserve", "zeph_in_reserve_atoms",
    "zsd_in_yield_reserve", "zeph_circ", "zephusd_circ", "zephrsv_circ", "zyield_circ",
    "assets", "assets_ma", "liabilities", "equity", "equity_ma", "reserve_ratio", "reserve_ratio_ma",
    "zsd_accrued_in_yield_reserve_from_yield_reward", "zsd_minted_for_yield",
    "conversion_transactions_count", "yield_conversion_transactions_count",
    "mint_reserve_count", "mint_reserve_volume", "fees_zephrsv",
    "redeem_reserve_count", "redeem_reserve_volume", "fees_zephusd",
    "mint_stable_count", "mint_stable_volume", "redeem_stable_count", "redeem_stable_volume",
    "fees_zeph", "mint_yield_count", "mint_yield_volume", "redeem_yield_count", "redeem_yield_volume",
    "fees_zephusd_yield", "fees_zyield",
  ];
  const colCount = cols.length;

  const placeholders: string[] = [];
  const values: (number | string | null)[] = [];
  let idx = 1;

  for (const s of statsBatch) {
    const rr = finiteOrNull(s.reserve_ratio ?? null);
    const rrma = finiteOrNull(s.reserve_ratio_ma ?? null);
    const phArr: string[] = [];
    for (let c = 0; c < colCount; c++) {
      phArr.push(`$${idx++}`);
    }
    placeholders.push(`(${phArr.join(", ")})`);
    values.push(
      s.block_height, s.block_timestamp, s.spot, s.moving_average, s.reserve, s.reserve_ma,
      s.stable, s.stable_ma, s.yield_price, s.zeph_in_reserve, s.zeph_in_reserve_atoms ?? null,
      s.zsd_in_yield_reserve, s.zeph_circ, s.zephusd_circ, s.zephrsv_circ, s.zyield_circ,
      s.assets, s.assets_ma, s.liabilities, s.equity, s.equity_ma, rr, rrma,
      s.zsd_accrued_in_yield_reserve_from_yield_reward, s.zsd_minted_for_yield,
      s.conversion_transactions_count, s.yield_conversion_transactions_count,
      s.mint_reserve_count, s.mint_reserve_volume, s.fees_zephrsv,
      s.redeem_reserve_count, s.redeem_reserve_volume, s.fees_zephusd,
      s.mint_stable_count, s.mint_stable_volume, s.redeem_stable_count, s.redeem_stable_volume,
      s.fees_zeph, s.mint_yield_count, s.mint_yield_volume, s.redeem_yield_count, s.redeem_yield_volume,
      s.fees_zephusd_yield, s.fees_zyield,
    );
  }

  const updateCols = cols.slice(1).map(c => `${c} = EXCLUDED.${c}`).join(", ");

  const sql = `INSERT INTO protocol_stats (${cols.join(", ")})
VALUES ${placeholders.join(", ")}
ON CONFLICT (block_height) DO UPDATE SET ${updateCols}`;

  await prisma.$executeRawUnsafe(sql, ...values);
}

export async function saveAggregatedProtocolStats(
  scale: "hour" | "day",
  windowStart: number,
  windowEnd: number | undefined,
  data: AggregatedData,
  pending: boolean
): Promise<void> {
  const prisma = getPrismaClient();
  if (scale === "hour") {
    const payload = toAggregatedInput(windowStart, windowEnd, data, pending);
    await prisma.protocolStatsHourly.upsert({
      where: { windowStart },
      update: payload,
      create: payload,
    });
  } else {
    const payload = toAggregatedInput(windowStart, windowEnd, data, pending);
    await prisma.protocolStatsDaily.upsert({
      where: { windowStart },
      update: payload,
      create: payload,
    });
  }
}

interface AggregatedBatchEntry {
  windowStart: number;
  windowEnd: number | undefined;
  data: AggregatedData;
  pending: boolean;
}

export async function saveAggregatedProtocolStatsBatch(
  scale: "hour" | "day",
  entries: AggregatedBatchEntry[]
): Promise<void> {
  if (entries.length === 0) return;
  const prisma = getPrismaClient();
  const tableName = scale === "hour" ? "protocol_stats_hourly" : "protocol_stats_daily";

  const cols = [
    "window_start", "window_end", "pending",
    "spot_open", "spot_close", "spot_high", "spot_low",
    "moving_average_open", "moving_average_close", "moving_average_high", "moving_average_low",
    "reserve_open", "reserve_close", "reserve_high", "reserve_low",
    "reserve_ma_open", "reserve_ma_close", "reserve_ma_high", "reserve_ma_low",
    "stable_open", "stable_close", "stable_high", "stable_low",
    "stable_ma_open", "stable_ma_close", "stable_ma_high", "stable_ma_low",
    "zyield_price_open", "zyield_price_close", "zyield_price_high", "zyield_price_low",
    "zeph_in_reserve_open", "zeph_in_reserve_close", "zeph_in_reserve_high", "zeph_in_reserve_low",
    "zsd_in_yield_reserve_open", "zsd_in_yield_reserve_close", "zsd_in_yield_reserve_high", "zsd_in_yield_reserve_low",
    "zeph_circ_open", "zeph_circ_close", "zeph_circ_high", "zeph_circ_low",
    "zephusd_circ_open", "zephusd_circ_close", "zephusd_circ_high", "zephusd_circ_low",
    "zephrsv_circ_open", "zephrsv_circ_close", "zephrsv_circ_high", "zephrsv_circ_low",
    "zyield_circ_open", "zyield_circ_close", "zyield_circ_high", "zyield_circ_low",
    "assets_open", "assets_close", "assets_high", "assets_low",
    "assets_ma_open", "assets_ma_close", "assets_ma_high", "assets_ma_low",
    "liabilities_open", "liabilities_close", "liabilities_high", "liabilities_low",
    "equity_open", "equity_close", "equity_high", "equity_low",
    "equity_ma_open", "equity_ma_close", "equity_ma_high", "equity_ma_low",
    "reserve_ratio_open", "reserve_ratio_close", "reserve_ratio_high", "reserve_ratio_low",
    "reserve_ratio_ma_open", "reserve_ratio_ma_close", "reserve_ratio_ma_high", "reserve_ratio_ma_low",
    "conversion_transactions_count", "yield_conversion_transactions_count",
    "mint_reserve_count", "mint_reserve_volume", "fees_zephrsv",
    "redeem_reserve_count", "redeem_reserve_volume", "fees_zephusd",
    "mint_stable_count", "mint_stable_volume", "redeem_stable_count", "redeem_stable_volume",
    "fees_zeph", "mint_yield_count", "mint_yield_volume",
    "fees_zyield", "redeem_yield_count", "redeem_yield_volume", "fees_zephusd_yield",
  ];
  const colCount = cols.length;

  const placeholders: string[] = [];
  const values: (number | string | boolean | null)[] = [];
  let idx = 1;

  for (const entry of entries) {
    const d = entry.data;
    const phArr: string[] = [];
    for (let c = 0; c < colCount; c++) {
      phArr.push(`$${idx++}`);
    }
    placeholders.push(`(${phArr.join(", ")})`);
    values.push(
      entry.windowStart, entry.windowEnd ?? null, entry.pending,
      d.spot_open, d.spot_close, d.spot_high, d.spot_low,
      d.moving_average_open, d.moving_average_close, d.moving_average_high, d.moving_average_low,
      d.reserve_open, d.reserve_close, d.reserve_high, d.reserve_low,
      d.reserve_ma_open, d.reserve_ma_close, d.reserve_ma_high, d.reserve_ma_low,
      d.stable_open, d.stable_close, d.stable_high, d.stable_low,
      d.stable_ma_open, d.stable_ma_close, d.stable_ma_high, d.stable_ma_low,
      d.zyield_price_open, d.zyield_price_close, d.zyield_price_high, d.zyield_price_low,
      d.zeph_in_reserve_open, d.zeph_in_reserve_close, d.zeph_in_reserve_high, d.zeph_in_reserve_low,
      d.zsd_in_yield_reserve_open, d.zsd_in_yield_reserve_close, d.zsd_in_yield_reserve_high, d.zsd_in_yield_reserve_low,
      d.zeph_circ_open, d.zeph_circ_close, d.zeph_circ_high, d.zeph_circ_low,
      d.zephusd_circ_open, d.zephusd_circ_close, d.zephusd_circ_high, d.zephusd_circ_low,
      d.zephrsv_circ_open, d.zephrsv_circ_close, d.zephrsv_circ_high, d.zephrsv_circ_low,
      d.zyield_circ_open, d.zyield_circ_close, d.zyield_circ_high, d.zyield_circ_low,
      d.assets_open, d.assets_close, d.assets_high, d.assets_low,
      d.assets_ma_open, d.assets_ma_close, d.assets_ma_high, d.assets_ma_low,
      d.liabilities_open, d.liabilities_close, d.liabilities_high, d.liabilities_low,
      d.equity_open, d.equity_close, d.equity_high, d.equity_low,
      d.equity_ma_open, d.equity_ma_close, d.equity_ma_high, d.equity_ma_low,
      d.reserve_ratio_open, d.reserve_ratio_close, d.reserve_ratio_high, d.reserve_ratio_low,
      d.reserve_ratio_ma_open, d.reserve_ratio_ma_close, d.reserve_ratio_ma_high, d.reserve_ratio_ma_low,
      d.conversion_transactions_count, d.yield_conversion_transactions_count,
      d.mint_reserve_count, d.mint_reserve_volume, d.fees_zephrsv,
      d.redeem_reserve_count, d.redeem_reserve_volume, d.fees_zephusd,
      d.mint_stable_count, d.mint_stable_volume, d.redeem_stable_count, d.redeem_stable_volume,
      d.fees_zeph, d.mint_yield_count, d.mint_yield_volume,
      d.fees_zyield, d.redeem_yield_count, d.redeem_yield_volume, d.fees_zephusd_yield,
    );
  }

  const updateCols = cols.slice(1).map(c => `${c} = EXCLUDED.${c}`).join(", ");

  const sql = `INSERT INTO ${tableName} (${cols.join(", ")})
VALUES ${placeholders.join(", ")}
ON CONFLICT (window_start) DO UPDATE SET ${updateCols}`;

  await prisma.$executeRawUnsafe(sql, ...values);
}

export async function fetchBlockProtocolStats(
  from?: number,
  to?: number
): Promise<ProtocolStats[]> {
  const prisma = getPrismaClient();
  const where: Prisma.ProtocolStatsBlockWhereInput = {};
  const blockHeightFilter: Prisma.IntFilter = {};
  if (from != null) {
    blockHeightFilter.gte = from;
  }
  if (to != null) {
    blockHeightFilter.lte = to;
  }
  if (Object.keys(blockHeightFilter).length > 0) {
    where.blockHeight = blockHeightFilter;
  }

  const rows = await prisma.protocolStatsBlock.findMany({
    where,
    orderBy: { blockHeight: "asc" },
  });

  return rows.map(mapBlockRow);
}

export async function fetchBlockProtocolStatsByTimestampRange(
  startTimestamp?: number,
  endTimestamp?: number
): Promise<ProtocolStats[]> {
  const prisma = getPrismaClient();
  const where: Prisma.ProtocolStatsBlockWhereInput = {};
  const timestampFilter: Prisma.IntFilter = {};
  if (startTimestamp != null) {
    timestampFilter.gte = startTimestamp;
  }
  if (endTimestamp != null) {
    timestampFilter.lt = endTimestamp;
  }
  if (Object.keys(timestampFilter).length > 0) {
    where.blockTimestamp = timestampFilter;
  }
  const rows = await prisma.protocolStatsBlock.findMany({
    where,
    orderBy: { blockTimestamp: "asc" },
  });
  return rows.map(mapBlockRow);
}

export async function getProtocolStatsBlock(blockHeight: number): Promise<ProtocolStats | null> {
  const prisma = getPrismaClient();
  const row = await prisma.protocolStatsBlock.findUnique({
    where: { blockHeight },
  });
  return row ? mapBlockRow(row) : null;
}

export async function fetchLatestProtocolStats(): Promise<ProtocolStats | null> {
  const prisma = getPrismaClient();
  const row = await prisma.protocolStatsBlock.findFirst({
    orderBy: { blockHeight: "desc" },
  });
  return row ? mapBlockRow(row) : null;
}

function mapAggregatedRow(row: any): AggregatedData {
  return {
    spot_open: row.spotOpen,
    spot_close: row.spotClose,
    spot_high: row.spotHigh,
    spot_low: row.spotLow,
    moving_average_open: row.movingAverageOpen,
    moving_average_close: row.movingAverageClose,
    moving_average_high: row.movingAverageHigh,
    moving_average_low: row.movingAverageLow,
    reserve_open: row.reserveOpen,
    reserve_close: row.reserveClose,
    reserve_high: row.reserveHigh,
    reserve_low: row.reserveLow,
    reserve_ma_open: row.reserveMaOpen,
    reserve_ma_close: row.reserveMaClose,
    reserve_ma_high: row.reserveMaHigh,
    reserve_ma_low: row.reserveMaLow,
    stable_open: row.stableOpen,
    stable_close: row.stableClose,
    stable_high: row.stableHigh,
    stable_low: row.stableLow,
    stable_ma_open: row.stableMaOpen,
    stable_ma_close: row.stableMaClose,
    stable_ma_high: row.stableMaHigh,
    stable_ma_low: row.stableMaLow,
    zyield_price_open: row.zyieldPriceOpen,
    zyield_price_close: row.zyieldPriceClose,
    zyield_price_high: row.zyieldPriceHigh,
    zyield_price_low: row.zyieldPriceLow,
    zeph_in_reserve_open: row.zephInReserveOpen,
    zeph_in_reserve_close: row.zephInReserveClose,
    zeph_in_reserve_high: row.zephInReserveHigh,
    zeph_in_reserve_low: row.zephInReserveLow,
    zsd_in_yield_reserve_open: row.zsdInYieldReserveOpen,
    zsd_in_yield_reserve_close: row.zsdInYieldReserveClose,
    zsd_in_yield_reserve_high: row.zsdInYieldReserveHigh,
    zsd_in_yield_reserve_low: row.zsdInYieldReserveLow,
    zeph_circ_open: row.zephCircOpen,
    zeph_circ_close: row.zephCircClose,
    zeph_circ_high: row.zephCircHigh,
    zeph_circ_low: row.zephCircLow,
    zephusd_circ_open: row.zephusdCircOpen,
    zephusd_circ_close: row.zephusdCircClose,
    zephusd_circ_high: row.zephusdCircHigh,
    zephusd_circ_low: row.zephusdCircLow,
    zephrsv_circ_open: row.zephrsvCircOpen,
    zephrsv_circ_close: row.zephrsvCircClose,
    zephrsv_circ_high: row.zephrsvCircHigh,
    zephrsv_circ_low: row.zephrsvCircLow,
    zyield_circ_open: row.zyieldCircOpen,
    zyield_circ_close: row.zyieldCircClose,
    zyield_circ_high: row.zyieldCircHigh,
    zyield_circ_low: row.zyieldCircLow,
    assets_open: row.assetsOpen,
    assets_close: row.assetsClose,
    assets_high: row.assetsHigh,
    assets_low: row.assetsLow,
    assets_ma_open: row.assetsMaOpen,
    assets_ma_close: row.assetsMaClose,
    assets_ma_high: row.assetsMaHigh,
    assets_ma_low: row.assetsMaLow,
    liabilities_open: row.liabilitiesOpen,
    liabilities_close: row.liabilitiesClose,
    liabilities_high: row.liabilitiesHigh,
    liabilities_low: row.liabilitiesLow,
    equity_open: row.equityOpen,
    equity_close: row.equityClose,
    equity_high: row.equityHigh,
    equity_low: row.equityLow,
    equity_ma_open: row.equityMaOpen,
    equity_ma_close: row.equityMaClose,
    equity_ma_high: row.equityMaHigh,
    equity_ma_low: row.equityMaLow,
    reserve_ratio_open: row.reserveRatioOpen,
    reserve_ratio_close: row.reserveRatioClose,
    reserve_ratio_high: row.reserveRatioHigh,
    reserve_ratio_low: row.reserveRatioLow,
    reserve_ratio_ma_open: row.reserveRatioMaOpen,
    reserve_ratio_ma_close: row.reserveRatioMaClose,
    reserve_ratio_ma_high: row.reserveRatioMaHigh,
    reserve_ratio_ma_low: row.reserveRatioMaLow,
    conversion_transactions_count: row.conversionTransactionsCount,
    yield_conversion_transactions_count: row.yieldConversionTransactionsCount,
    mint_reserve_count: row.mintReserveCount,
    mint_reserve_volume: row.mintReserveVolume,
    fees_zephrsv: row.feesZephrsv,
    redeem_reserve_count: row.redeemReserveCount,
    redeem_reserve_volume: row.redeemReserveVolume,
    fees_zephusd: row.feesZephusd,
    mint_stable_count: row.mintStableCount,
    mint_stable_volume: row.mintStableVolume,
    redeem_stable_count: row.redeemStableCount,
    redeem_stable_volume: row.redeemStableVolume,
    fees_zeph: row.feesZeph,
    mint_yield_count: row.mintYieldCount,
    mint_yield_volume: row.mintYieldVolume,
    fees_zyield: row.feesZyield,
    redeem_yield_count: row.redeemYieldCount,
    redeem_yield_volume: row.redeemYieldVolume,
    fees_zephusd_yield: row.feesZephusdYield,
    pending: row.pending ?? undefined,
    window_start: row.windowStart,
    window_end: row.windowEnd ?? undefined,
  };
}

export async function fetchAggregatedProtocolStats(
  scale: "hour" | "day",
  from?: number,
  to?: number
): Promise<Array<{ timestamp: number; data: AggregatedData }>> {
  const prisma = getPrismaClient();
  const windowFilter: Prisma.IntFilter = {};
  if (from != null) {
    windowFilter.gte = from;
  }
  if (to != null) {
    windowFilter.lte = to;
  }
  const hasFilter = Object.keys(windowFilter).length > 0;
  const hourlyWhere: Prisma.ProtocolStatsHourlyWhereInput = hasFilter ? { windowStart: windowFilter } : {};
  const dailyWhere: Prisma.ProtocolStatsDailyWhereInput = hasFilter ? { windowStart: windowFilter } : {};

  if (scale === "hour") {
    const rows = await prisma.protocolStatsHourly.findMany({
      where: hourlyWhere,
      orderBy: { windowStart: "asc" },
    });
    return rows.map((row) => ({
      timestamp: row.windowStart,
      data: mapAggregatedRow(row),
    }));
  }

  const rows = await prisma.protocolStatsDaily.findMany({
    where: dailyWhere,
    orderBy: { windowStart: "asc" },
  });

  return rows.map((row) => ({
    timestamp: row.windowStart,
    data: mapAggregatedRow(row),
  }));
}

export async function deleteBlockStatsAboveHeight(height: number): Promise<void> {
  const prisma = getPrismaClient();
  await prisma.protocolStatsBlock.deleteMany({
    where: {
      blockHeight: {
        gt: height,
      },
    },
  });
}

export async function deleteAggregatesFromTimestamp(scale: "hour" | "day", timestamp: number): Promise<void> {
  const prisma = getPrismaClient();
  if (scale === "hour") {
    await prisma.protocolStatsHourly.deleteMany({
      where: {
        windowStart: {
          gt: timestamp,
        },
      },
    });
  } else {
    await prisma.protocolStatsDaily.deleteMany({
      where: {
        windowStart: {
          gt: timestamp,
        },
      },
    });
  }
}
