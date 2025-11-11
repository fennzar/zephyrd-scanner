import { AggregatedData, ProtocolStats, ReserveDiffReport, ReserveSnapshot } from "./utils";
import { UNAUDITABLE_ZEPH_MINT, INITIAL_TREASURY_ZEPH } from "./constants";

type WindowType = "hourly" | "daily";
type MetricSuffix = "open" | "close" | "high" | "low";

interface MetricConfig {
  label: string;
  prefix: string;
  decimals?: number;
}

function formatNumber(value: number | undefined, decimals: number): string {
  if (value === undefined || !Number.isFinite(value)) {
    return "-";
  }
  return value.toLocaleString("en-US", {
    minimumFractionDigits: 0,
    maximumFractionDigits: decimals,
  });
}

function metricValue(data: AggregatedData, prefix: string, suffix: MetricSuffix): number | undefined {
  const key = `${prefix}_${suffix}` as keyof AggregatedData;
  const value = data[key];
  return typeof value === "number" ? value : undefined;
}

function buildMetricRow(config: MetricConfig, data: AggregatedData) {
  const decimals = config.decimals ?? 4;
  return {
    metric: config.label,
    open: formatNumber(metricValue(data, config.prefix, "open"), decimals),
    close: formatNumber(metricValue(data, config.prefix, "close"), decimals),
    high: formatNumber(metricValue(data, config.prefix, "high"), decimals),
    low: formatNumber(metricValue(data, config.prefix, "low"), decimals),
  };
}

function printTable(windowType: WindowType, title: string, configs: MetricConfig[], data: AggregatedData) {
  if (configs.length === 0) {
    return;
  }
  console.log(`[aggregation-${windowType}] ${title}`);
  console.table(configs.map((cfg) => buildMetricRow(cfg, data)));
}

function formatCounters(windowType: WindowType, data: AggregatedData, windowStart: number) {
  const rows = [
    {
      asset: "ZSD",
      mint_count: formatNumber(data.mint_stable_count, 0),
      mint_amount: formatNumber(data.mint_stable_volume, 2),
      redeem_count: formatNumber(data.redeem_stable_count, 0),
      redeem_amount: formatNumber(data.redeem_stable_volume, 2),
      fees: formatNumber(data.fees_zephusd, 4),
    },
    {
      asset: "ZRS",
      mint_count: formatNumber(data.mint_reserve_count, 0),
      mint_amount: formatNumber(data.mint_reserve_volume, 2),
      redeem_count: formatNumber(data.redeem_reserve_count, 0),
      redeem_amount: formatNumber(data.redeem_reserve_volume, 2),
      fees: formatNumber(data.fees_zephrsv, 4),
    },
    {
      asset: "ZYS",
      mint_count: formatNumber(data.mint_yield_count, 0),
      mint_amount: formatNumber(data.mint_yield_volume, 2),
      redeem_count: formatNumber(data.redeem_yield_count, 0),
      redeem_amount: formatNumber(data.redeem_yield_volume, 2),
      fees: `${formatNumber(data.fees_zyield, 4)} / ${formatNumber(data.fees_zephusd_yield, 4)}`,
    },
  ];

  console.log(`[aggregation-${windowType}] counters & fees`);
  console.table(rows);

  console.log(
    `[aggregation-${windowType}] pending=${data.pending ? "yes" : "no"} | window=[${formatNumber(
      windowStart,
      0
    )} → ${formatNumber(data.window_end, 0)}] | conversions=${formatNumber(
      data.conversion_transactions_count,
      0
    )} | yield_conversions=${formatNumber(data.yield_conversion_transactions_count, 0)}`
  );
}

export function logAggregatedSummary(windowType: WindowType, windowStart: number, data: AggregatedData) {
  console.log(`[aggregation-${windowType}] summary for window ${formatNumber(windowStart, 0)}`);

  printTable(
    windowType,
    "prices",
    [
      { label: "spot", prefix: "spot" },
      { label: "moving_avg", prefix: "moving_average" },
      { label: "reserve", prefix: "reserve" },
      { label: "reserve_ma", prefix: "reserve_ma" },
      { label: "stable", prefix: "stable" },
      { label: "stable_ma", prefix: "stable_ma" },
      { label: "zyield_price", prefix: "zyield_price" },
    ],
    data
  );

  printTable(
    windowType,
    "reserves & circulating",
    [
      { label: "zeph_in_reserve", prefix: "zeph_in_reserve", decimals: 2 },
      { label: "zsd_yield_reserve", prefix: "zsd_in_yield_reserve", decimals: 2 },
      { label: "zeph_circ", prefix: "zeph_circ", decimals: 2 },
      { label: "zsd_circ", prefix: "zephusd_circ", decimals: 2 },
      { label: "zrs_circ", prefix: "zephrsv_circ", decimals: 2 },
      { label: "zys_circ", prefix: "zyield_circ", decimals: 2 },
    ],
    data
  );

  printTable(
    windowType,
    "balances & ratios",
    [
      { label: "assets", prefix: "assets", decimals: 2 },
      { label: "assets_ma", prefix: "assets_ma", decimals: 2 },
      { label: "liabilities", prefix: "liabilities", decimals: 2 },
      { label: "equity", prefix: "equity", decimals: 2 },
      { label: "equity_ma", prefix: "equity_ma", decimals: 2 },
      { label: "reserve_ratio", prefix: "reserve_ratio", decimals: 2 },
      { label: "reserve_ratio_ma", prefix: "reserve_ratio_ma", decimals: 2 },
    ],
    data
  );

  formatCounters(windowType, data, windowStart);
}

export interface TotalsSummary {
  numeric: Record<string, number>;
  other: string[];
}

export function summarizeTotals(totals: Record<string, unknown>): TotalsSummary {
  const numeric: Record<string, number> = {};
  const other: string[] = [];

  for (const [key, value] of Object.entries(totals)) {
    if (typeof value === "number") {
      numeric[key] = value;
      continue;
    }
    if (typeof value === "string") {
      const parsed = Number(value);
      if (Number.isFinite(parsed)) {
        numeric[key] = parsed;
        continue;
      }
      other.push(`${key}: ${value}`);
      continue;
    }
    other.push(`${key}: ${String(value)}`);
  }

  return { numeric, other };
}

export function logTotals(totals: Record<string, unknown>): TotalsSummary {
  const summary = summarizeTotals(totals);
  const numericTotals = summary.numeric;
  const minedTotal =
    (numericTotals.miner_reward ?? 0) +
    (numericTotals.governance_reward ?? 0) +
    (numericTotals.reserve_reward ?? 0) +
    (numericTotals.yield_reward ?? 0);
  const totalAll = minedTotal + INITIAL_TREASURY_ZEPH + UNAUDITABLE_ZEPH_MINT;
  summary.numeric.total_mined = minedTotal;
  summary.numeric.total_all = totalAll;

  const rewardRows = [
    {
      type: "miner",
      amount: formatNumber(numericTotals.miner_reward, 4),
    },
    {
      type: "governance",
      amount: formatNumber(numericTotals.governance_reward, 4),
    },
    {
      type: "reserve",
      amount: formatNumber(numericTotals.reserve_reward, 4),
    },
    {
      type: "yield",
      amount: formatNumber(numericTotals.yield_reward, 4),
    },
    {
      type: "total_mined",
      amount: formatNumber(minedTotal, 4),
    },
    {
      type: "initial_treasury",
      amount: formatNumber(INITIAL_TREASURY_ZEPH, 4),
    },
    {
      type: "unauditable_mint",
      amount: formatNumber(UNAUDITABLE_ZEPH_MINT, 4),
    },
    {
      type: "total_all",
      amount: formatNumber(totalAll, 4),
    },
  ];

  console.log("[totals] rewards");
  console.table(rewardRows);

  const yieldConversions =
    numericTotals.yield_conversion_transactions ?? numericTotals.yield_conversion_transactions_count ?? 0;

  console.log(
    `[totals] conversions=${formatNumber(numericTotals.conversion_transactions, 0)} | yield_conversions=${formatNumber(
      yieldConversions,
      0
    )}`
  );

  const assetRows = [
    {
      asset: "ZSD",
      mint_count: formatNumber(numericTotals.mint_stable_count, 0),
      mint_amount: formatNumber(numericTotals.mint_stable_volume, 2),
      redeem_count: formatNumber(numericTotals.redeem_stable_count, 0),
      redeem_amount: formatNumber(numericTotals.redeem_stable_volume, 2),
      net: formatNumber(
        (numericTotals.mint_stable_volume ?? 0) - (numericTotals.redeem_stable_volume ?? 0),
        2
      ),
      fees: formatNumber(numericTotals.fees_zephusd, 4),
    },
    {
      asset: "ZRS",
      mint_count: formatNumber(numericTotals.mint_reserve_count, 0),
      mint_amount: formatNumber(numericTotals.mint_reserve_volume, 2),
      redeem_count: formatNumber(numericTotals.redeem_reserve_count, 0),
      redeem_amount: formatNumber(numericTotals.redeem_reserve_volume, 2),
      net: formatNumber(
        (numericTotals.mint_reserve_volume ?? 0) - (numericTotals.redeem_reserve_volume ?? 0),
        2
      ),
      fees: formatNumber(numericTotals.fees_zephrsv, 4),
    },
    {
      asset: "ZYS",
      mint_count: formatNumber(numericTotals.mint_yield_count, 0),
      mint_amount: formatNumber(numericTotals.mint_yield_volume, 2),
      redeem_count: formatNumber(numericTotals.redeem_yield_count, 0),
      redeem_amount: formatNumber(numericTotals.redeem_yield_volume, 2),
      net: formatNumber((numericTotals.mint_yield_volume ?? 0) - (numericTotals.redeem_yield_volume ?? 0), 2),
      fees: `${formatNumber(numericTotals.fees_zyield, 4)} / ${formatNumber(numericTotals.fees_zephusd_yield, 4)}`,
    },
  ];

  console.log("[totals] by asset");
  console.table(assetRows);

  if (summary.other.length > 0) {
    console.log("[totals] other:", summary.other.join(", "));
  }

  return summary;
}

export function logReserveHeights(details: { aggregated: number; daemon: number; daemonPrevious: number }) {
  console.log("[reserve] heights");
  console.table([
    {
      aggregated: formatNumber(details.aggregated, 0),
      daemon: formatNumber(details.daemon, 0),
      daemon_previous: formatNumber(details.daemonPrevious, 0),
    },
  ]);
}

export function logReserveSnapshotStatus(status: {
  action: "initial" | "gap-check" | "store" | "skip";
  aggregatedHeight?: number;
  lastSnapshotHeight?: number | null;
  gap?: number;
  required?: number;
  storedPreviousHeight?: number;
}) {
  console.log(`[reserve] snapshot status (${status.action})`);
  const row: Record<string, string> = {};

  if (status.aggregatedHeight !== undefined) {
    row.aggregated = formatNumber(status.aggregatedHeight, 0);
  }
  if (status.lastSnapshotHeight !== undefined) {
    row.last_snapshot = status.lastSnapshotHeight === null ? "-" : formatNumber(status.lastSnapshotHeight, 0);
  }
  if (status.gap !== undefined) {
    row.gap = formatNumber(status.gap, 0);
  }
  if (status.required !== undefined) {
    row.required = formatNumber(status.required, 0);
  }
  if (status.storedPreviousHeight !== undefined) {
    row.stored_previous = formatNumber(status.storedPreviousHeight, 0);
  }

  console.table([row]);
}

export function logReserveDiffReport(report: ReserveDiffReport, tolerance: number): boolean {
  console.log(
    `[reserve] diff source | block=${formatNumber(report.block_height, 0)} | reserve=${formatNumber(
      report.reserve_height ?? 0,
      0
    )} | source=${report.source}`
  );

  const rows = report.diffs.map((entry) => ({
    field: entry.field,
    on_chain: formatNumber(entry.on_chain, 6),
    cached: formatNumber(entry.cached, 6),
    diff: formatNumber(entry.difference, 6),
    diff_atoms: formatNumber(entry.difference_atoms, 0),
  }));
  console.table(rows);

  const zephEntry = report.diffs.find((entry) => entry.field === "zeph_in_reserve");
  const diffValue = Math.abs(zephEntry?.difference ?? 0);
  const passed = diffValue <= tolerance;
  const diffMessage = `[reserve] diff ${passed ? "PASS" : "FAIL"} | |diff|=${formatNumber(diffValue, 6)} | tolerance=${formatNumber(
    tolerance,
    6
  )}`;
  if (passed) {
    console.log(diffMessage);
  } else {
    console.warn(diffMessage);
  }

  return passed;
}

export interface HistoricalReturnRow {
  period: string;
  returnPct: number;
  zsdAccrued: number;
  apy?: number | null;
}

export function logHistoricalReturns(rows: HistoricalReturnRow[]) {
  console.log("[historical] returns");
  console.table(
    rows.map((row) => ({
      period: row.period,
      return_pct: formatNumber(row.returnPct, 4),
      zsd_accrued: formatNumber(row.zsdAccrued, 4),
      effective_apy: row.apy != null ? formatNumber(row.apy, 4) : "-",
    }))
  );
}

export interface ProjectedBaseStats {
  blockHeight: number;
  zephPrice: number;
  zysPrice: number;
  zsdCirc: number;
  zysCirc: number;
  zsdReserve: number;
  reserveRatio: number;
  fallbackPricing: boolean;
}

export function logProjectedBaseStats(stats: ProjectedBaseStats) {
  console.log("[projected] base stats");
  console.table([
    {
      block_height: formatNumber(stats.blockHeight, 0),
      zeph_price: formatNumber(stats.zephPrice, 4),
      zys_price: formatNumber(stats.zysPrice, 4),
      zsd_circ: formatNumber(stats.zsdCirc, 4),
      zys_circ: formatNumber(stats.zysCirc, 4),
      zsd_reserve: formatNumber(stats.zsdReserve, 4),
      reserve_ratio: formatNumber(stats.reserveRatio, 4),
      fallback_pricing: stats.fallbackPricing ? "yes" : "no",
    },
  ]);
}

export interface ProjectedAccrualRow {
  period: string;
  low: number;
  simple: number;
  high: number;
}

export function logProjectedAccruals(rows: ProjectedAccrualRow[]) {
  console.log("[projected] zsd accruals");
  console.table(
    rows.map((row) => ({
      period: row.period,
      low: formatNumber(row.low, 4),
      simple: formatNumber(row.simple, 4),
      high: formatNumber(row.high, 4),
    }))
  );
}

export interface ProjectedReturnRow {
  period: string;
  lowAmount: number;
  lowPct: number;
  simpleAmount: number;
  simplePct: number;
  highAmount: number;
  highPct: number;
}

export function logProjectedReturns(rows: ProjectedReturnRow[]) {
  console.log("[projected] zys returns");
  console.table(
    rows.map((row) => ({
      period: row.period,
      low_amount: formatNumber(row.lowAmount, 4),
      low_pct: formatNumber(row.lowPct, 4),
      simple_amount: formatNumber(row.simpleAmount, 4),
      simple_pct: formatNumber(row.simplePct, 4),
      high_amount: formatNumber(row.highAmount, 4),
      high_pct: formatNumber(row.highPct, 4),
    }))
  );
}

export interface ProjectedAssumptionRow {
  label: string;
  value: number;
  decimals?: number;
}

export function logProjectedAssumptions(rows: ProjectedAssumptionRow[]) {
  console.log("[projected] assumptions");
  console.table(
    rows.map((row) => ({
      label: row.label,
      value: formatNumber(row.value, row.decimals ?? 4),
    }))
  );
}

function computeDiff(aggregator: number | undefined, onChain: number | undefined): number | undefined {
  if (!Number.isFinite(aggregator ?? NaN) || !Number.isFinite(onChain ?? NaN)) {
    return undefined;
  }
  return (aggregator as number) - (onChain as number);
}

export function logScannerHealth(
  totalsSummary: TotalsSummary | null,
  stats: ProtocolStats | null,
  snapshot: ReserveSnapshot | null
) {
  console.log("[health] scanner vs reserve_info");

  if (!snapshot) {
    console.log("[health] no reserve snapshot available");
    return;
  }

  const rows: Array<{ metric: string; scanner: string; on_chain: string; diff: string }> = [];
  const numericTotals = totalsSummary?.numeric ?? {};

  const zepTotal = numericTotals.total_all;
  const zsdTotal = (numericTotals.mint_stable_volume ?? 0) - (numericTotals.redeem_stable_volume ?? 0);
  const zrsTotal = (numericTotals.mint_reserve_volume ?? 0) - (numericTotals.redeem_reserve_volume ?? 0);
  const zysTotal = (numericTotals.mint_yield_volume ?? 0) - (numericTotals.redeem_yield_volume ?? 0);

  if (totalsSummary) {
    const totalsRows = [
      {
        metric: "ZEPH circ (totals)",
        aggregatorValue: zepTotal,
        onChainValue: snapshot.on_chain.zeph_reserve + snapshot.on_chain.zsd_circ + snapshot.on_chain.zrs_circ,
        decimals: 2,
      },
      {
        metric: "ZSD circ (totals)",
        aggregatorValue: zsdTotal,
        onChainValue: snapshot.on_chain.zsd_circ,
        decimals: 2,
      },
      {
        metric: "ZRS circ (totals)",
        aggregatorValue: zrsTotal,
        onChainValue: snapshot.on_chain.zrs_circ,
        decimals: 2,
      },
      {
        metric: "ZYS circ (totals)",
        aggregatorValue: zysTotal,
        onChainValue: snapshot.on_chain.zyield_circ,
        decimals: 2,
      },
      {
        metric: "ZEPH reserve (totals)",
        aggregatorValue: snapshot.on_chain.zeph_reserve,
        onChainValue: snapshot.on_chain.zeph_reserve,
        decimals: 2,
      },
      {
        metric: "ZSD yield reserve (totals)",
        aggregatorValue: snapshot.on_chain.zsd_yield_reserve,
        onChainValue: snapshot.on_chain.zsd_yield_reserve,
        decimals: 2,
      },
    ];

    for (const { metric, aggregatorValue, onChainValue, decimals } of totalsRows) {
      const diff = computeDiff(aggregatorValue, onChainValue);
      rows.push({
        metric,
        scanner: formatNumber(aggregatorValue, decimals),
        on_chain: formatNumber(onChainValue, decimals),
        diff: formatNumber(diff, decimals),
      });
    }
    rows.push({ metric: "----", scanner: "-", on_chain: "-", diff: "-" });
  }

  if (stats) {
    const statFields = [
      {
        metric: "Zeph circ (stats)",
        aggregatorValue: stats.zeph_circ,
        onChainValue: numericTotals.total_all,
        decimals: 2,
      },
      {
        metric: "Zeph reserve (stats)",
        aggregatorValue: stats.zeph_in_reserve,
        onChainValue: snapshot.on_chain.zeph_reserve,
        decimals: 2,
      },
      {
        metric: "ZSD yield reserve (stats)",
        aggregatorValue: stats.zsd_in_yield_reserve,
        onChainValue: snapshot.on_chain.zsd_yield_reserve,
        decimals: 2,
      },
      {
        metric: "ZSD circ (stats)",
        aggregatorValue: stats.zephusd_circ,
        onChainValue: snapshot.on_chain.zsd_circ,
        decimals: 2,
      },
      {
        metric: "ZRS circ (stats)",
        aggregatorValue: stats.zephrsv_circ,
        onChainValue: snapshot.on_chain.zrs_circ,
        decimals: 2,
      },
      {
        metric: "ZYS circ (stats)",
        aggregatorValue: stats.zyield_circ,
        onChainValue: snapshot.on_chain.zyield_circ,
        decimals: 2,
      },
      {
        metric: "Reserve ratio",
        aggregatorValue: stats.reserve_ratio ?? undefined,
        onChainValue: snapshot.on_chain.reserve_ratio ?? undefined,
        decimals: 4,
      },
    ];

    for (const { metric, aggregatorValue, onChainValue, decimals } of statFields) {
      const diff = computeDiff(aggregatorValue, onChainValue);
      rows.push({
        metric,
        scanner: formatNumber(aggregatorValue, decimals),
        on_chain: formatNumber(onChainValue, decimals),
        diff: formatNumber(diff, decimals),
      });
    }
  }

  console.table(rows);

  if (!totalsSummary) {
    console.log("[health] totals unavailable for net calculation");
  }
  if (!stats) {
    console.log("[health] latest protocol stats unavailable");
  }
}
