import { AggregatedData, ProtocolStats, ReserveSnapshot } from "./utils";

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

function printTable(title: string, configs: MetricConfig[], data: AggregatedData) {
  if (configs.length === 0) {
    return;
  }
  console.log(`[aggregation] ${title}`);
  console.table(configs.map((cfg) => buildMetricRow(cfg, data)));
}

function formatCounters(data: AggregatedData, windowStart: number) {
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

  console.log("[aggregation] counters & fees");
  console.table(rows);

  console.log(
    `[aggregation] pending=${data.pending ? "yes" : "no"} | window=[${formatNumber(
      windowStart,
      0
    )} → ${formatNumber(data.window_end, 0)}] | conversions=${formatNumber(
      data.conversion_transactions_count,
      0
    )} | yield_conversions=${formatNumber(data.yield_conversion_transactions_count, 0)}`
  );
}

export function logAggregatedSummary(windowType: WindowType, windowStart: number, data: AggregatedData) {
  console.log(`[aggregation] ${windowType} summary for window ${formatNumber(windowStart, 0)}`);

  printTable(
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

  formatCounters(data, windowStart);
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
      type: "total",
      amount: formatNumber(
        (numericTotals.miner_reward ?? 0) +
          (numericTotals.governance_reward ?? 0) +
          (numericTotals.reserve_reward ?? 0) +
          (numericTotals.yield_reward ?? 0),
        4
      ),
    },
  ];

  console.log("[totals] rewards");
  console.table(rewardRows);

  console.log(
    `[totals] conversions=${formatNumber(numericTotals.conversion_transactions, 0)} | yield_conversions=${formatNumber(
      numericTotals.yield_conversion_transactions_count,
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

  const rows: Array<{ metric: string; aggregator: string; on_chain: string; diff: string }> = [];
  const numericTotals = totalsSummary?.numeric ?? {};

  const zsdNet = (numericTotals.mint_stable_volume ?? 0) - (numericTotals.redeem_stable_volume ?? 0);
  const zrsNet = (numericTotals.mint_reserve_volume ?? 0) - (numericTotals.redeem_reserve_volume ?? 0);
  const zysNet = (numericTotals.mint_yield_volume ?? 0) - (numericTotals.redeem_yield_volume ?? 0);

  if (totalsSummary) {
    const netFields = [
      {
        metric: "ZSD circ (net)",
        aggregatorValue: zsdNet,
        onChainValue: snapshot.on_chain.zsd_circ,
        decimals: 2,
      },
      {
        metric: "ZRS circ (net)",
        aggregatorValue: zrsNet,
        onChainValue: snapshot.on_chain.zrs_circ,
        decimals: 2,
      },
      {
        metric: "ZYS circ (net)",
        aggregatorValue: zysNet,
        onChainValue: snapshot.on_chain.zyield_circ,
        decimals: 2,
      },
    ];

    for (const { metric, aggregatorValue, onChainValue, decimals } of netFields) {
      const diff = computeDiff(aggregatorValue, onChainValue);
      rows.push({
        metric,
        aggregator: formatNumber(aggregatorValue, decimals),
        on_chain: formatNumber(onChainValue, decimals),
        diff: formatNumber(diff, decimals),
      });
    }
  }

  if (stats) {
    const statFields = [
      {
        metric: "Zeph reserve",
        aggregatorValue: stats.zeph_in_reserve,
        onChainValue: snapshot.on_chain.zeph_reserve,
        decimals: 2,
      },
      {
        metric: "ZSD yield reserve",
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
        aggregatorValue: stats.reserve_ratio,
        onChainValue: snapshot.on_chain.reserve_ratio ?? undefined,
        decimals: 4,
      },
    ];

    for (const { metric, aggregatorValue, onChainValue, decimals } of statFields) {
      const diff = computeDiff(aggregatorValue, onChainValue);
      rows.push({
        metric,
        aggregator: formatNumber(aggregatorValue, decimals),
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
