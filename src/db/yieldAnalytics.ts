import { HistoricalReturnRange, ProjectionScenario, ProjectionTimeframe } from "@prisma/client";
import {
  HistoricalReturnEntry,
  HistoricalReturns,
  ProjectedReturns,
  ProjectedReturnTier,
  ProjectedReturnScenario,
  ApyHistoryEntry as ApyHistory,
} from "../yield";
import { getPrismaClient } from "./index";

const prisma = () => getPrismaClient();

const HISTORICAL_KEY_MAP: Record<keyof HistoricalReturns, HistoricalReturnRange> = {
  lastBlock: "LAST_BLOCK",
  oneDay: "ONE_DAY",
  oneWeek: "ONE_WEEK",
  oneMonth: "ONE_MONTH",
  threeMonths: "THREE_MONTHS",
  oneYear: "ONE_YEAR",
  allTime: "ALL_TIME",
};

const HISTORICAL_KEY_REVERSE_MAP: Record<HistoricalReturnRange, keyof HistoricalReturns> = {
  LAST_BLOCK: "lastBlock",
  ONE_DAY: "oneDay",
  ONE_WEEK: "oneWeek",
  ONE_MONTH: "oneMonth",
  THREE_MONTHS: "threeMonths",
  ONE_YEAR: "oneYear",
  ALL_TIME: "allTime",
};

const PROJECTION_TIMEFRAME_MAP: Record<keyof ProjectedReturns, ProjectionTimeframe> = {
  oneWeek: "ONE_WEEK",
  oneMonth: "ONE_MONTH",
  threeMonths: "THREE_MONTHS",
  sixMonths: "SIX_MONTHS",
  oneYear: "ONE_YEAR",
};

const PROJECTION_TIMEFRAME_REVERSE_MAP: Record<ProjectionTimeframe, keyof ProjectedReturns> = {
  ONE_WEEK: "oneWeek",
  ONE_MONTH: "oneMonth",
  THREE_MONTHS: "threeMonths",
  SIX_MONTHS: "sixMonths",
  ONE_YEAR: "oneYear",
};

const scenarioToEnum = (name: keyof ProjectedReturnTier): ProjectionScenario => {
  switch (name) {
    case "low":
      return "LOW";
    case "simple":
      return "SIMPLE";
    case "high":
      return "HIGH";
    default:
      return "SIMPLE";
  }
};

export async function upsertHistoricalReturns(data: HistoricalReturns): Promise<void> {
  const client = prisma();
  const entries = Object.entries(data).filter(([key]) => {
    if (!HISTORICAL_KEY_MAP[key as keyof HistoricalReturns]) {
      console.warn(`[yield] Skipping historical return key '${key}' – no enum mapping`);
      return false;
    }
    return true;
  });

  await Promise.all(
    entries.map(([key, value]) =>
      client.historicalReturn.upsert({
        where: { range: HISTORICAL_KEY_MAP[key as keyof HistoricalReturns] },
        update: {
          returnPct: value.return,
          zsdAccrued: value.ZSDAccrued,
          effectiveApy: value.effectiveApy,
        },
        create: {
          range: HISTORICAL_KEY_MAP[key as keyof HistoricalReturns],
          returnPct: value.return,
          zsdAccrued: value.ZSDAccrued,
          effectiveApy: value.effectiveApy,
        },
      })
    )
  );
}

export async function fetchHistoricalReturns(): Promise<HistoricalReturns | null> {
  const rows = await prisma().historicalReturn.findMany();
  if (rows.length === 0) {
    return null;
  }
  const result: HistoricalReturns = {
    lastBlock: { return: 0, ZSDAccrued: 0, effectiveApy: 0 },
    oneDay: { return: 0, ZSDAccrued: 0, effectiveApy: 0 },
    oneWeek: { return: 0, ZSDAccrued: 0, effectiveApy: 0 },
    oneMonth: { return: 0, ZSDAccrued: 0, effectiveApy: 0 },
    threeMonths: { return: 0, ZSDAccrued: 0, effectiveApy: 0 },
    oneYear: { return: 0, ZSDAccrued: 0, effectiveApy: 0 },
    allTime: { return: 0, ZSDAccrued: 0, effectiveApy: 0 },
  };
  for (const row of rows) {
    const key = HISTORICAL_KEY_REVERSE_MAP[row.range];
    if (!key) {
      continue;
    }
    result[key] = {
      return: row.returnPct,
      ZSDAccrued: row.zsdAccrued,
      effectiveApy: row.effectiveApy,
    };
  }
  return result;
}

type ProjectionRecord = {
  timeframe: ProjectionTimeframe;
  scenario: ProjectionScenario;
  zysPrice: number;
  returnPct: number;
};

export async function upsertProjectedReturns(data: ProjectedReturns): Promise<void> {
  const client = prisma();
  const records: ProjectionRecord[] = [];
  for (const [timeframe, tier] of Object.entries(data)) {
    const tf = PROJECTION_TIMEFRAME_MAP[timeframe as keyof ProjectedReturns];
    if (!tf) {
      console.warn(`[yield] Skipping projected timeframe '${timeframe}' – no enum mapping`);
      continue;
    }
    const pushScenario = (name: keyof ProjectedReturnTier) => {
      const scenario = tier[name];
      records.push({
        timeframe: tf,
        scenario: scenarioToEnum(name),
        zysPrice: scenario.zys_price,
        returnPct: scenario.return,
      });
    };
    pushScenario("low");
    pushScenario("simple");
    pushScenario("high");
  }

  await Promise.all(
    records.map((record) =>
      client.projectedReturn.upsert({
        where: {
          timeframe_scenario: {
            timeframe: record.timeframe,
            scenario: record.scenario,
          },
        },
        update: {
          zysPrice: record.zysPrice,
          returnPct: record.returnPct,
        },
        create: {
          timeframe: record.timeframe,
          scenario: record.scenario,
          zysPrice: record.zysPrice,
          returnPct: record.returnPct,
        },
      })
    )
  );
}

export async function fetchProjectedReturns(): Promise<ProjectedReturns | null> {
  const rows = await prisma().projectedReturn.findMany();
  if (rows.length === 0) {
    return null;
  }
  const emptyTier = (): ProjectedReturnTier => ({
    low: { zys_price: 0, return: 0 },
    simple: { zys_price: 0, return: 0 },
    high: { zys_price: 0, return: 0 },
  });
  const result: ProjectedReturns = {
    oneWeek: emptyTier(),
    oneMonth: emptyTier(),
    threeMonths: emptyTier(),
    sixMonths: emptyTier(),
    oneYear: emptyTier(),
  };
  for (const row of rows) {
    const timeframeKey = PROJECTION_TIMEFRAME_REVERSE_MAP[row.timeframe];
    if (!timeframeKey) {
      console.warn(`[yield] Skipping projected return with unsupported timeframe '${row.timeframe}'`);
      continue;
    }
    const tier = result[timeframeKey];
    const scenario = row.scenario.toLowerCase() as keyof ProjectedReturnTier;
    if (!tier[scenario]) {
      console.warn(`[yield] Skipping projected return with unsupported scenario '${row.scenario}'`);
      continue;
    }
    tier[scenario] = {
      zys_price: row.zysPrice,
      return: row.returnPct,
    };
  }
  return result;
}

export async function replaceApyHistory(entries: ApyHistory[]): Promise<void> {
  const client = prisma();
  await client.$transaction([
    client.apyHistoryEntry.deleteMany(),
    client.apyHistoryEntry.createMany({
      data: entries.map((entry) => ({
        timestamp: entry.timestamp,
        blockHeight: entry.block_height,
        returnPct: entry.return,
        zysPrice: entry.zys_price,
      })),
    }),
  ]);
}

export async function fetchApyHistory(): Promise<ApyHistory[]> {
  const rows = await prisma().apyHistoryEntry.findMany({
    orderBy: { timestamp: "asc" },
  });
  return rows.map((row) => ({
    timestamp: row.timestamp,
    block_height: row.blockHeight,
    return: row.returnPct,
    zys_price: row.zysPrice,
  }));
}

export async function appendZysPriceHistory(entries: { block_height: number; zys_price: number; timestamp: number }[]): Promise<void> {
  if (entries.length === 0) {
    return;
  }
  await prisma().zysPriceHistoryEntry.createMany({
    data: entries.map((entry) => ({
      blockHeight: entry.block_height,
      zysPrice: entry.zys_price,
      timestamp: entry.timestamp,
    })),
    skipDuplicates: true,
  });
}

export async function fetchZysPriceHistory(): Promise<{ timestamp: number; block_height: number; zys_price: number }[]> {
  const rows = await prisma().zysPriceHistoryEntry.findMany({
    orderBy: { timestamp: "asc" },
  });
  return rows.map((row) => ({
    timestamp: row.timestamp,
    block_height: row.blockHeight,
    zys_price: row.zysPrice,
  }));
}
