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

const PROJECTION_TIMEFRAME_MAP: Record<keyof ProjectedReturns, ProjectionTimeframe> = {
  oneWeek: "ONE_WEEK",
  oneMonth: "ONE_MONTH",
  threeMonths: "THREE_MONTHS",
  sixMonths: "SIX_MONTHS",
  oneYear: "ONE_YEAR",
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
  const result = {} as HistoricalReturns;
  for (const row of rows) {
    const key = row.range.toString().charAt(0) + row.range.toString().slice(1).toLowerCase();
    result[key as keyof HistoricalReturns] = {
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
  const result = {} as ProjectedReturns;
  for (const row of rows) {
    const timeframe = row.timeframe.toLowerCase();
    const tier = (result[timeframe as keyof ProjectedReturns] ||= {
      low: { zys_price: 0, return: 0 },
      simple: { zys_price: 0, return: 0 },
      high: { zys_price: 0, return: 0 },
    } as ProjectedReturnTier);
    const scenario = row.scenario.toLowerCase() as keyof ProjectedReturnTier;
    const target: ProjectedReturnScenario = {
      zys_price: row.zysPrice,
      return: row.returnPct,
    };
    tier[scenario] = target;
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
