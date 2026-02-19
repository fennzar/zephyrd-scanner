import { stores } from "./storage/factory";

function toNumber(value: string | null | undefined, fallback = 0): number {
  if (typeof value !== "string" || value.trim() === "") {
    return fallback;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

async function getNumber(key: string, fallback = 0): Promise<number> {
  const raw = await stores.scannerState.get(key);
  return toNumber(raw, fallback);
}

async function setNumber(key: string, value: number): Promise<void> {
  await stores.scannerState.set(key, value.toString());
}

export async function getAggregatorHeight(): Promise<number> {
  return getNumber("height_aggregator", -1);
}

export async function setAggregatorHeight(height: number): Promise<void> {
  await setNumber("height_aggregator", height);
}

export async function getPricingHeight(): Promise<number> {
  return getNumber("height_prs");
}

export async function setPricingHeight(height: number): Promise<void> {
  await setNumber("height_prs", height);
}

export async function getTransactionHeight(): Promise<number> {
  return getNumber("height_txs");
}

export async function setTransactionHeight(height: number): Promise<void> {
  await setNumber("height_txs", height);
}

export async function getHourlyTimestamp(): Promise<number> {
  return getNumber("timestamp_aggregator_hourly");
}

export async function setHourlyTimestamp(timestamp: number): Promise<void> {
  await setNumber("timestamp_aggregator_hourly", timestamp);
}

export async function getDailyTimestamp(): Promise<number> {
  return getNumber("timestamp_aggregator_daily");
}

export async function setDailyTimestamp(timestamp: number): Promise<void> {
  await setNumber("timestamp_aggregator_daily", timestamp);
}

