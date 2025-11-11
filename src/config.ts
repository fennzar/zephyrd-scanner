import dotenv from "dotenv";

dotenv.config();

export type DataStoreMode = "redis" | "postgres" | "hybrid";

function normalizeDataStore(value?: string): DataStoreMode {
  const normalized = (value ?? "redis").toLowerCase();
  if (normalized === "postgres" || normalized === "hybrid") {
    return normalized;
  }
  return "redis";
}

const dataStoreMode = normalizeDataStore(process.env.DATA_STORE);

export function getDataStoreMode(): DataStoreMode {
  return dataStoreMode;
}

export function useRedis(): boolean {
  return dataStoreMode === "redis" || dataStoreMode === "hybrid";
}

export function usePostgres(): boolean {
  return dataStoreMode === "postgres" || dataStoreMode === "hybrid";
}

export function dualWriteEnabled(): boolean {
  return dataStoreMode === "hybrid";
}

export function getDatabaseUrl(): string | undefined {
  return process.env.DATABASE_URL;
}
