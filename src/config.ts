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

export interface RuntimeConfigSummary {
  dataStoreMode: DataStoreMode;
  redisEnabled: boolean;
  postgresEnabled: boolean;
  redis: {
    url?: string;
    host: string;
    port: string;
    db: string;
  };
  postgres: {
    configured: boolean;
    host?: string;
    port?: string;
    database?: string;
    schema?: string;
    user?: string;
    error?: string;
  };
}

function describeDatabaseUrl(url?: string): RuntimeConfigSummary["postgres"] {
  if (!url) {
    return { configured: false };
  }
  try {
    const parsed = new URL(url);
    return {
      configured: true,
      host: parsed.hostname || undefined,
      port: parsed.port || "5432",
      database: parsed.pathname.replace(/^\//, "") || undefined,
      schema: parsed.searchParams.get("schema") ?? undefined,
      user: parsed.username || undefined,
    };
  } catch (error) {
    return {
      configured: false,
      error: error instanceof Error ? error.message : "Invalid DATABASE_URL",
    };
  }
}

export function getRuntimeConfigSummary(): RuntimeConfigSummary {
  const {
    REDIS_URL,
    REDIS_HOST = "localhost",
    REDIS_PORT = "6379",
    REDIS_DB = "0",
  } = process.env;

  const redis = {
    url: REDIS_URL,
    host: REDIS_HOST,
    port: REDIS_PORT,
    db: REDIS_DB,
  };

  return {
    dataStoreMode,
    redisEnabled: useRedis(),
    postgresEnabled: usePostgres(),
    redis,
    postgres: describeDatabaseUrl(getDatabaseUrl()),
  };
}

export function getStartBlock(): number {
  const envValue = process.env.START_BLOCK;
  if (!envValue) return 0;
  const parsed = parseInt(envValue, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : 0;
}

export function logRuntimeConfig(context = "runtime"): void {
  const summary = getRuntimeConfigSummary();
  console.log(
    `[config][${context}] DATA_STORE=${summary.dataStoreMode} (redis=${summary.redisEnabled} | postgres=${summary.postgresEnabled})`
  );

  if (summary.redisEnabled) {
    console.log(
      `[config][${context}] Redis host=${summary.redis.host}:${summary.redis.port} db=${summary.redis.db} url=${summary.redis.url ? "custom" : "derived"
      }`
    );
  } else {
    console.log(`[config][${context}] Redis disabled for this process (DATA_STORE=${summary.dataStoreMode})`);
  }

  if (summary.postgres.configured) {
    const pg = summary.postgres;
    console.log(
      `[config][${context}] Postgres host=${pg.host ?? "?"}:${pg.port ?? "5432"} db=${pg.database ?? "?"} schema=${pg.schema ?? "public"
      } user=${pg.user ?? "?"}`
    );
  } else if (summary.postgres.error) {
    console.warn(`[config][${context}] Postgres disabled – ${summary.postgres.error}`);
  } else {
    console.log(`[config][${context}] Postgres disabled (DATABASE_URL not set)`);
  }
}
