import type { RedisOptions } from "ioredis";
import Redis from "ioredis";
import dotenv from "dotenv";

dotenv.config();

const {
  REDIS_URL,
  REDIS_HOST = "localhost",
  REDIS_PORT = "6379",
  REDIS_PASSWORD,
  REDIS_DB = "0",
  REDIS_CONNECTION_NAME,
} = process.env;

const parsedPort = Number(REDIS_PORT);
const parsedDb = Number(REDIS_DB);

const TRANSIENT_ERROR_CODES = new Set(["ECONNRESET", "ECONNREFUSED", "ETIMEDOUT"]);

function resolveDb(dbOverride?: number): number {
  if (Number.isFinite(dbOverride)) {
    return Number(dbOverride);
  }
  return Number.isFinite(parsedDb) ? parsedDb : 0;
}

function buildOptions(db: number): RedisOptions {
  const connectionName = REDIS_CONNECTION_NAME ?? `zephyrdscanner:${process.pid}`;

  const baseOptions: RedisOptions = {
    db,
    password: REDIS_PASSWORD || undefined,
    keepAlive: 15000,
    connectionName,
    maxRetriesPerRequest: null,
    retryStrategy(times) {
      const delay = Math.min(times * 200, 2000);
      console.warn(`[redis] retry #${times}, next attempt in ${delay}ms`);
      return delay;
    },
    reconnectOnError(error) {
      const code = (error as NodeJS.ErrnoException)?.code;
      if (code && TRANSIENT_ERROR_CODES.has(code)) {
        console.warn(`[redis] reconnecting after ${code}`);
        return true;
      }
      return false;
    },
  };

  if (!REDIS_URL || REDIS_URL.length === 0) {
    baseOptions.host = REDIS_HOST;
    baseOptions.port = Number.isFinite(parsedPort) ? parsedPort : 6379;
  }

  return baseOptions;
}

export function createRedisClient(dbOverride?: number): Redis {
  const db = resolveDb(dbOverride);
  const options = buildOptions(db);

  const instance =
    typeof REDIS_URL === "string" && REDIS_URL.length > 0 ? new Redis(REDIS_URL, options) : new Redis(options);

  attachLogging(instance);
  return instance;
}

function attachLogging(instance: Redis) {
  instance.on("connect", () => {
    const dbIndex = instance.options.db ?? 0;
    console.log(`[redis] connected (db ${dbIndex})`);
  });

  instance.on("ready", () => {
    console.log("[redis] ready");
  });

  instance.on("reconnecting", (delay: any) => {
    console.warn(`[redis] reconnecting in ${delay}ms`);
  });

  instance.on("end", () => {
    console.warn("[redis] connection closed");
  });

  instance.on("error", (error: NodeJS.ErrnoException) => {
    const code = error?.code;
    if (code && TRANSIENT_ERROR_CODES.has(code)) {
      console.warn(`[redis] transient ${code} (${error.syscall ?? "unknown syscall"}) – waiting for retry`);
      return;
    }
    console.error("[redis] error:", error);
  });
}

const client = createRedisClient();

export default client;
