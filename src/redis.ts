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

const TRANSIENT_ERROR_CODES = new Set(["ECONNRESET", "ECONNREFUSED", "ETIMEDOUT"]);

function buildOptions(db: number): RedisOptions {
  const connectionName = REDIS_CONNECTION_NAME ?? `zephyrdscanner:${process.pid}:db${db}`;

  const baseOptions: RedisOptions = {
    db,
    password: REDIS_PASSWORD || undefined,
    keepAlive: 15000,
    connectionName,
    maxRetriesPerRequest: null,
    retryStrategy(times) {
      const delay = Math.min(times * 200, 2000);
      console.warn(`[redis][db${db}] retry #${times}, next attempt in ${delay}ms`);
      return delay;
    },
    reconnectOnError(error) {
      const code = (error as NodeJS.ErrnoException)?.code;
      if (code && TRANSIENT_ERROR_CODES.has(code)) {
        console.warn(`[redis][db${db}] reconnecting after ${code}`);
        return true;
      }
      return false;
    },
  };

  if (!REDIS_URL || REDIS_URL.length === 0) {
    const parsedPort = Number(REDIS_PORT);
    baseOptions.host = REDIS_HOST;
    baseOptions.port = Number.isFinite(parsedPort) ? parsedPort : 6379;
  }

  return baseOptions;
}

export function createRedisClient(overrides?: number | { dbOverride?: number; lazyConnect?: boolean }): Redis {
  const opts = typeof overrides === "number" ? { dbOverride: overrides } : overrides;
  const parsedDb = Number(opts?.dbOverride ?? REDIS_DB ?? 0);
  const db = Number.isFinite(parsedDb) ? parsedDb : 0;
  const options = buildOptions(db);

  if (opts?.lazyConnect) {
    options.lazyConnect = true;
  }

  const instance =
    typeof REDIS_URL === "string" && REDIS_URL.length > 0 ? new Redis(REDIS_URL, options) : new Redis(options);

  attachLogging(instance, db);
  return instance;
}

function attachLogging(instance: Redis, db: number) {
  instance.on("connect", () => {
    console.log(`[redis][db${db}] connected`);
  });

  instance.on("ready", () => {
    console.log(`[redis][db${db}] ready`);
  });

  instance.on("reconnecting", (delay: any) => {
    console.warn(`[redis][db${db}] reconnecting in ${delay}ms`);
  });

  instance.on("end", () => {
    console.warn(`[redis][db${db}] connection closed`);
  });

  instance.on("error", (error: NodeJS.ErrnoException) => {
    const code = error?.code;
    if (code && TRANSIENT_ERROR_CODES.has(code)) {
      console.warn(`[redis][db${db}] transient ${code} (${error.syscall ?? "unknown syscall"}) – waiting for retry`);
      return;
    }
    console.error(`[redis][db${db}] error:`, error);
  });
}

const dataStore = (process.env.DATA_STORE ?? "redis").toLowerCase();
const redisNeeded = dataStore === "redis" || dataStore === "hybrid";

// When Redis is not the data store, use lazyConnect so no TCP connection is opened.
// All redis calls are gated behind useRedis(), so the connection is never triggered.
const client = redisNeeded ? createRedisClient() : createRedisClient({ lazyConnect: true });

export default client;
