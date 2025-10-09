import Redis from "ioredis";
import dotenv from "dotenv";

dotenv.config();

const { REDIS_URL, REDIS_HOST = "localhost", REDIS_PORT = "6379", REDIS_PASSWORD, REDIS_DB = "0" } = process.env;

const parsedPort = Number(REDIS_PORT);
const parsedDb = Number(REDIS_DB);

function resolveDb(dbOverride?: number): number {
  if (Number.isFinite(dbOverride)) {
    return Number(dbOverride);
  }
  return Number.isFinite(parsedDb) ? parsedDb : 0;
}

export function createRedisClient(dbOverride?: number): Redis {
  const db = resolveDb(dbOverride);

  if (typeof REDIS_URL === "string" && REDIS_URL.length > 0) {
    return new Redis(REDIS_URL, { db });
  }

  return new Redis({
    port: Number.isFinite(parsedPort) ? parsedPort : 6379,
    host: REDIS_HOST,
    password: REDIS_PASSWORD || undefined,
    db,
  });
}

const client = createRedisClient();

client.on("error", (error) => {
  console.error(error);
});

client.on("connect", async () => {
  console.log("Redis client connected");
});

client.on("ready", function () {
  console.log("Redis client ready");
});

client.on("end", function () {
  console.log("Redis client disconnected");
});

export default client;
