import Redis from "ioredis";

const client = new Redis({
  port: 6379,
  host: "localhost",
  db: 0,
});

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
