/* eslint-disable no-console */
import childProcess from "node:child_process";
import process from "node:process";

function ensureEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    console.error(`[db:reset] Missing required env var ${name}`);
    process.exit(1);
  }
  return value;
}

function run(command: string, env: Record<string, string>) {
  childProcess.execSync(command, {
    stdio: "inherit",
    env: { ...process.env, ...env },
  });
}

async function main() {
  const url = new URL(ensureEnv("DATABASE_URL"));
  const dbName = url.pathname.replace(/^\//, "");
  const user = url.username;
  const password = url.password;
  const host = url.hostname;
  const port = url.port || "5432";

  console.log(`[db:reset] Dropping database ${dbName}`);
  run(
    `PGPASSWORD=${password} dropdb --if-exists -h ${host} -p ${port} -U ${user} ${dbName}`,
    {}
  );

  console.log(`[db:reset] Creating database ${dbName}`);
  run(
    `PGPASSWORD=${password} createdb -h ${host} -p ${port} -U ${user} ${dbName}`,
    {}
  );

  console.log(`[db:reset] Database recreated. Run migrations next (npm run prisma:migrate:deploy).`);
}

main().catch((error) => {
  console.error("[db:reset] Failed", error);
  process.exit(1);
});
