/* eslint-disable no-console */
process.env.DATA_STORE = process.env.DATA_STORE ?? "postgres";

import childProcess from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import process from "node:process";

function ensureEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    console.error(`[db:restore] Missing required env var ${name}`);
    process.exit(1);
  }
  return value;
}

interface CliOptions {
  file: string;
}

function parseArgs(): CliOptions {
  const args = process.argv.slice(2);
  let file: string | undefined;
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if ((arg === "--file" || arg === "-f") && args[i + 1]) {
      file = args[i + 1];
      i++;
    }
  }

  if (!file) {
    console.error("Usage: npm run db:restore-sql -- --file <path/to/postgres_backup.sql>");
    process.exit(1);
  }

  return {
    file: path.resolve(file),
  };
}

async function main() {
  const options = parseArgs();
  const backupPath = options.file;

  if (!fs.existsSync(backupPath)) {
    console.error(`[db:restore] Backup file not found: ${backupPath}`);
    process.exit(1);
  }

  const databaseUrl = ensureEnv("DATABASE_URL");
  const url = new URL(databaseUrl);

  const dbName = url.pathname.replace(/^\//, "");
  const user = url.username;
  const password = url.password;
  const host = url.hostname;
  const port = url.port || "5432";

  const args = ["-h", host, "-p", port, "-U", user, "-d", dbName, "-f", backupPath];

  console.log(`[db:restore] Running psql < ${backupPath}`);
  const result = childProcess.spawnSync("psql", args, {
    env: { ...process.env, PGPASSWORD: password },
    stdio: "inherit",
  });

  if (result.error) {
    console.error("[db:restore] Failed to invoke psql", result.error);
    process.exit(1);
  }

  if (result.status !== 0) {
    console.error(`[db:restore] psql exited with code ${result.status}`);
    process.exit(result.status ?? 1);
  }

  console.log("[db:restore] Restore complete");
}

main().catch((error) => {
  console.error("[db:restore] Failed", error);
  process.exit(1);
});
