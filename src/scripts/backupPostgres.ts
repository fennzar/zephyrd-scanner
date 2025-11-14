/* eslint-disable no-console */
import childProcess from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import process from "node:process";
import dotenv from "dotenv";

dotenv.config();

function ensureEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    console.error(`[db:backup] Missing required env var ${name}`);
    process.exit(1);
  }
  return value;
}

function formatTimestamp() {
  return new Date().toISOString().replace(/:/g, "-");
}

function parseArgs(): { tag?: string } {
  const args = process.argv.slice(2);
  const options: { tag?: string } = {};
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === "--tag" && args[i + 1]) {
      options.tag = args[i + 1];
      i++;
    }
  }
  return options;
}

function sanitizeTag(tag: string): string {
  return tag.replace(/[^a-zA-Z0-9._-]/g, "-");
}

async function main() {
  const { tag } = parseArgs();
  const databaseUrl = ensureEnv("DATABASE_URL");
  const url = new URL(databaseUrl);

  const dbName = url.pathname.replace(/^\//, "");
  const user = url.username;
  const password = url.password;
  const host = url.hostname;
  const port = url.port || "5432";

  const schemaParam = url.searchParams.get("schema");
  const schemas = schemaParam
    ? schemaParam
        .split(",")
        .map((schema) => schema.trim())
        .filter((schema) => schema.length > 0)
    : [];

  const backupDir = path.resolve(process.cwd(), "backups");
  fs.mkdirSync(backupDir, { recursive: true });
  const tagSuffix = tag ? `_${sanitizeTag(tag)}` : "";
  const outputPath = path.join(
    backupDir,
    `postgres_backup_${formatTimestamp()}${tagSuffix}.sql`
  );

  const args = ["-h", host, "-p", port, "-U", user];
  schemas.forEach((schema) => {
    args.push("--schema", schema);
  });
  args.push(dbName);

  console.log(`[db:backup] Running pg_dump -> ${outputPath}`);
  const outputFd = fs.openSync(outputPath, "w");
  try {
    const result = childProcess.spawnSync("pg_dump", args, {
      env: { ...process.env, PGPASSWORD: password },
      stdio: ["inherit", outputFd, "inherit"],
    });

    if (result.error) {
      throw result.error;
    }

    if (result.status !== 0) {
      throw new Error(`pg_dump exited with code ${result.status}`);
    }
  } finally {
    fs.closeSync(outputFd);
  }

  console.log("[db:backup] Backup complete");
}

main().catch((error) => {
  console.error("[db:backup] Failed", error);
  process.exit(1);
});
