import dotenv from "dotenv";
import path from "node:path";
import { ChildProcess, spawn } from "node:child_process";
import type { Server } from "node:http";
import { createRequire } from "node:module";

import { startServer } from "./server";

dotenv.config();

interface BootstrapResult {
  server: Server | null;
  scanner?: ChildProcess;
}

function startScannerProcess(): ChildProcess | undefined {
  if (process.env.ENABLE_SCANNER === "false") {
    console.log("Scanner process disabled via ENABLE_SCANNER=false");
    return undefined;
  }

  const scannerEntry = path.join(__dirname, "scanner-runner.ts");
  const require = createRequire(__filename);
  const tsxCli = require.resolve("tsx/cli");

  const child = spawn(
    process.execPath,
    [tsxCli, scannerEntry],
    {
      stdio: "inherit",
      env: { ...process.env },
    }
  );

  child.on("exit", (code, signal) => {
    const reason = signal ? `signal ${signal}` : `code ${code}`;
    console.log(`[scanner] process exited with ${reason}`);
    if (signal !== "SIGTERM" && code !== 0) {
      console.error("Scanner terminated unexpectedly. Review logs above.");
    }
  });

  return child;
}

function bootstrap(): BootstrapResult {
  const { server } = startServer();
  const scanner = startScannerProcess();

  const shutdown = (signal: NodeJS.Signals) => {
    console.log(`Received ${signal}. Shutting down Zephyrd Scanner services...`);

    scanner?.kill("SIGTERM");

    if (server) {
      server.close(() => {
        process.exit(0);
      });
    } else {
      process.exit(0);
    }
  };

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);

  return { server, scanner };
}

bootstrap();
