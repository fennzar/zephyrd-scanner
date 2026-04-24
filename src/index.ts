import dotenv from "dotenv";
import path from "node:path";
import { ChildProcess, spawn } from "node:child_process";
import type { Server } from "node:http";
import { createRequire } from "node:module";

import { startServer } from "./server";
import { logRuntimeConfig } from "./config";

dotenv.config();
logRuntimeConfig("server");

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

// After a shutdown signal, give graceful cleanup this long before forcing exit.
const FORCED_EXIT_MS = 5_000;

function bootstrap(): BootstrapResult {
  const { server } = startServer();
  const scanner = startScannerProcess();

  let isShuttingDown = false;

  const shutdown = (signal: NodeJS.Signals) => {
    if (isShuttingDown) {
      console.log(`Shutdown already in progress (${signal} ignored).`);
      return;
    }
    isShuttingDown = true;

    console.log(`Received ${signal}. Shutting down Zephyrd Scanner services...`);

    // Hard deadline — any hang below (child process, prisma, server.close)
    // is terminated so the parent always exits within the window.
    const killTimer = setTimeout(() => {
      console.error(`Shutdown exceeded ${FORCED_EXIT_MS}ms, forcing exit.`);
      process.exit(1);
    }, FORCED_EXIT_MS);
    killTimer.unref();

    scanner?.kill("SIGTERM");

    (async () => {
      try {
        const { disconnectPrisma } = await import("./db");
        await disconnectPrisma();
        console.log("Prisma connection closed.");
      } catch {}

      if (server) {
        // Node 18.2+ — forcibly terminate keep-alive sockets so server.close
        // isn't blocked by idle connections.
        server.closeAllConnections?.();
        server.close(() => {
          clearTimeout(killTimer);
          process.exit(0);
        });
      } else {
        clearTimeout(killTimer);
        process.exit(0);
      }
    })();
  };

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);

  return { server, scanner };
}

bootstrap();
