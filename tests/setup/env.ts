// Preload: sets env vars BEFORE any src/ module imports.
// This runs via bunfig.toml [test].preload so DATA_STORE is cached
// correctly when src/config.ts loads at import time.
// dotenv.config() in source files uses { override: false } by default,
// so these values win.

import { config } from "dotenv";
import { resolve } from "path";

config({ path: resolve(import.meta.dir, "../../.env.test") });
